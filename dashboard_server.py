"""
Farm Dashboard Server v5 — atomic FB + IG roster separation
Persistent via Upstash Redis or an attached Railway Volume. 24h follower delta.

Endpoints:
  POST /update/fb/{phone}    FB script sends status
  POST /update/ig/{phone}    IG script sends status
  GET  /status/fb/{phone}    FB status (includes 24h delta)
  GET  /status/ig/{phone}    IG status
  GET  /all/fb               all FB statuses
  GET  /all/ig               all IG statuses
  GET  /snapshots            list daily snapshots
  GET  /snapshot/{date}      specific day snapshot
  POST /crane/command        queue crane command
  GET  /crane/pending        agent polls
  POST /crane/result/{id}    agent reports done
  GET  /crane/containers/{p} container list
  POST /crane/containers/{p} update container list
  GET  /crane/queue          recent commands
  GET  /crane/state/{p}      container fresh/used state
  POST /crane/state/{p}      update state
"""

import json, os, uuid, time, datetime, threading, hashlib, fnmatch
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse
try:
    import requests as _req
    _HAS_REQUESTS = True
except ImportError:
    import urllib.request as _urllib
    _HAS_REQUESTS = False

UPSTASH_URL   = os.environ.get('UPSTASH_URL', '')
UPSTASH_TOKEN = os.environ.get('UPSTASH_TOKEN', '')
_volume_path  = os.environ.get('RAILWAY_VOLUME_MOUNT_PATH', '').strip()
_local_store_path = os.path.join(_volume_path, 'farm_dashboard_state.json') if _volume_path else ''
_local_store = {}
_local_lock = threading.RLock()

def _init_local_store():
    """Use Railway's attached volume when no Upstash database is configured."""
    global _local_store_path, _local_store
    if UPSTASH_URL or not _local_store_path:
        return
    try:
        os.makedirs(os.path.dirname(_local_store_path), exist_ok=True)
        if os.path.isfile(_local_store_path):
            with open(_local_store_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if isinstance(data, dict):
                _local_store = data
    except Exception as e:
        print(f'[storage] Volume unavailable: {e}')
        _local_store_path = ''

def _save_local_store():
    if not _local_store_path:
        return
    temp_path = _local_store_path + '.tmp'
    with open(temp_path, 'w', encoding='utf-8') as f:
        json.dump(_local_store, f, separators=(',', ':'))
    os.replace(temp_path, _local_store_path)

def _using_persistent_store():
    return bool(UPSTASH_URL or _local_store_path)

def _storage_label():
    if UPSTASH_URL:
        return 'Upstash Redis'
    if _local_store_path:
        return f'Railway Volume ({_volume_path})'
    return 'MEMORY ONLY'

# ─── OOPSIE BIO LINK ANALYTICS ───────────────────────────────────────────────
# Every existing Oopsie handle ends in the matching Phone number.  The public
# page URLs stay in one clear map so the dashboard can render the right link on
# the right Phone card.  OOPSIE_PAGES_JSON may override this map later without
# a code change, for example: {"1":"https://oopsie.bio/example1", ...}.
_DEFAULT_OOPSIE_PAGES = {
    '1': 'https://oopsie.bio/madgph1',
    '2': 'https://oopsie.bio/maddy2',
    '3': 'https://oopsie.bio/maddgirl3',
    '4': 'https://oopsie.bio/maddbab4',
    '5': 'https://oopsie.bio/maddiecutie5',
    '6': 'https://oopsie.bio/madpookie6',
    '7': 'https://oopsie.bio/maddmad7',
    '8': 'https://oopsie.bio/madd8',
    '9': 'https://oopsie.bio/ismaddie9',
}

def _json_env(name, fallback):
    raw = os.environ.get(name, '').strip()
    if not raw:
        return dict(fallback)
    try:
        value = json.loads(raw)
        if isinstance(value, dict):
            return {str(key): str(item).strip() for key, item in value.items() if str(item).strip()}
    except Exception:
        print(f'[analytics] Ignoring invalid {name}')
    return dict(fallback)

OOPSIE_PAGES = _json_env('OOPSIE_PAGES_JSON', _DEFAULT_OOPSIE_PAGES)
# Set this one Railway variable later to activate final-button click tracking.
# The target URLs never appear in the dashboard response.
OOPSIE_CLICK_TARGETS = _json_env('OOPSIE_CLICK_TARGETS_JSON', {})
_OOPSIE_BUCKET_KEY = 'oopsie:v1:hourly'
_OOPSIE_START_KEY = 'oopsie:v1:tracking_started_at'
_OOPSIE_RETENTION_SECONDS = 31 * 24 * 3600

def _valid_http_url(value):
    try:
        parsed = urlparse(str(value))
        return parsed.scheme in ('http', 'https') and bool(parsed.netloc)
    except Exception:
        return False

def _oopsie_buckets():
    data = _rget(_OOPSIE_BUCKET_KEY) or {}
    return data if isinstance(data, dict) else {}

def _record_oopsie_event(phone, event):
    """Persist an hourly view/click bucket; old buckets expire after 31 days."""
    phone = str(phone)
    if phone not in OOPSIE_PAGES or event not in ('views', 'clicks'):
        return False
    now = time.time()
    hour = str(int(now // 3600) * 3600)
    with _local_lock:
        buckets = _oopsie_buckets()
        bucket = buckets.setdefault(hour, {})
        phone_stats = bucket.setdefault(phone, {'views': 0, 'clicks': 0})
        phone_stats[event] = int(phone_stats.get(event, 0) or 0) + 1
        cutoff = now - _OOPSIE_RETENTION_SECONDS
        for bucket_hour in list(buckets):
            try:
                expired = float(bucket_hour) < cutoff
            except (TypeError, ValueError):
                expired = True
            if expired:
                buckets.pop(bucket_hour, None)
        _rset(_OOPSIE_BUCKET_KEY, buckets)
        if not _rget(_OOPSIE_START_KEY):
            _rset(_OOPSIE_START_KEY, now)
    return True

def _oopsie_analytics():
    """Return hour-resolution rolling totals for the last 24h, 7d and 30d."""
    now = time.time()
    windows = {'24h': 24 * 3600, '7d': 7 * 24 * 3600, '30d': 30 * 24 * 3600}
    phones = {
        phone: {
            'phone': int(phone),
            'page_url': url,
            'view_path': f'/t/oopsie/{phone}',
            'click_path': f'/t/oopsie/{phone}/click',
            'click_tracking_ready': _valid_http_url(OOPSIE_CLICK_TARGETS.get(phone, '')),
            'periods': {name: {'views': 0, 'clicks': 0} for name in windows},
        }
        for phone, url in sorted(OOPSIE_PAGES.items(), key=lambda item: int(item[0]))
    }
    for bucket_hour, phone_stats in _oopsie_buckets().items():
        try:
            ts = float(bucket_hour)
        except (TypeError, ValueError):
            continue
        if not isinstance(phone_stats, dict):
            continue
        for phone, stats in phone_stats.items():
            if phone not in phones or not isinstance(stats, dict):
                continue
            for period, seconds in windows.items():
                if now - ts < seconds:
                    phones[phone]['periods'][period]['views'] += int(stats.get('views', 0) or 0)
                    phones[phone]['periods'][period]['clicks'] += int(stats.get('clicks', 0) or 0)
    return {
        'ok': True,
        'tracking_started_at': _rget(_OOPSIE_START_KEY),
        'resolution': 'hourly',
        'phones': phones,
    }

# ─── UPSTASH ─────────────────────────────────────────────────────────────────
def _redis(method, path, body=None):
    if not UPSTASH_URL: return None
    url = UPSTASH_URL.rstrip('/') + path
    hdrs = {'Authorization': f'Bearer {UPSTASH_TOKEN}', 'Content-Type': 'application/json'}
    try:
        if _HAS_REQUESTS:
            r = _req.get(url, headers=hdrs, timeout=5) if method=='GET' else \
                _req.post(url, headers=hdrs, data=(body or b''), timeout=5)
            return r.json().get('result')
        else:
            req = _urllib.Request(url, data=(body if body else None), headers=hdrs, method=method)
            with _urllib.urlopen(req, timeout=5) as resp:
                return json.loads(resp.read()).get('result')
    except Exception as e:
        print(f'[redis] {e}')
        return None

def _rset(key, value):
    if UPSTASH_URL:
        _redis('POST', f'/set/{key}', json.dumps(value).encode())
        return
    if _local_store_path:
        with _local_lock:
            _local_store[key] = value
            _save_local_store()

def _rget(key):
    if UPSTASH_URL:
        raw = _redis('GET', f'/get/{key}')
        if raw is None: return None
        try: return json.loads(raw)
        except: return raw
    if _local_store_path:
        with _local_lock:
            value = _local_store.get(key)
            return json.loads(json.dumps(value)) if value is not None else None
    return None

def _rdel(key):
    if UPSTASH_URL:
        _redis('POST', f'/del/{key}')
        return
    if _local_store_path:
        with _local_lock:
            if key in _local_store:
                _local_store.pop(key, None)
                _save_local_store()

def _rkeys(pattern):
    if UPSTASH_URL:
        raw = _redis('GET', f'/keys/{pattern}')
        return raw if isinstance(raw, list) else []
    if _local_store_path:
        with _local_lock:
            return [key for key in _local_store if fnmatch.fnmatch(key, pattern)]
    return []

# Each roster is persisted independently by platform *and phone*.  This makes
# updates safe even if Railway serves requests from more than one process.
ROSTER_SCHEMA = 'v5'

def _roster_phone_key(platform, phone):
    return f'crane:{ROSTER_SCHEMA}:containers:{platform}:{phone}'

def _roster_meta_key(platform, phone):
    return f'crane:{ROSTER_SCHEMA}:meta:{platform}:{phone}'

def _purge_retired_roster_data():
    """Delete every retired roster namespace so it cannot come back."""
    for key in (
        'crane:containers', 'crane:containers:fb', 'crane:containers:ig',
        'crane:v3:containers:fb', 'crane:v3:containers:ig',
    ):
        _rdel(key)
    for key in (
        _rkeys('crane:meta:*') + _rkeys('crane:v3:meta:*') +
        _rkeys('crane:v3:containers:*') + _rkeys('crane:v4:meta:*') +
        _rkeys('crane:v4:containers:*') + _rkeys('crane:v5:meta:*') +
        _rkeys('crane:v5:containers:*')
    ):
        _rdel(key)

# ─── IN-MEMORY CACHE ─────────────────────────────────────────────────────────
_fb   = {}   # phone -> status
_ig   = {}   # phone -> status
_crane_q    = {}
_crane_ctrs_fb = {}   # phone -> FB containers
_crane_ctrs_ig = {}   # phone -> IG containers
_lock = threading.Lock()

def _normalize_roster(containers):
    """Normalize a Crane roster while preserving container identity (name + UUID)."""
    out, seen = [], set()
    if not isinstance(containers, list):
        return []
    for item in containers:
        if not isinstance(item, dict):
            continue
        name = str(item.get('name', '')).strip()
        # The automation itself addresses containers by numeric name. Reject
        # headers/diagnostic lines accidentally parsed as containers.
        if not name or not name.isdigit() or name in seen:
            continue
        seen.add(name)
        try: num = int(item.get('num', name))
        except Exception: num = int(name)
        out.append({
            'name': name,
            'num': num,
            'uuid': str(item.get('uuid', '') or ''),
            'active': bool(item.get('active', False)),
        })
    out.sort(key=lambda x: (x['num'], x['name']))
    return out

def _roster_signature(containers):
    payload = json.dumps(containers, sort_keys=True, separators=(',', ':')).encode()
    return hashlib.sha256(payload).hexdigest()

def _invalidate_container_data(platform, phone, name):
    """Drop stats/history for a container whose UUID generation changed."""
    store = _fb if platform == 'fb' else _ig
    current = store.get(phone) or {}
    accounts = dict(current.get('accounts') or {})
    accounts.pop(str(name), None)
    current['accounts'] = accounts
    store[phone] = current
    _rset(f'{platform}:status:{phone}', current)

    hist = _rget(f'{platform}:fh:{phone}') or {}
    hist.pop(str(name), None)
    _rset(f'{platform}:fh:{phone}', hist)

    created = _rget(f'{platform}:created:{phone}') or {}
    created.pop(str(name), None)
    _rset(f'{platform}:created:{phone}', created)
    _rdel(f'{platform}:flw_daily:{phone}:{name}')

def _load_all():
    if not _using_persistent_store():
        print('[startup] No Upstash or Railway Volume — running in-memory only')
        return
    print(f'[startup] Loading from {_storage_label()}...')
    # Phone statuses
    for key in _rkeys('fb:status:*'):
        phone = key.replace('fb:status:', '')
        d = _rget(key)
        if d: _fb[phone] = d
    for key in _rkeys('ig:status:*'):
        phone = key.replace('ig:status:', '')
        d = _rget(key)
        if d: _ig[phone] = d
    # Crane queue
    q = _rget('crane:queue')
    if q: _crane_q.update(q)
    # Each v5 roster is an independent platform+phone record.  Never load a
    # shared list from any earlier server version.
    for platform, store in (('fb', _crane_ctrs_fb), ('ig', _crane_ctrs_ig)):
        for key in _rkeys(f'crane:{ROSTER_SCHEMA}:containers:{platform}:*'):
            phone = key.rsplit(':', 1)[-1]
            roster = _rget(key)
            if isinstance(roster, list):
                store[phone] = _normalize_roster(roster)
    print(f'[startup] FB phones: {list(_fb.keys())} | IG phones: {list(_ig.keys())} | FB ctrs: {list(_crane_ctrs_fb.keys())}')

# ─── FOLLOWER HISTORY (24h delta) ────────────────────────────────────────────
def _parse_followers(raw):
    """Convert 1.5K→1500, 5.1K→5100, 1M→1000000, 123→123."""
    s = str(raw).strip().upper().replace(',','').replace(' ','')
    if not s or s in ('?','—',''):
        return 0
    try:
        if s.endswith('K'): return int(float(s[:-1]) * 1000)
        if s.endswith('M'): return int(float(s[:-1]) * 1_000_000)
        return int(float(s))
    except: return 0

def _normalize_followers(accounts: dict):
    """Normalize all follower counts to integers when saving."""
    for acc in accounts.values():
        raw = acc.get('followers', '?')
        if raw and str(raw) not in ('?', ''):
            num = _parse_followers(raw)
            acc['followers'] = str(num)  # always store as number string

def _update_follower_history(platform, phone, accounts):
    """Store follower + views counts and compute 24h deltas."""
    now = time.time()
    hist_key = f'{platform}:fh:{phone}'
    history  = _rget(hist_key) or {}
    deltas   = {}

    for acc_num, acc in accounts.items():
        followers = _parse_followers(acc.get('followers', 0))
        views     = int(acc.get('views', 0) or 0)
        acc_hist  = history.get(str(acc_num), [])
        # Add current entry with both followers and views
        acc_hist.append({'ts': now, 'v': followers, 'views': views})
        # Keep last 25h only
        acc_hist = [e for e in acc_hist if now - e['ts'] < 25 * 3600]
        # Find entry closest to 24h ago
        target = now - 86400
        closest = min(acc_hist, key=lambda e: abs(e['ts'] - target), default=None)
        if closest and abs(closest['ts'] - target) < 7200:
            deltas[str(acc_num)] = {
                'followers': followers - closest['v'],
                'views':     views - closest.get('views', 0),
            }
        else:
            deltas[str(acc_num)] = {'followers': None, 'views': None}
        history[str(acc_num)] = acc_hist

    _rset(hist_key, history)
    return deltas

# ─── DAILY FOLLOWER LOG (per container per day) ─────────────────────────────
def _log_daily_followers(platform, phone, accounts):
    """Store one entry per day per container. Keeps last 60 days."""
    today = datetime.date.today().isoformat()
    for acc_num, acc in accounts.items():
        flw = _parse_followers(acc.get('followers', 0))
        if flw <= 0:
            continue
        key = f'{platform}:flw_daily:{phone}:{acc_num}'
        log = _rget(key) or {}
        log[today] = flw
        # Keep last 60 days
        if len(log) > 60:
            oldest = sorted(log.keys())[:-60]
            for d in oldest:
                del log[d]
        _rset(key, log)

def _get_daily_followers(platform, phone, container):
    """Return sorted list of {date, followers} for one container."""
    key = f'{platform}:flw_daily:{phone}:{container}'
    log = _rget(key) or {}
    return [{'date': d, 'followers': v} for d, v in sorted(log.items())]

# ─── FIRST-SEEN TRACKING (account created date) ──────────────────────────────
def _track_first_seen(platform, phone, accounts):
    """Record the first time we see each account (approximate created date)."""
    now = time.time()
    key = f'{platform}:created:{phone}'
    created = _rget(key) or {}
    changed = False
    for acc_num in accounts:
        if str(acc_num) not in created:
            created[str(acc_num)] = now
            changed = True
    if changed: _rset(key, created)
    return created

def _get_first_seen(platform, phone):
    return _rget(f'{platform}:created:{phone}') or {}

# ─── STATUS SAVE/LOAD ─────────────────────────────────────────────────────────
def _save(platform, phone, data):
    store = _fb if platform == 'fb' else _ig
    existing = store.get(phone, {})

    # ALWAYS carry forward existing accounts — never wipe them
    if existing.get('accounts'):
        merged = dict(existing['accounts'])  # start with everything we know
        incoming = data.get('accounts') or {}
        for acc_num, acc in incoming.items():
            key = str(acc_num)
            if key not in merged:
                merged[key] = dict(acc)
                continue
            for field, val in acc.items():
                if val is None:
                    continue
                sval = str(val).strip().lower() if isinstance(val, str) else None
                if field in ('followers', 'name') and sval in ('', '?', '—'):
                    continue
                if field == 'views':
                    try:
                        if int(val) <= 0 and int(merged[key].get('views', 0) or 0) > 0:
                            continue
                    except Exception:
                        pass
                merged[key][field] = val
        data['accounts'] = merged
    # If no existing accounts but new data has some, keep those
    elif not data.get('accounts') and existing.get('accounts'):
        data['accounts'] = existing['accounts']

    # Preserve reels_posted/verified — always take the higher value
    for field in ('reels_posted', 'reels_verified'):
        old_val = existing.get(field) or 0
        new_val = data.get(field) or 0
        data[field] = max(old_val, new_val)

    # Preserve last_update if new data doesn't have it
    if not data.get('last_update') and existing.get('last_update'):
        data['last_update'] = existing['last_update']

    store[phone] = data
    _rset(f'{platform}:status:{phone}', data)

def _get(platform, phone):
    store = _fb if platform == 'fb' else _ig
    return store.get(phone)

# ─── DAILY SNAPSHOTS ─────────────────────────────────────────────────────────
_today = datetime.date.today().isoformat()

def _maybe_snapshot():
    global _today
    today = datetime.date.today().isoformat()
    if today == _today: return
    yesterday, _today = _today, today
    try:
        snap = {}
        for platform, store in [('fb', _fb), ('ig', _ig)]:
            snap[platform] = {}
            for phone, s in store.items():
                snap[platform][phone] = {
                    'reels_posted': s.get('reels_posted', 0),
                    'reels_verified': s.get('reels_verified', 0),
                    'accounts': s.get('accounts', {}),
                    'date': yesterday,
                }
        _rset(f'snapshot:{yesterday}', snap)
        print(f'[snapshot] Saved {yesterday}')
        all_snaps = sorted(_rkeys('snapshot:*'))
        for old in all_snaps[:-30]: _rdel(old)
    except Exception as e:
        print(f'[snapshot] {e}')

def _list_snapshots():
    return sorted([k.replace('snapshot:', '') for k in _rkeys('snapshot:*')], reverse=True)

# ─── CRANE ────────────────────────────────────────────────────────────────────
def _save_crane_q():
    _rset('crane:queue', _crane_q)

def _get_crane_ctrs(platform, phone):
    """Read the current roster from its own durable key, never a shared map."""
    store = _crane_ctrs_fb if platform == 'fb' else _crane_ctrs_ig
    if _using_persistent_store():
        saved = _rget(_roster_phone_key(platform, phone))
        if isinstance(saved, list):
            roster = _normalize_roster(saved)
            store[phone] = roster
            return roster
    return _normalize_roster(store.get(phone, []))

def _save_crane_ctrs(platform, phone, data, source='unknown'):
    store = _crane_ctrs_fb if platform == 'fb' else _crane_ctrs_ig
    new_roster = _normalize_roster(data)
    # Fetch this exact key first so UUID invalidation remains correct across
    # worker restarts or multiple Railway processes.
    old_roster = _get_crane_ctrs(platform, phone)
    if isinstance(data, list) and data and not new_roster and old_roster:
        # A non-empty payload that parses to zero containers is almost certainly
        # a transient/formatting error; keep the last valid roster.
        new_roster = old_roster

    old_by_name = {c['name']: c for c in old_roster}
    new_by_name = {c['name']: c for c in new_roster}

    # Same container number + different UUID means a delete/re-create.
    # Never let the old account stats follow the new container generation.
    for name, new_ctr in new_by_name.items():
        old_ctr = old_by_name.get(name)
        if old_ctr and old_ctr.get('uuid') and new_ctr.get('uuid') and old_ctr['uuid'].lower() != new_ctr['uuid'].lower():
            _invalidate_container_data(platform, phone, name)

    store[phone] = new_roster
    signature = _roster_signature(new_roster)
    meta_key = _roster_meta_key(platform, phone)
    old_meta = _rget(meta_key) or {}
    now = time.time()
    meta = {
        'signature': signature,
        'container_count': len(new_roster),
        'last_seen': now,
        'updated_at': now if old_meta.get('signature') != signature else old_meta.get('updated_at', now),
        'source': source,
    }
    _rset(_roster_phone_key(platform, phone), new_roster)
    _rset(meta_key, meta)
    return new_roster, meta

# ─── HTTP HANDLER ─────────────────────────────────────────────────────────────
class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args): pass

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def out(self, data, status=200):
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(body)

    def redirect(self, location):
        """Send visitors straight on while keeping analytics responses private."""
        self.send_response(302)
        self.send_header('Location', location)
        self.send_header('Cache-Control', 'no-store, private')
        self.send_header('Referrer-Policy', 'strict-origin-when-cross-origin')
        self.end_headers()

    def do_GET(self):
        _maybe_snapshot()
        p = urlparse(self.path).path

        # Public numbered tracking links.  Put /t/oopsie/1 in Phone 1's
        # Facebook bio (and so on); it records a page view, then immediately
        # opens that phone's existing Oopsie page.  The Oopsie page itself is
        # unchanged.
        if p.startswith('/t/oopsie/'):
            parts = p.split('/')
            phone = parts[3] if len(parts) >= 4 else ''
            if phone not in OOPSIE_PAGES:
                self.out({'ok': False, 'error': 'Unknown Oopsie phone link'}, 404); return
            if len(parts) == 4:
                _record_oopsie_event(phone, 'views')
                self.redirect(OOPSIE_PAGES[phone]); return
            if len(parts) == 5 and parts[4] == 'click':
                destination = OOPSIE_CLICK_TARGETS.get(phone, '')
                if not _valid_http_url(destination):
                    self.out({
                        'ok': False,
                        'error': 'Click tracking is not configured for this phone yet',
                    }, 409)
                    return
                _record_oopsie_event(phone, 'clicks')
                self.redirect(destination); return
            self.out({'ok': False, 'error': 'Unknown tracking link'}, 404); return

        if p == '/health':
            self.out({
                'ok': True,
                'version': 'v5',
                'schema': 'phone-platform-v5',
                'roster_storage': 'one durable record per platform and phone',
                'storage': _storage_label(),
                'persistent': _using_persistent_store(),
                'oopsie_tracking': True,
            })
            return

        if p == '/analytics/oopsie':
            self.out(_oopsie_analytics()); return

        # Permanently purge every old roster namespace.  The current v4 phone
        # reporter may repopulate the new v5 store.
        # updates may repopulate the new platform-specific store.
        if p == '/admin/reset-containers':
            with _lock:
                _crane_ctrs_fb.clear()
                _crane_ctrs_ig.clear()
                _purge_retired_roster_data()
            self.out({'ok': True, 'msg': 'All legacy rosters permanently purged. Waiting for v4 phone reporters.'})
            return

        # Status endpoints
        for platform in ('fb', 'ig'):
            if p.startswith(f'/status/{platform}/'):
                phone = p.split('/')[-1]
                data  = _get(platform, phone)
                if data:
                    # Enrich with first-seen dates
                    created = _get_first_seen(platform, phone)
                    if data.get('accounts'):
                        for num, acc in data['accounts'].items():
                            acc['created_at'] = created.get(str(num))
                self.out(data)
                return

            if p == f'/all/{platform}':
                store = _fb if platform == 'fb' else _ig
                result = {}
                for phone, data in store.items():
                    created = _get_first_seen(platform, phone)
                    if data.get('accounts'):
                        for num, acc in data['accounts'].items():
                            acc['created_at'] = created.get(str(num))
                    result[phone] = data
                self.out(result)
                return

        # Follower daily history: /followers_history/fb/1/5
        for platform in ('fb', 'ig'):
            if p.startswith(f'/followers_history/{platform}/'):
                parts = p.split('/')
                if len(parts) >= 5:
                    phone, container = parts[3], parts[4]
                    self.out({'history': _get_daily_followers(platform, phone, container)}); return
                self.out({'history': []}); return

        # Snapshots
        if p == '/snapshots':
            self.out({'dates': _list_snapshots()}); return
        if p.startswith('/snapshot/'):
            self.out(_rget(f'snapshot:{p.split("/")[-1]}') or {}); return

        # Crane
        if p == '/crane/queue':
            recent = sorted(_crane_q.values(), key=lambda x: x.get('created',0), reverse=True)[:30]
            self.out({'commands': recent}); return
        if p == '/crane/pending':
            self.out({'commands': [v for v in _crane_q.values() if v['status']=='pending']}); return

        # Platform-aware container endpoints: /crane/containers/fb/1 or /crane/containers/ig/1
        for plat in ('fb', 'ig'):
            if p.startswith(f'/crane/containers/{plat}/'):
                phone = p.split('/')[-1]
                meta = _rget(_roster_meta_key(plat, phone)) or {}
                self.out({'phone': phone, 'containers': _get_crane_ctrs(plat, phone), 'meta': meta}); return

        # The old shared endpoint is intentionally gone.
        if p.startswith('/crane/containers/'):
            self.out({'ok': False, 'error': 'Legacy roster endpoint retired'}, 410); return

        # Platform-aware state: /crane/state/fb/1 or /crane/state/ig/1
        for plat in ('fb', 'ig'):
            if p.startswith(f'/crane/state/{plat}/'):
                phone = p.split('/')[-1]
                self.out({'state': _rget(f'ctr_state:{plat}:{phone}') or {}}); return

        if p.startswith('/crane/state/'):
            phone = p.split('/')[-1]
            self.out({'state': _rget(f'ctr_state:fb:{phone}') or {}}); return

        self.out({})

    def do_POST(self):
        _maybe_snapshot()
        p = urlparse(self.path).path
        length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(length)
        try: data = json.loads(body) if body else {}
        except: data = {}

        # Platform status updates
        for platform in ('fb', 'ig'):
            if p.startswith(f'/update/{platform}/'):
                phone = p.split('/')[-1]
                with _lock:
                    # Compute 24h follower + views deltas
                    if data.get('accounts'):
                        _normalize_followers(data['accounts'])  # convert K/M to numbers
                        _log_daily_followers(platform, phone, data['accounts'])  # daily history
                        deltas   = _update_follower_history(platform, phone, data['accounts'])
                        created  = _track_first_seen(platform, phone, data['accounts'])
                        for acc_num, acc in data['accounts'].items():
                            d = deltas.get(str(acc_num), {})
                            acc['delta_24h']       = d.get('followers') if isinstance(d, dict) else d
                            acc['views_delta_24h'] = d.get('views') if isinstance(d, dict) else None
                            acc['created_at']      = created.get(str(acc_num))
                    _save(platform, phone, data)
                self.out({'ok': True}); return

        # Account creator notification.  This is deliberately separate from a
        # roster update: Crane is still the only authority for membership.
        if p == '/account/created':
            platform = data.get('platform', 'fb')
            phone = str(data.get('phone', '')).strip()
            container = str(data.get('container', '')).strip()
            if platform not in ('fb', 'ig') or not phone.isdigit() or not container.isdigit():
                self.out({'ok': False, 'error': 'platform, phone and numeric container are required'}, 400); return
            with _lock:
                existing = _get(platform, phone) or {}
                accounts = dict(existing.get('accounts') or {})
                account = dict(accounts.get(container) or {})
                name = str(data.get('name', '')).strip()
                if name and name not in ('?', '—'):
                    account['name'] = name
                account['container'] = int(container)
                accounts[container] = account
                existing['accounts'] = accounts
                _save(platform, phone, existing)

                created = _get_first_seen(platform, phone)
                if container not in created:
                    created[container] = time.time()
                    _rset(f'{platform}:created:{phone}', created)
            self.out({'ok': True}); return

        # Followers-only update from phone_reporter
        for platform in ('fb', 'ig'):
            if p.startswith(f'/update/followers/{platform}/'):
                phone = p.split('/')[-1]
                with _lock:
                    existing = _get(platform, phone) or {}
                    accs = existing.get('accounts', {})
                    for acc_num, flw in (data.get('followers') or {}).items():
                        if str(acc_num) not in accs:
                            accs[str(acc_num)] = {}
                        accs[str(acc_num)]['followers'] = flw
                        accs[str(acc_num)]['followers_ts'] = time.time()
                    existing['accounts'] = accs
                    # Update 24h history
                    deltas = _update_follower_history(platform, phone, accs)
                    for acc_num, acc in accs.items():
                        d = deltas.get(str(acc_num), {})
                        acc['delta_24h'] = d.get('followers') if isinstance(d, dict) else d
                    _save(platform, phone, existing)
                self.out({'ok': True}); return

        # Crane
        if p == '/crane/command':
            cmd_id = str(uuid.uuid4())[:8]
            with _lock:
                _crane_q[cmd_id] = {
                    'id': cmd_id, 'phone': data.get('phone',1),
                    'action': data.get('action','list'),
                    'name': data.get('name',''),
                    'container_num': data.get('container_num',''),
                    'status': 'pending', 'result': '', 'created': time.time(),
                }
                done = sorted([v for v in _crane_q.values() if v['status']!='pending'],
                              key=lambda x:x.get('created',0), reverse=True)
                for old in done[100:]: _crane_q.pop(old['id'], None)
                _save_crane_q()
            self.out({'ok': True, 'id': cmd_id}); return

        if p.startswith('/crane/result/'):
            cmd_id = p.split('/')[-1]
            with _lock:
                if cmd_id in _crane_q:
                    _crane_q[cmd_id]['status']  = data.get('status','done')
                    _crane_q[cmd_id]['result']  = data.get('result','')
                    _crane_q[cmd_id]['done_at'] = time.time()
                    _save_crane_q()
            self.out({'ok': True}); return

        # Platform-aware container update: /crane/containers/fb/1 or /crane/containers/ig/1
        for plat in ('fb', 'ig'):
            if p.startswith(f'/crane/containers/{plat}/'):
                phone = p.split('/')[-1]
                # Reject reporters from every older release.  This is what
                # stops their shared/stale values from ever reappearing.
                if data.get('source') != 'phone_reporter_v4':
                    self.out({'ok': False, 'error': 'Reporter upgrade required'}, 409); return
                with _lock:
                    roster, meta = _save_crane_ctrs(plat, phone, data.get('containers',[]), 'phone_reporter_v4')
                self.out({'ok': True, 'containers': roster, 'meta': meta}); return

        # Never accept the original shared-list endpoint again.
        if p.startswith('/crane/containers/'):
            self.out({'ok': False, 'error': 'Legacy roster endpoint retired'}, 410); return

        # Platform-aware state: /crane/state/fb/1 or /crane/state/ig/1
        for plat in ('fb', 'ig'):
            if p.startswith(f'/crane/state/{plat}/'):
                phone = p.split('/')[-1]
                _rset(f'ctr_state:{plat}:{phone}', data.get('state',{}))
                self.out({'ok': True}); return

        if p.startswith('/crane/state/'):
            phone = p.split('/')[-1]
            _rset(f'ctr_state:fb:{phone}', data.get('state',{}))
            self.out({'ok': True}); return

        self.out({})

if __name__ == '__main__':
    _init_local_store()
    _load_all()
    port = int(os.environ.get('PORT', 5050))
    print(f'Farm Dashboard v5 | Port {port} | Storage: {_storage_label()}')
    HTTPServer(('0.0.0.0', port), Handler).serve_forever()
