"""
Farm Dashboard Server v2 — FB + IG platform separation
Persistent via Upstash Redis. 24h follower delta. First-seen date tracking.

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

import json, os, uuid, time, datetime, threading
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
    _redis('POST', f'/set/{key}', json.dumps(value).encode())

def _rget(key):
    raw = _redis('GET', f'/get/{key}')
    if raw is None: return None
    try: return json.loads(raw)
    except: return raw

def _rdel(key):
    _redis('POST', f'/del/{key}')

def _rkeys(pattern):
    raw = _redis('POST', f'/keys/{pattern}')
    return raw if isinstance(raw, list) else []

# ─── IN-MEMORY CACHE ─────────────────────────────────────────────────────────
_fb   = {}   # phone -> status
_ig   = {}   # phone -> status
_crane_q    = {}
_crane_ctrs = {}
_lock = threading.Lock()

def _load_all():
    if not UPSTASH_URL:
        print('[startup] No Upstash — running in-memory only')
        return
    print('[startup] Loading from Upstash...')
    for key in _rkeys('fb:status:*'):
        phone = key.replace('fb:status:', '')
        d = _rget(key)
        if d: _fb[phone] = d
    for key in _rkeys('ig:status:*'):
        phone = key.replace('ig:status:', '')
        d = _rget(key)
        if d: _ig[phone] = d
    q = _rget('crane:queue')
    if q: _crane_q.update(q)
    c = _rget('crane:containers')
    if c: _crane_ctrs.update(c)
    print(f'[startup] FB phones: {list(_fb.keys())} | IG phones: {list(_ig.keys())}')

# ─── FOLLOWER HISTORY (24h delta) ────────────────────────────────────────────
def _parse_followers(raw):
    s = str(raw).strip().upper().replace(',','').replace(' ','')
    if not s or s in ('?','—',''):
        return 0
    try:
        if s.endswith('K'): return int(float(s[:-1]) * 1000)
        if s.endswith('M'): return int(float(s[:-1]) * 1_000_000)
        return int(float(s))
    except: return 0

def _update_follower_history(platform, phone, accounts):
    """Store follower counts and compute 24h deltas. Returns dict of deltas."""
    now = time.time()
    hist_key = f'{platform}:fh:{phone}'
    history  = _rget(hist_key) or {}
    deltas   = {}

    for acc_num, acc in accounts.items():
        followers = _parse_followers(acc.get('followers', 0))
        acc_hist  = history.get(str(acc_num), [])
        # Add current entry
        acc_hist.append({'ts': now, 'v': followers})
        # Keep last 25h only
        acc_hist = [e for e in acc_hist if now - e['ts'] < 25 * 3600]
        # Find entry closest to 24h ago (within 2h tolerance)
        target = now - 86400
        closest = min(acc_hist, key=lambda e: abs(e['ts'] - target), default=None)
        if closest and abs(closest['ts'] - target) < 7200:
            deltas[str(acc_num)] = followers - closest['v']
        else:
            deltas[str(acc_num)] = None
        history[str(acc_num)] = acc_hist

    _rset(hist_key, history)
    return deltas

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

def _save_crane_ctrs():
    _rset('crane:containers', _crane_ctrs)

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

    def do_GET(self):
        _maybe_snapshot()
        p = urlparse(self.path).path

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
        if p.startswith('/crane/containers/'):
            phone = p.split('/')[-1]
            self.out({'phone': phone, 'containers': _crane_ctrs.get(phone, [])}); return
        if p.startswith('/crane/state/'):
            phone = p.split('/')[-1]
            self.out({'state': _rget(f'ctr_state:{phone}') or {}}); return

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
                    # Compute 24h follower deltas
                    if data.get('accounts'):
                        deltas   = _update_follower_history(platform, phone, data['accounts'])
                        created  = _track_first_seen(platform, phone, data['accounts'])
                        for acc_num, acc in data['accounts'].items():
                            acc['delta_24h']   = deltas.get(str(acc_num))
                            acc['created_at']  = created.get(str(acc_num))
                    _save(platform, phone, data)
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

        if p.startswith('/crane/containers/'):
            phone = p.split('/')[-1]
            with _lock:
                _crane_ctrs[phone] = data.get('containers',[])
                _save_crane_ctrs()
            self.out({'ok': True}); return

        if p.startswith('/crane/state/'):
            phone = p.split('/')[-1]
            _rset(f'ctr_state:{phone}', data.get('state',{}))
            self.out({'ok': True}); return

        self.out({})

if __name__ == '__main__':
    _load_all()
    port = int(os.environ.get('PORT', 5050))
    print(f'Farm Dashboard v2 | Port {port} | Upstash: {"OK" if UPSTASH_URL else "NOT SET"}')
    HTTPServer(('0.0.0.0', port), Handler).serve_forever()
