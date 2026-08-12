"""
Farm Dashboard Server — persistent storage via Upstash Redis REST API.

Setup:
  1. upstash.com → create free Redis DB → copy REST URL + REST TOKEN
  2. Railway Variables → UPSTASH_URL=... UPSTASH_TOKEN=...
  3. Redeploy

Data persists forever across all redeploys.
Daily snapshots saved automatically at midnight.
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

# ─── UPSTASH CONFIG ──────────────────────────────────────────────────────────
UPSTASH_URL   = os.environ.get('UPSTASH_URL', '')    # e.g. https://xxx.upstash.io
UPSTASH_TOKEN = os.environ.get('UPSTASH_TOKEN', '')  # Bearer token

def _redis(method, path, body=None):
    """Call Upstash REST API. Returns parsed result or None."""
    if not UPSTASH_URL or not UPSTASH_TOKEN:
        return None
    url     = UPSTASH_URL.rstrip('/') + path
    headers = {'Authorization': f'Bearer {UPSTASH_TOKEN}',
               'Content-Type': 'application/json'}
    try:
        if _HAS_REQUESTS:
            if method == 'GET':
                r = _req.get(url, headers=headers, timeout=5)
            else:
                r = _req.post(url, headers=headers,
                              data=body.encode() if body else b'', timeout=5)
            return r.json().get('result')
        else:
            req = _urllib.Request(url, data=(body.encode() if body else None),
                                  headers=headers, method=method)
            with _urllib.urlopen(req, timeout=5) as resp:
                return json.loads(resp.read()).get('result')
    except Exception as e:
        print(f'[redis] {method} {path} error: {e}')
        return None

def _rset(key, value):
    """Store a Python object as JSON string in Redis."""
    encoded = json.dumps(value)
    _redis('POST', f'/set/{key}', encoded)

def _rget(key):
    """Get a Python object from Redis (was stored as JSON string)."""
    raw = _redis('GET', f'/get/{key}')
    if raw is None: return None
    try:    return json.loads(raw)
    except: return raw

def _rdel(key):
    _redis('POST', f'/del/{key}')

def _rkeys(pattern):
    """Get all keys matching pattern."""
    raw = _redis('POST', f'/keys/{pattern}')
    return raw if isinstance(raw, list) else []

# ─── IN-MEMORY CACHE (warmed from Redis on startup) ──────────────────────────
_status     = {}   # phone -> status dict
_crane_q    = {}   # cmd_id -> command
_crane_ctrs = {}   # phone -> [containers]
_lock       = threading.Lock()

def _load_all():
    """Restore all data from Redis on startup."""
    if not UPSTASH_URL:
        print('[startup] No UPSTASH_URL set — running in-memory only (data lost on redeploy)')
        return
    print('[startup] Loading data from Upstash Redis...')
    # Load phone statuses
    keys = _rkeys('status:*')
    for key in keys:
        phone = key.replace('status:', '')
        data = _rget(key)
        if data: _status[phone] = data
    # Load crane queue
    q = _rget('crane:queue')
    if q: _crane_q.update(q)
    # Load container cache
    c = _rget('crane:containers')
    if c: _crane_ctrs.update(c)
    print(f'[startup] Loaded {len(_status)} phones, {len(_crane_q)} crane commands')

def _save_status(phone, data):
    _status[phone] = data
    _rset(f'status:{phone}', data)

def _save_crane_q():
    _rset('crane:queue', _crane_q)

def _save_crane_ctrs():
    _rset('crane:containers', _crane_ctrs)

# ─── DAILY SNAPSHOTS ─────────────────────────────────────────────────────────
_today = datetime.date.today().isoformat()

def _maybe_snapshot():
    global _today
    today = datetime.date.today().isoformat()
    if today == _today:
        return
    yesterday = _today
    _today = today
    try:
        snap = {}
        for phone, s in _status.items():
            snap[phone] = {
                'reels_posted':   s.get('reels_posted', 0),
                'reels_verified': s.get('reels_verified', 0),
                'accounts':       s.get('accounts', {}),
                'date':           yesterday,
            }
        _rset(f'snapshot:{yesterday}', snap)
        print(f'[snapshot] Saved {yesterday}')
        # Keep last 30 days — delete older ones
        all_snaps = _rkeys('snapshot:*')
        for old_key in sorted(all_snaps)[:-30]:
            _rdel(old_key)
    except Exception as e:
        print(f'[snapshot] Error: {e}')

def _list_snapshots():
    keys = _rkeys('snapshot:*')
    dates = sorted([k.replace('snapshot:', '') for k in keys], reverse=True)
    return dates

def _load_snapshot(date):
    return _rget(f'snapshot:{date}')

# ─── HTTP HANDLER ─────────────────────────────────────────────────────────────
class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args): pass

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def json_out(self, data, status=200):
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        _maybe_snapshot()
        path = urlparse(self.path).path

        if path.startswith('/status/'):
            phone = path.split('/')[-1]
            self.json_out(_status.get(phone))
            return

        if path == '/all':
            self.json_out(_status)
            return

        if path == '/snapshots':
            self.json_out({'dates': _list_snapshots()})
            return

        if path.startswith('/snapshot/'):
            date = path.split('/')[-1]
            snap = _load_snapshot(date)
            self.json_out(snap if snap else {})
            return

        if path == '/crane/queue':
            recent = sorted(_crane_q.values(),
                            key=lambda x: x.get('created', 0), reverse=True)[:30]
            self.json_out({'commands': recent})
            return

        if path == '/crane/pending':
            pending = [v for v in _crane_q.values() if v['status'] == 'pending']
            self.json_out({'commands': pending})
            return

        if path.startswith('/crane/containers/'):
            phone = path.split('/')[-1]
            self.json_out({'phone': phone, 'containers': _crane_ctrs.get(phone, [])})
            return

        self.json_out({})

    def do_POST(self):
        _maybe_snapshot()
        path = urlparse(self.path).path
        length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(length)
        try:    data = json.loads(body) if body else {}
        except: data = {}

        if path.startswith('/update/'):
            phone = path.split('/')[-1]
            with _lock:
                _save_status(phone, data)
            self.json_out({'ok': True})
            return

        if path == '/crane/command':
            cmd_id = str(uuid.uuid4())[:8]
            with _lock:
                _crane_q[cmd_id] = {
                    'id':            cmd_id,
                    'phone':         data.get('phone', 1),
                    'action':        data.get('action', 'list'),
                    'name':          data.get('name', ''),
                    'container_num': data.get('container_num', ''),
                    'status':        'pending',
                    'result':        '',
                    'created':       time.time(),
                }
                done = sorted(
                    [v for v in _crane_q.values() if v['status'] != 'pending'],
                    key=lambda x: x.get('created', 0), reverse=True
                )
                for old in done[100:]:
                    _crane_q.pop(old['id'], None)
                _save_crane_q()
            self.json_out({'ok': True, 'id': cmd_id})
            return

        if path.startswith('/crane/result/'):
            cmd_id = path.split('/')[-1]
            with _lock:
                if cmd_id in _crane_q:
                    _crane_q[cmd_id]['status']  = data.get('status', 'done')
                    _crane_q[cmd_id]['result']  = data.get('result', '')
                    _crane_q[cmd_id]['done_at'] = time.time()
                    _save_crane_q()
            self.json_out({'ok': True})
            return

        if path.startswith('/crane/containers/'):
            phone = path.split('/')[-1]
            with _lock:
                _crane_ctrs[phone] = data.get('containers', [])
                _save_crane_ctrs()
            self.json_out({'ok': True})
            return

        self.json_out({})


if __name__ == '__main__':
    _load_all()
    port = int(os.environ.get('PORT', 5050))
    print(f'Farm Dashboard on port {port} | Upstash: {"connected" if UPSTASH_URL else "NOT SET"}')
    HTTPServer(('0.0.0.0', port), Handler).serve_forever()
