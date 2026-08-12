import json, os, glob, uuid, time
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse

STATUS_DIR = os.environ.get('STATUS_DIR', '/tmp/dashboard')
CRANE_DIR  = os.environ.get('CRANE_DIR',  '/tmp/crane')

# ─── CRANE HELPERS ───────────────────────────────────────────────────────────
def crane_load_queue():
    p = os.path.join(CRANE_DIR, 'queue.json')
    if os.path.exists(p):
        try:
            with open(p) as f: return json.load(f)
        except: pass
    return {}

def crane_save_queue(q):
    os.makedirs(CRANE_DIR, exist_ok=True)
    with open(os.path.join(CRANE_DIR, 'queue.json'), 'w') as f:
        json.dump(q, f)

def crane_load_containers(phone):
    p = os.path.join(CRANE_DIR, f'containers_{phone}.json')
    if os.path.exists(p):
        try:
            with open(p) as f: return json.load(f)
        except: pass
    return []

def crane_save_containers(phone, data):
    os.makedirs(CRANE_DIR, exist_ok=True)
    with open(os.path.join(CRANE_DIR, f'containers_{phone}.json'), 'w') as f:
        json.dump(data, f)

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Farm Dashboard</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap" rel="stylesheet">
<style>
*, *::before, *::after { margin: 0; padding: 0; box-sizing: border-box; }
:root {
  --bg: #05050f; --surface: #0a0a1a; --surface2: #0f0f22;
  --border: #1c1c35; --border2: #252545;
  --accent: #7c3aed; --accent2: #a855f7; --accent3: #c084fc;
  --blue: #3b82f6; --green: #10b981; --red: #ef4444;
  --yellow: #f59e0b; --orange: #f97316;
  --text: #e2e8f0; --text2: #94a3b8; --text3: #475569;
}
body { background:var(--bg); color:var(--text); font-family:'Inter',sans-serif; min-height:100vh; overflow-x:hidden; }
body::before { content:''; position:fixed; top:-200px; left:50%; transform:translateX(-50%); width:800px; height:400px; background:radial-gradient(ellipse,rgba(124,58,237,.08) 0%,transparent 70%); pointer-events:none; z-index:0; }

header { position:sticky; top:0; z-index:100; background:rgba(5,5,15,.9); backdrop-filter:blur(20px); border-bottom:1px solid var(--border); padding:0 28px; height:60px; display:flex; align-items:center; justify-content:space-between; }
.logo { display:flex; align-items:center; gap:10px; font-size:15px; font-weight:800; }
.logo-icon { width:30px; height:30px; background:linear-gradient(135deg,var(--accent),var(--accent2)); border-radius:8px; display:flex; align-items:center; justify-content:center; font-size:14px; box-shadow:0 0 20px rgba(124,58,237,.4); }
.header-stats { display:flex; align-items:center; gap:6px; }
.hstat { display:flex; align-items:center; gap:10px; background:var(--surface); border:1px solid var(--border); border-radius:10px; padding:7px 14px; }
.hstat-val { font-size:18px; font-weight:800; background:linear-gradient(135deg,var(--accent2),var(--accent3)); -webkit-background-clip:text; -webkit-text-fill-color:transparent; }
.hstat-label { font-size:10px; color:var(--text3); font-weight:500; text-transform:uppercase; letter-spacing:.5px; }
.divider { width:1px; height:30px; background:var(--border); margin:0 4px; }


.btn { background:linear-gradient(135deg,var(--accent),var(--accent2)); color:white; border:none; padding:8px 18px; border-radius:10px; font-size:12px; font-weight:600; cursor:pointer; font-family:inherit; box-shadow:0 0 20px rgba(124,58,237,.3); transition:opacity .2s; }
.btn:hover { opacity:.85; }
.btn-sm { padding:5px 12px; font-size:11px; border-radius:8px; }
.btn-red { background:linear-gradient(135deg,#dc2626,#ef4444); box-shadow:0 0 12px rgba(239,68,68,.3); }
.btn-green { background:linear-gradient(135deg,#059669,#10b981); box-shadow:0 0 12px rgba(16,185,129,.3); }
.btn-blue { background:linear-gradient(135deg,#2563eb,#3b82f6); box-shadow:0 0 12px rgba(59,130,246,.3); }
.btn-yellow { background:linear-gradient(135deg,#d97706,#f59e0b); box-shadow:0 0 12px rgba(245,158,11,.3); }
.btn-ghost { background:transparent; border:1px solid var(--border2); color:var(--text2); box-shadow:none; }
.btn-ghost:hover { background:var(--surface2); opacity:1; }

main { padding:24px 28px; max-width:1600px; margin:0 auto; position:relative; z-index:1; }
.grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(340px,1fr)); gap:16px; }

.card { background:var(--surface); border:1px solid var(--border); border-radius:16px; overflow:hidden; position:relative; transition:border-color .3s,transform .2s; }
.card::before { content:''; position:absolute; top:0; left:0; right:0; height:1px; background:linear-gradient(90deg,transparent,rgba(124,58,237,.5),transparent); opacity:0; transition:opacity .3s; }
.card:hover { transform:translateY(-2px); border-color:var(--border2); }
.card:hover::before { opacity:1; }
.card.running { border-color:rgba(59,130,246,.3); }
.card.running::before { opacity:1; background:linear-gradient(90deg,transparent,rgba(59,130,246,.5),transparent); }
.card.error { border-color:rgba(239,68,68,.3); }

.card-head { padding:16px 18px; display:flex; align-items:center; justify-content:space-between; border-bottom:1px solid var(--border); background:linear-gradient(180deg,var(--surface2) 0%,var(--surface) 100%); }
.phone-label { display:flex; align-items:center; gap:12px; }
.phone-num { width:40px; height:40px; border-radius:12px; background:linear-gradient(135deg,rgba(124,58,237,.2),rgba(168,85,247,.1)); border:1px solid rgba(124,58,237,.3); display:flex; align-items:center; justify-content:center; font-size:16px; font-weight:900; color:var(--accent3); }
.phone-meta { line-height:1; }
.phone-title { font-size:14px; font-weight:700; }
.phone-ip { font-size:11px; color:var(--text3); margin-top:3px; font-family:monospace; }
.head-right { display:flex; align-items:center; gap:6px; }
.pill { font-size:11px; font-weight:600; padding:4px 12px; border-radius:20px; display:flex; align-items:center; gap:5px; }
.pill-running { background:rgba(59,130,246,.1); color:#60a5fa; border:1px solid rgba(59,130,246,.2); }
.pill-idle { background:rgba(71,85,105,.15); color:var(--text3); border:1px solid var(--border); }
.pill-error { background:rgba(239,68,68,.1); color:#f87171; border:1px solid rgba(239,68,68,.2); }
.dot { width:6px; height:6px; border-radius:50%; background:#3b82f6; animation:pulse 2s infinite; }
@keyframes pulse { 0%,100%{opacity:1;transform:scale(1)} 50%{opacity:.4;transform:scale(.8)} }

.card-body { padding:16px 18px; }
.prog-row { display:flex; justify-content:space-between; align-items:center; margin-bottom:7px; }
.prog-label { font-size:11px; color:var(--text3); font-weight:500; }
.prog-val { font-size:11px; color:var(--accent3); font-weight:700; }
.prog-bar { height:4px; background:var(--border); border-radius:4px; margin-bottom:16px; overflow:hidden; }
.prog-fill { height:100%; border-radius:4px; background:linear-gradient(90deg,var(--accent),var(--accent2),var(--accent3)); box-shadow:0 0 8px rgba(124,58,237,.5); }

.stats-row { display:grid; grid-template-columns:1fr 1fr 1fr; gap:8px; margin-bottom:14px; }
.stat-box { background:var(--bg); border:1px solid var(--border); border-radius:10px; padding:10px 8px; text-align:center; }
.stat-val { font-size:20px; font-weight:800; background:linear-gradient(135deg,var(--accent2),var(--accent3)); -webkit-background-clip:text; -webkit-text-fill-color:transparent; line-height:1; }
.stat-lbl { font-size:9px; color:var(--text3); text-transform:uppercase; letter-spacing:.8px; margin-top:4px; font-weight:500; }

.accs-head { display:flex; justify-content:space-between; align-items:center; margin-bottom:8px; padding-top:14px; border-top:1px solid var(--border); }
.accs-title { font-size:10px; color:var(--text3); font-weight:600; text-transform:uppercase; letter-spacing:.8px; }
.accs-count { font-size:10px; color:var(--accent3); font-weight:700; }
.accs-list { display:flex; flex-direction:column; gap:3px; max-height:160px; overflow-y:auto; scrollbar-width:thin; scrollbar-color:var(--border) transparent; }
.acc-row { display:flex; justify-content:space-between; align-items:center; padding:6px 10px; border-radius:8px; border:1px solid transparent; background:var(--bg); transition:border-color .2s; }
.acc-row:hover { border-color:var(--border); }
.acc-row.active { border-color:rgba(124,58,237,.4); background:rgba(124,58,237,.07); }
.acc-name { font-size:11px; color:var(--text2); font-weight:500; }
.acc-row.active .acc-name { color:var(--accent3); font-weight:600; }
.acc-followers { font-size:11px; font-weight:700; color:var(--accent2); }

/* CRANE PANEL */
.crane-toggle { font-size:10px; padding:4px 10px; background:rgba(124,58,237,.15); border:1px solid rgba(124,58,237,.3); color:var(--accent3); border-radius:8px; cursor:pointer; font-family:inherit; font-weight:600; transition:background .2s; }
.crane-toggle:hover { background:rgba(124,58,237,.25); }
.crane-panel { display:none; border-top:1px solid var(--border); background:var(--bg); }
.crane-panel.open { display:block; }
.crane-head { padding:10px 18px; display:flex; align-items:center; justify-content:space-between; border-bottom:1px solid var(--border); }
.crane-head-title { font-size:11px; color:var(--text3); font-weight:600; text-transform:uppercase; letter-spacing:.8px; }
.crane-actions { display:flex; gap:6px; }
.crane-containers { padding:10px 18px; display:flex; flex-direction:column; gap:4px; max-height:200px; overflow-y:auto; scrollbar-width:thin; }
.crane-empty { font-size:11px; color:var(--text3); padding:8px 0; text-align:center; }
.ctr-row { display:flex; align-items:center; gap:8px; padding:7px 10px; border-radius:8px; background:var(--surface); border:1px solid var(--border); }
.ctr-row.ctr-active { border-color:rgba(16,185,129,.4); background:rgba(16,185,129,.06); }
.ctr-name { font-size:12px; font-weight:600; color:var(--text2); flex:1; font-family:monospace; }
.ctr-row.ctr-active .ctr-name { color:#34d399; }
.ctr-active-badge { font-size:9px; padding:2px 7px; background:rgba(16,185,129,.15); color:#34d399; border-radius:20px; border:1px solid rgba(16,185,129,.3); font-weight:700; text-transform:uppercase; }
.ctr-btns { display:flex; gap:4px; }
.ctr-btn { padding:3px 9px; font-size:10px; border-radius:6px; border:none; cursor:pointer; font-family:inherit; font-weight:600; transition:opacity .2s; }
.ctr-btn:hover { opacity:.8; }
.ctr-btn-switch { background:rgba(59,130,246,.15); color:#60a5fa; border:1px solid rgba(59,130,246,.3); }
.ctr-btn-wipe   { background:rgba(245,158,11,.15); color:#fbbf24; border:1px solid rgba(245,158,11,.3); }
.ctr-btn-del    { background:rgba(239,68,68,.15); color:#f87171; border:1px solid rgba(239,68,68,.3); }
.crane-log { padding:8px 18px 10px; border-top:1px solid var(--border); }
.crane-log-title { font-size:9px; color:var(--text3); text-transform:uppercase; letter-spacing:.8px; margin-bottom:4px; }
.crane-log-items { display:flex; flex-direction:column; gap:2px; max-height:60px; overflow-y:auto; }
.crane-log-item { font-size:10px; color:var(--text3); font-family:monospace; display:flex; gap:6px; }
.crane-log-item.done { color:#34d399; }
.crane-log-item.error { color:#f87171; }
.crane-log-item.pending { color:#fbbf24; }

/* MODAL */
.modal-overlay { display:none; position:fixed; inset:0; background:rgba(0,0,0,.7); backdrop-filter:blur(4px); z-index:999; align-items:center; justify-content:center; }
.modal-overlay.open { display:flex; }
.modal { background:var(--surface); border:1px solid var(--border2); border-radius:16px; padding:24px; width:320px; }
.modal-title { font-size:15px; font-weight:700; margin-bottom:16px; }
.modal input { width:100%; background:var(--bg); border:1px solid var(--border2); border-radius:8px; padding:10px 12px; color:var(--text); font-family:inherit; font-size:13px; margin-bottom:14px; outline:none; }
.modal input:focus { border-color:var(--accent2); }
.modal-btns { display:flex; gap:8px; }
.modal-btns .btn { flex:1; }
</style>
</head>
<body>
<header>
  <div class="logo"><div class="logo-icon">⬡</div>Farm Dashboard</div>
  <div class="header-stats">
    <div class="hstat"><div class="hstat-val" id="h-accounts">137</div><div class="hstat-label">Accounts</div></div>
    <div class="divider"></div>
    <div class="hstat"><div class="hstat-val" id="h-reels">—</div><div class="hstat-label">Reels Today</div></div>
    <div class="divider"></div>
    <div class="hstat"><div class="hstat-val" id="h-active">—</div><div class="hstat-label">Active Phones</div></div>
    <div class="divider"></div>
    <button class="btn" onclick="load()">↺ Refresh</button>
  </div>
</header>

<main><div class="grid" id="grid"></div></main>

<!-- Create container modal -->
<div class="modal-overlay" id="modal-create">
  <div class="modal">
    <div class="modal-title">Create Container</div>
    <input id="modal-name" placeholder="Container name (e.g. 11)" />
    <div class="modal-btns">
      <button class="btn btn-green" onclick="modalConfirmCreate()">Create</button>
      <button class="btn btn-ghost" onclick="closeModal()">Cancel</button>
    </div>
  </div>
</div>

<script>
const API = 'https://farm-production-8282.up.railway.app';
const PHONES = {
  1:{ip:'192.168.1.9', accounts:10},
  2:{ip:'192.168.1.12',accounts:30},
  3:{ip:'192.168.1.14',accounts:20},
  4:{ip:'192.168.1.10',accounts:20},
  5:{ip:'192.168.1.11',accounts:19},
  6:{ip:'192.168.1.15',accounts:20},
  7:{ip:'192.168.1.13',accounts:18}
};
const craneOpen   = {};    // phone -> bool (panel open)
const craneCtrs   = {};    // phone -> [{name, active}]
const craneLogs   = {};    // phone -> [{status,text,t}]
let   modalPhone  = null;

function fmt(n){if(!n||n==='?')return'—';n=parseInt(n);if(n>=1000)return(n/1000).toFixed(1)+'K';return n;}
function ts(){return new Date().toLocaleTimeString('en',{hour12:false,hour:'2-digit',minute:'2-digit',second:'2-digit'});}

// ── CRANE API ────────────────────────────────────────────────────────────────
async function craneCmd(phone, action, name='', num=''){
  const body = {phone:parseInt(phone), action, name, container_num:num};
  const r = await fetch(API+'/crane/command',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
  const d = await r.json();
  if(!craneLogs[phone]) craneLogs[phone]=[];
  craneLogs[phone].unshift({status:'pending',text:`${action}${num?' #'+num:''}${name?' "'+name+'"':''}`,t:ts(),id:d.id});
  if(craneLogs[phone].length>10) craneLogs[phone].pop();
  renderCraneLog(phone);
  pollCraneResult(phone, d.id);
  return d.id;
}

async function pollCraneResult(phone, cmdId){
  for(let i=0;i<60;i++){
    await new Promise(r=>setTimeout(r,2000));
    try{
      const r = await fetch(API+'/crane/queue');
      const d = await r.json();
      const cmd = (d.commands||[]).find(c=>c.id===cmdId);
      if(!cmd) continue;
      if(cmd.status==='done'||cmd.status==='error'){
        if(craneLogs[phone]){
          const entry = craneLogs[phone].find(l=>l.id===cmdId);
          if(entry){ entry.status=cmd.status; entry.text=(entry.text+' → '+(cmd.result||'').slice(0,40)).slice(0,60); }
        }
        renderCraneLog(phone);
        // If it was a list command, refresh containers
        if(cmd.status==='done') craneRefresh(phone);
        return;
      }
    }catch(e){}
  }
}

async function craneRefresh(phone){
  try{
    const r = await fetch(API+'/crane/containers/'+phone);
    const d = await r.json();
    craneCtrs[phone] = d.containers || [];
    renderCraneContainers(phone);
  }catch(e){}
}

function craneSendList(phone){
  craneCmd(phone,'list');
}

function craneSwitchTo(phone, num){
  if(!confirm('Switch phone '+phone+' to container '+num+'?')) return;
  craneCmd(phone,'switch','',num);
}

function craneWipe(phone, num){
  if(!confirm('Wipe container '+num+' on phone '+phone+'? This deletes account data!')) return;
  craneCmd(phone,'wipe','',num);
}

function craneDelete(phone, num){
  if(!confirm('DELETE container '+num+' on phone '+phone+'? Cannot be undone!')) return;
  craneCmd(phone,'delete','',num).then(()=>setTimeout(()=>craneSendList(phone),5000));
}

function openCreateModal(phone){
  modalPhone = phone;
  document.getElementById('modal-name').value='';
  document.getElementById('modal-create').classList.add('open');
  setTimeout(()=>document.getElementById('modal-name').focus(),100);
}

function closeModal(){
  document.getElementById('modal-create').classList.remove('open');
  modalPhone=null;
}

function modalConfirmCreate(){
  const name = document.getElementById('modal-name').value.trim();
  if(!name){alert('Enter a container name');return;}
  closeModal();
  craneCmd(modalPhone,'create',name,'').then(()=>setTimeout(()=>craneSendList(modalPhone),5000));
}

// ── CRANE RENDER ─────────────────────────────────────────────────────────────
function renderCraneContainers(phone){
  const el = document.getElementById('crane-ctrs-'+phone);
  if(!el) return;
  const ctrs = craneCtrs[phone]||[];
  if(!ctrs.length){ el.innerHTML='<div class="crane-empty">No containers — click Refresh</div>'; return; }
  el.innerHTML = ctrs.map(c=>`
    <div class="ctr-row${c.active?' ctr-active':''}">
      <span class="ctr-name">${c.name}</span>
      ${c.active?'<span class="ctr-active-badge">active</span>':''}
      <div class="ctr-btns">
        <button class="ctr-btn ctr-btn-switch" onclick="craneSwitchTo(${phone},'${c.name}')">Switch</button>
        <button class="ctr-btn ctr-btn-wipe"   onclick="craneWipe(${phone},'${c.name}')">Wipe</button>
        <button class="ctr-btn ctr-btn-del"    onclick="craneDelete(${phone},'${c.name}')">Del</button>
      </div>
    </div>`).join('');
}

function renderCraneLog(phone){
  const el = document.getElementById('crane-log-'+phone);
  if(!el) return;
  const logs = craneLogs[phone]||[];
  el.innerHTML = logs.map(l=>
    `<div class="crane-log-item ${l.status}">[${l.t}] ${l.text}</div>`
  ).join('') || '<div class="crane-log-item">No recent commands</div>';
}

function toggleCrane(phone){
  craneOpen[phone] = !craneOpen[phone];
  const panel = document.getElementById('crane-panel-'+phone);
  if(craneOpen[phone]){
    panel.classList.add('open');
    // Auto-load containers if empty
    if(!(craneCtrs[phone]||[]).length) craneRefresh(phone);
  } else {
    panel.classList.remove('open');
  }
}

// ── BUILD CARDS ONCE ──────────────────────────────────────────────────────────
function buildCards(){
  const grid = document.getElementById('grid');
  grid.innerHTML='';
  for(const[num,cfg]of Object.entries(PHONES)){
    const card=document.createElement('div');
    card.className='card';
    card.id='card-'+num;
    card.innerHTML=`
      <div class="card-head">
        <div class="phone-label">
          <div class="phone-num">${num}</div>
          <div class="phone-meta">
            <div class="phone-title">Phone ${num}</div>
            <div class="phone-ip">${cfg.ip}</div>
          </div>
        </div>
        <div class="head-right">
          <button class="crane-toggle" onclick="toggleCrane(${num})">⚙ Crane</button>
          <div class="pill pill-idle" id="pill-${num}">Idle</div>
        </div>
      </div>
      <div class="card-body">
        <div class="prog-row">
          <span class="prog-label" id="prog-label-${num}">Container 0 / ${cfg.accounts}</span>
          <span class="prog-val"  id="prog-val-${num}">0%</span>
        </div>
        <div class="prog-bar"><div class="prog-fill" id="prog-fill-${num}" style="width:0%"></div></div>
        <div class="stats-row">
          <div class="stat-box"><div class="stat-val">${cfg.accounts}</div><div class="stat-lbl">Accounts</div></div>
          <div class="stat-box"><div class="stat-val" id="stat-reels-${num}">—</div><div class="stat-lbl">Reels</div></div>
          <div class="stat-box"><div class="stat-val" id="stat-verif-${num}">—</div><div class="stat-lbl">Verified</div></div>
        </div>
        <div class="accs-head">
          <span class="accs-title">Accounts</span>
          <span class="accs-count">${cfg.accounts} total</span>
        </div>
        <div class="accs-list" id="accs-${num}">
          ${Array.from({length:cfg.accounts},(_,i)=>
            `<div class="acc-row" id="acc-${num}-${i+1}">
               <span class="acc-name">Account ${i+1}</span>
               <span class="acc-followers" id="flw-${num}-${i+1}">—</span>
             </div>`
          ).join('')}
        </div>
      </div>
      <div class="crane-panel" id="crane-panel-${num}">
        <div class="crane-head">
          <span class="crane-head-title">⚙ Crane — Phone ${num}</span>
          <div class="crane-actions">
            <button class="btn btn-sm btn-green" onclick="openCreateModal(${num})">+ Create</button>
            <button class="btn btn-sm btn-blue"  onclick="craneSendList(${num})">↺ Refresh</button>
          </div>
        </div>
        <div class="crane-containers" id="crane-ctrs-${num}">
          <div class="crane-empty">Click Refresh to load containers</div>
        </div>
        <div class="crane-log">
          <div class="crane-log-title">Recent commands</div>
          <div class="crane-log-items" id="crane-log-${num}">
            <div class="crane-log-item">No recent commands</div>
          </div>
        </div>
      </div>`;
    grid.appendChild(card);
  }
}

// ── UPDATE DATA IN-PLACE (no rebuild, no flash) ───────────────────────────────
async function load(){
  let totalReels=0, active=0;
  for(const[num,cfg]of Object.entries(PHONES)){
    let s=null;
    try{const r=await fetch(API+'/status/'+num,{signal:AbortSignal.timeout(3000)});s=await r.json();}catch(e){}
    const running=s?.status==='running', error=s?.status==='error';
    if(running) active++;
    if(s) totalReels+=s.reels_posted||0;
    const pct=s?Math.min(100,Math.round((s.container/s.total_containers)*100)):0;
    const accounts=s?.accounts||{};

    // Card class
    const card=document.getElementById('card-'+num);
    if(card) card.className='card'+(running?' running':error?' error':'');

    // Status pill
    const pill=document.getElementById('pill-'+num);
    if(pill){
      pill.className='pill '+(running?'pill-running':error?'pill-error':'pill-idle');
      pill.innerHTML=(running?'<span class="dot"></span>Running':error?'Error':'Idle');
    }

    // Progress
    const lbl=document.getElementById('prog-label-'+num);
    const val=document.getElementById('prog-val-'+num);
    const fill=document.getElementById('prog-fill-'+num);
    if(lbl) lbl.textContent=`Container ${s?s.container:0} / ${s?s.total_containers:cfg.accounts}`;
    if(val) val.textContent=pct+'%';
    if(fill) fill.style.width=pct+'%';

    // Stats
    const sr=document.getElementById('stat-reels-'+num);
    const sv=document.getElementById('stat-verif-'+num);
    if(sr) sr.textContent=s?.reels_posted??'—';
    if(sv) sv.textContent=s?.reels_verified??'—';

    // Account followers
    for(let i=1;i<=cfg.accounts;i++){
      const row=document.getElementById('acc-'+num+'-'+i);
      const flw=document.getElementById('flw-'+num+'-'+i);
      const acc=accounts[i]||{};
      const isActive=s?.current_container===i;
      if(row) row.className='acc-row'+(isActive?' active':'');
      if(flw) flw.textContent=acc.followers?fmt(acc.followers):'—';
    }
  }
  document.getElementById('h-reels').textContent=totalReels||'—';
  document.getElementById('h-active').textContent=active||'—';
}

// Auto-refresh silently every 15s — no flash, no countdown
setInterval(load, 15000);

document.getElementById('modal-create').addEventListener('click',e=>{if(e.target===e.currentTarget)closeModal();});
document.getElementById('modal-name').addEventListener('keydown',e=>{if(e.key==='Enter')modalConfirmCreate();});

// Refresh button just calls load(), no rebuild
document.querySelector('.btn').onclick = load;

buildCards();
load();
</script>
</body>
</html>"""

# ─── HTTP HANDLER ─────────────────────────────────────────────────────────────
class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args): pass

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def json_response(self, data, status=200):
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = urlparse(self.path).path

        # Dashboard HTML
        if path in ('/', '/dashboard'):
            body = DASHBOARD_HTML.encode()
            self.send_response(200)
            self.send_header('Content-Type', 'text/html')
            self.send_header('Cache-Control', 'no-cache')
            self.end_headers()
            self.wfile.write(body)
            return

        # Phone status
        if path.startswith('/status/'):
            phone = path.split('/')[-1]
            fpath = os.path.join(STATUS_DIR, f'phone_{phone}_status.json')
            if os.path.exists(fpath):
                with open(fpath) as f:
                    self.json_response(json.load(f))
            else:
                self.json_response(None)
            return

        # All statuses
        if path == '/all':
            result = {}
            os.makedirs(STATUS_DIR, exist_ok=True)
            for fpath in glob.glob(os.path.join(STATUS_DIR, 'phone_*_status.json')):
                num = os.path.basename(fpath).split('_')[1]
                try:
                    with open(fpath) as f: result[num] = json.load(f)
                except: pass
            self.json_response(result)
            return

        # ── CRANE ENDPOINTS ───────────────────────────────────────────────────
        # GET /crane/queue — return all recent commands
        if path == '/crane/queue':
            q = crane_load_queue()
            recent = sorted(q.values(), key=lambda x: x.get('created', 0), reverse=True)[:30]
            self.json_response({'commands': recent})
            return

        # GET /crane/pending — return only pending commands (polled by agent)
        if path == '/crane/pending':
            q = crane_load_queue()
            pending = [v for v in q.values() if v['status'] == 'pending']
            self.json_response({'commands': pending})
            return

        # GET /crane/containers/{phone} — return cached container list
        if path.startswith('/crane/containers/'):
            phone = path.split('/')[-1]
            self.json_response({'phone': phone, 'containers': crane_load_containers(phone)})
            return

        self.json_response({})

    def do_POST(self):
        path = urlparse(self.path).path
        length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(length)
        try:
            data = json.loads(body) if body else {}
        except:
            data = {}

        # Phone status update
        if path.startswith('/update/'):
            phone = path.split('/')[-1]
            os.makedirs(STATUS_DIR, exist_ok=True)
            fpath = os.path.join(STATUS_DIR, f'phone_{phone}_status.json')
            with open(fpath, 'wb') as f: f.write(body)
            self.json_response({'ok': True})
            return

        # ── CRANE ENDPOINTS ───────────────────────────────────────────────────
        # POST /crane/command — queue a command from dashboard
        if path == '/crane/command':
            cmd_id = str(uuid.uuid4())[:8]
            q = crane_load_queue()
            q[cmd_id] = {
                'id':            cmd_id,
                'phone':         data.get('phone', 1),
                'action':        data.get('action', 'list'),
                'name':          data.get('name', ''),
                'container_num': data.get('container_num', ''),
                'status':        'pending',
                'result':        '',
                'created':       time.time(),
            }
            # Clean old done commands (keep last 50)
            done = sorted(
                [v for v in q.values() if v['status'] != 'pending'],
                key=lambda x: x.get('created', 0), reverse=True
            )
            for old in done[50:]:
                del q[old['id']]
            crane_save_queue(q)
            self.json_response({'ok': True, 'id': cmd_id})
            return

        # POST /crane/result/{cmd_id} — agent reports completion
        if path.startswith('/crane/result/'):
            cmd_id = path.split('/')[-1]
            q = crane_load_queue()
            if cmd_id in q:
                q[cmd_id]['status']  = data.get('status', 'done')
                q[cmd_id]['result']  = data.get('result', '')
                q[cmd_id]['done_at'] = time.time()
                crane_save_queue(q)
            self.json_response({'ok': True})
            return

        # POST /crane/containers/{phone} — agent updates container list
        if path.startswith('/crane/containers/'):
            phone = path.split('/')[-1]
            crane_save_containers(phone, data.get('containers', []))
            self.json_response({'ok': True})
            return

        self.json_response({})


if __name__ == '__main__':
    os.makedirs(STATUS_DIR, exist_ok=True)
    os.makedirs(CRANE_DIR, exist_ok=True)
    port = int(os.environ.get('PORT', 5050))
    print(f'Dashboard + Crane API running on port {port}')
    HTTPServer(('0.0.0.0', port), Handler).serve_forever()
