// Discovery probe for QLab 5's WebSocket API. Zero dependencies — uses Node's
// built-in WebSocket (Node 22+).
//
// Usage:
//   QLAB_PASSCODE=... node scripts/discover.js
//   QLAB_URL=ws://other-mac.local:53000 node scripts/discover.js
//
// Writes timestamped .json (full log) + .md (summary) to discovery-output/.
// Goal: figure out which addresses give us the data we need to build a
// timeline viewer (cue list, durations, continue modes, live progress).

import { writeFileSync, mkdirSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const QLAB_URL = process.env.QLAB_URL ?? 'ws://127.0.0.1:53000';
const QLAB_PASSCODE = process.env.QLAB_PASSCODE ?? '';
const WATCH_UPDATES_MS = Number(process.env.WATCH_UPDATES_MS ?? 10000);
const PER_CUE_LIMIT = Number(process.env.PER_CUE_LIMIT ?? 20);

const __dirname = dirname(fileURLToPath(import.meta.url));
const outDir = join(__dirname, '..', 'discovery-output');
mkdirSync(outDir, { recursive: true });
const stamp = new Date().toISOString().replace(/[:.]/g, '-');
const jsonPath = join(outDir, `${stamp}.json`);
const mdPath = join(outDir, `${stamp}.md`);

const ws = new WebSocket(QLAB_URL);
const log = [];
const replies = new Map();
const pending = new Map();
const updatesObserved = [];

function send(address, args = [], workspace_id) {
  const msg = { address, args };
  if (workspace_id) msg.workspace_id = workspace_id;
  log.push({ ts: Date.now(), dir: 'OUT', ...msg });
  ws.send(JSON.stringify(msg));
}

function ask(address, args = [], workspace_id, timeoutMs = 2000) {
  return new Promise((resolve) => {
    const timer = setTimeout(() => {
      pending.delete(address);
      resolve({ timeout: true });
    }, timeoutMs);
    pending.set(address, (reply) => { clearTimeout(timer); resolve(reply); });
    send(address, args, workspace_id);
  });
}

ws.addEventListener('message', (e) => {
  let msg;
  try { msg = JSON.parse(e.data); } catch { return; }
  log.push({ ts: Date.now(), dir: 'IN', ...msg });
  if (msg.address?.startsWith('/update/')) { updatesObserved.push(msg); return; }
  if (msg.address) {
    replies.set(msg.address, msg);
    const r = pending.get(msg.address);
    if (r) { pending.delete(msg.address); r(msg); }
  }
});

ws.addEventListener('error', (e) => console.error('WS error:', e.message ?? e));

const GLOBAL_ADDRESSES = ['/version', '/workspaces', '/alwaysReply', '/showMode'];
const WORKSPACE_INFO_ADDRESSES = [
  '/workspaceName', '/uniqueID', '/showMode', '/auditionWindow',
  '/cueLists', '/selectedCues',
];
const PLAYBACK_ADDRESSES = [
  '/cue/playhead/uniqueID', '/cue/playhead/displayName', '/cue/playhead/number',
  '/cue/active/uniqueID', '/runningCues', '/runningOrPausedCues',
];
const PER_CUE_PROPERTIES = [
  'uniqueID', 'number', 'name', 'listName', 'displayName', 'type', 'notes',
  'colorName', 'mode', 'flagged', 'armed', 'continueMode',
  'preWait', 'postWait', 'duration', 'currentDuration',
  'actionElapsed', 'percentActionElapsed',
  'percentPreWaitElapsed', 'percentPostWaitElapsed',
  'isRunning', 'isPaused', 'isBroken', 'isLoaded',
  'children', 'parent',
];

async function main() {
  await new Promise((res, rej) => {
    ws.addEventListener('open', () => res(), { once: true });
    ws.addEventListener('error', (e) => rej(new Error(e.message ?? 'ws error')), { once: true });
  });
  console.log(`connected to ${QLAB_URL}`);

  await ask('/alwaysReply', [1]);
  const connectReply = await ask('/connect', QLAB_PASSCODE ? [QLAB_PASSCODE] : []);
  console.log('/connect →', connectReply.data ?? connectReply);

  for (const addr of GLOBAL_ADDRESSES) await ask(addr);

  const wsList = replies.get('/workspaces')?.data;
  const workspaceId = Array.isArray(wsList) && wsList[0]?.uniqueID;
  console.log(`workspaces: ${Array.isArray(wsList) ? wsList.length : '?'}, using ${workspaceId ?? '(none)'}`);

  if (workspaceId) {
    for (const addr of WORKSPACE_INFO_ADDRESSES) await ask(addr, [], workspaceId);
    for (const addr of PLAYBACK_ADDRESSES) await ask(addr, [], workspaceId);
  }

  const cueLists = replies.get('/cueLists')?.data ?? [];
  const cueIds = [];
  function collectIds(node, depth = 0) {
    if (!node) return;
    if (node.uniqueID) cueIds.push({ id: node.uniqueID, depth, type: node.type, number: node.number, name: node.listName ?? node.name });
    if (Array.isArray(node.cues)) node.cues.forEach((c) => collectIds(c, depth + 1));
  }
  cueLists.forEach((cl) => collectIds(cl));
  console.log(`found ${cueIds.length} cues across ${cueLists.length} cue list(s)`);

  const sample = cueIds.slice(0, PER_CUE_LIMIT);
  for (const cue of sample) {
    for (const prop of PER_CUE_PROPERTIES) {
      await ask(`/cue_id/${cue.id}/${prop}`, [], workspaceId);
    }
  }

  console.log(`\nEnabling /updates and watching for ${WATCH_UPDATES_MS}ms — interact with QLab now…`);
  await ask('/updates', [1], workspaceId);
  await new Promise((r) => setTimeout(r, WATCH_UPDATES_MS));
  await ask('/updates', [0], workspaceId);

  const out = {
    meta: { stamp, qlabUrl: QLAB_URL, workspaceId, cueCount: cueIds.length, cueListCount: cueLists.length, sampledCues: sample.length, updatesObserved: updatesObserved.length },
    cueLists, sampledCues: sample,
    replies: Object.fromEntries(replies),
    updatesObserved, log,
  };
  writeFileSync(jsonPath, JSON.stringify(out, null, 2));

  const lines = [];
  lines.push(`# QLab discovery — ${stamp}`, '');
  lines.push(`- QLab URL: \`${QLAB_URL}\``);
  lines.push(`- Workspace: \`${workspaceId ?? '(none)'}\``);
  lines.push(`- Cue lists: ${cueLists.length}`);
  lines.push(`- Cues found: ${cueIds.length} (sampled ${sample.length})`);
  lines.push(`- Update pushes observed: ${updatesObserved.length}`, '');
  lines.push('## Probe results by status', '');
  const byStatus = { ok: [], denied: [], error: [], other: [], timeout: [] };
  for (const [addr, reply] of replies) {
    const status = reply.timeout ? 'timeout' : (reply.status ?? 'other');
    (byStatus[status] ?? byStatus.other).push({ addr, dataPreview: previewData(reply.data) });
  }
  for (const [status, items] of Object.entries(byStatus)) {
    if (!items.length) continue;
    lines.push(`### ${status} (${items.length})`, '');
    for (const it of items.slice(0, 200)) lines.push(`- \`${it.addr}\` → ${it.dataPreview}`);
    lines.push('');
  }
  if (updatesObserved.length) {
    lines.push('## Update pushes observed', '');
    const m = new Map();
    for (const u of updatesObserved) m.set(u.address, (m.get(u.address) ?? 0) + 1);
    for (const [addr, n] of [...m.entries()].sort((a, b) => b[1] - a[1])) lines.push(`- \`${addr}\` × ${n}`);
    lines.push('');
  }
  writeFileSync(mdPath, lines.join('\n'));

  console.log(`\n✔ wrote ${jsonPath}`);
  console.log(`✔ wrote ${mdPath}`);
  ws.close();
  process.exit(0);
}

function previewData(data) {
  if (data === undefined) return '_(no data)_';
  if (data === null) return 'null';
  if (typeof data === 'string') return JSON.stringify(data.length > 80 ? data.slice(0, 80) + '…' : data);
  if (Array.isArray(data)) return `[array, len=${data.length}]`;
  if (typeof data === 'object') return `{object, keys=${Object.keys(data).slice(0, 6).join(',')}${Object.keys(data).length > 6 ? '…' : ''}}`;
  return JSON.stringify(data);
}

main().catch((e) => { console.error(e); process.exit(1); });
