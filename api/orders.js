/* Comidas Rápidas Juan — API de pedidos (Vercel serverless + Upstash Redis por REST).
 *   POST   /api/orders            el cliente envía un pedido desde la página
 *   GET    /api/orders?since=N    el panel lista los pedidos (necesita clave)
 *   PATCH  /api/orders            el panel cambia estado / cobrado / nota
 *   DELETE /api/orders?id=...     el panel borra un pedido cancelado
 * Variables de entorno: ADMIN_PIN y la base de datos: REDIS_URL (Redis Cloud, conexión directa)
 * o, como alternativa, Upstash por REST (KV_REST_API_URL + KV_REST_API_TOKEN).
 * Todas las llaves usan el prefijo "crj:" para no mezclarse con otros proyectos.
 */
'use strict';
const crypto = require('crypto');
const MENU = require('./_menu.json');

const REDIS_URL = process.env.REDIS_URL || '';
const DB_URL = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL || '';
const DB_TOKEN = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN || '';
const HAS_DB = !!REDIS_URL || !!(DB_URL && DB_TOKEN);
const P = 'crj:';

const STATUSES = ['nuevo', 'preparando', 'listo', 'entregado', 'cancelado'];
const KEEP = 800;          // pedidos que lista el panel
const MAX_LINES = 40;      // líneas distintas por pedido
const MAX_QTY = 50;        // unidades por línea

/* Conexión directa a Redis Cloud con el protocolo de Redis (RESP), sin librerías externas.
 * Se reutiliza la conexión entre llamadas de la misma función. */
const net = require('net');
const tls = require('tls');
let conn = null;
function connectRedis() {
  if (conn) return conn;
  conn = new Promise(function (resolve, reject) {
    const u = new URL(REDIS_URL);
    const secure = u.protocol === 'rediss:';
    const opts = { host: u.hostname, port: Number(u.port || 6379) };
    const sock = secure ? tls.connect(Object.assign({ servername: u.hostname }, opts)) : net.connect(opts);
    const state = { sock: sock, buf: Buffer.alloc(0), queue: [] };
    sock.setNoDelay(true);
    sock.setTimeout(8000);
    function fail(e) {
      conn = null;
      state.queue.splice(0).forEach(function (q) { q.reject(e); });
      try { sock.destroy(); } catch (x) {}
      reject(e);
    }
    sock.on('timeout', function () { fail(new Error('redis timeout')); });
    sock.on('error', fail);
    sock.on('close', function () { conn = null; state.queue.splice(0).forEach(function (q) { q.reject(new Error('redis closed')); }); });
    sock.on('data', function (d) {
      state.buf = state.buf.length ? Buffer.concat([state.buf, d]) : d;
      for (;;) {
        const r = parseReply(state.buf, 0);
        if (!r) break;
        state.buf = state.buf.slice(r.end);
        const q = state.queue.shift();
        if (q) { if (r.value instanceof Error) q.reject(r.value); else q.resolve(r.value); }
      }
    });
    state.send = function (args) {
      return new Promise(function (res2, rej2) {
        state.queue.push({ resolve: res2, reject: rej2 });
        let out = '*' + args.length + '\r\n';
        args.forEach(function (x) { const s2 = String(x); out += '$' + Buffer.byteLength(s2) + '\r\n' + s2 + '\r\n'; });
        sock.write(out);
      });
    };
    sock.once(secure ? 'secureConnect' : 'connect', function () {
      const user = decodeURIComponent(u.username || ''), pass = decodeURIComponent(u.password || '');
      const auth = pass ? state.send(user && user !== 'default' ? ['AUTH', user, pass] : ['AUTH', pass]) : Promise.resolve('OK');
      auth.then(function () { resolve(state); }, fail);
    });
  });
  return conn;
}
function parseReply(buf, i) {
  const nl = buf.indexOf('\r\n', i);
  if (nl < 0) return null;
  const type = String.fromCharCode(buf[i]);
  const line = buf.toString('utf8', i + 1, nl);
  const next = nl + 2;
  if (type === '+') return { value: line, end: next };
  if (type === '-') return { value: new Error(line), end: next };
  if (type === ':') return { value: Number(line), end: next };
  if (type === '$') {
    const n = Number(line);
    if (n < 0) return { value: null, end: next };
    if (buf.length < next + n + 2) return null;
    return { value: buf.toString('utf8', next, next + n), end: next + n + 2 };
  }
  if (type === '*') {
    const n = Number(line);
    if (n < 0) return { value: null, end: next };
    const arr = []; let p = next;
    for (let k = 0; k < n; k++) { const r = parseReply(buf, p); if (!r) return null; arr.push(r.value); p = r.end; }
    return { value: arr, end: p };
  }
  return { value: new Error('redis protocol'), end: next };
}

async function db(cmds) {
  if (REDIS_URL) {
    const c = await connectRedis();
    return Promise.all(cmds.map(function (cmd) { return c.send(cmd); }));   // se envían en tubería, en orden
  }
  const r = await fetch(DB_URL + '/pipeline', {
    method: 'POST',
    headers: { Authorization: 'Bearer ' + DB_TOKEN, 'Content-Type': 'application/json' },
    body: JSON.stringify(cmds)
  });
  if (!r.ok) throw new Error('db ' + r.status);
  const out = await r.json();
  out.forEach(function (x) { if (x && x.error) throw new Error('db ' + x.error); });
  return out.map(function (x) { return x.result; });
}

function send(res, code, obj) {
  res.statusCode = code;
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.setHeader('Cache-Control', 'no-store');
  res.setHeader('X-Robots-Tag', 'noindex');
  res.end(JSON.stringify(obj));
}

function clientIp(req) {
  const h = req.headers;
  return String(h['x-real-ip'] || (h['x-forwarded-for'] || '').split(',')[0] || 'unknown').trim().slice(0, 64);
}

function clean(v, max) {
  // Quita caracteres de control y recorta. El panel además escapa todo al mostrarlo.
  return String(v == null ? '' : v).replace(/[\u0000-\u001f\u007f<>]/g, ' ').replace(/\s+/g, ' ').trim().slice(0, max);
}

function normPin(s) { return String(s || '').toUpperCase().replace(/[\s-]/g, ''); }
function safeEqual(a, b) {
  const x = crypto.createHash('sha256').update(a).digest();
  const y = crypto.createHash('sha256').update(b).digest();
  return crypto.timingSafeEqual(x, y);
}

/* Revisa la clave del panel, con bloqueo tras varios intentos fallidos. */
async function checkAdmin(req, res) {
  const pin = normPin(process.env.ADMIN_PIN);
  if (pin.length < 6) { send(res, 503, { error: 'pin_not_set' }); return false; }
  const ip = clientIp(req);
  const failKey = P + 'fail:' + ip;
  const [fails] = await db([['GET', failKey]]);
  if (Number(fails || 0) >= 8) { send(res, 429, { error: 'locked' }); return false; }
  const given = normPin(req.headers['x-admin-pin']);
  if (!given || !safeEqual(given, pin)) {
    await db([['INCR', failKey], ['EXPIRE', failKey, 900]]);
    send(res, 401, { error: 'bad_pin' });
    return false;
  }
  if (Number(fails || 0) > 0) await db([['DEL', failKey]]);
  return true;
}

function sameOrigin(req) {
  const o = req.headers.origin;
  if (!o) return true;
  try { return new URL(o).host === req.headers.host; } catch (e) { return false; }
}

async function readBody(req) {
  if (req.body && typeof req.body === 'object') return req.body;
  if (typeof req.body === 'string') { try { return JSON.parse(req.body); } catch (e) { return null; } }
  return null;
}

async function createOrder(req, res) {
  if (!sameOrigin(req)) return send(res, 403, { error: 'origin' });
  const b = await readBody(req);
  if (!b || typeof b !== 'object') return send(res, 400, { error: 'body' });
  if (b.hp) return send(res, 200, { ok: true, number: 0 });   // trampa para robots: finge éxito

  // Límite por persona: 6 pedidos por 10 minutos
  const ip = clientIp(req);
  const rlKey = P + 'rl:' + ip + ':' + Math.floor(Date.now() / 600000);
  const [cnt] = await db([['INCR', rlKey], ['EXPIRE', rlKey, 700]]);
  if (Number(cnt) > 6) return send(res, 429, { error: 'rate' });

  // Productos: solo platos del menú, cantidades enteras, opción válida
  const src = Array.isArray(b.items) ? b.items.slice(0, MAX_LINES) : [];
  const items = [];
  src.forEach(function (it) {
    if (!it || typeof it !== 'object') return;
    const m = MENU[String(it.id)];
    const q = Math.min(Math.floor(Number(it.qty)), MAX_QTY);
    if (!m || !(q > 0)) return;
    let op = clean(it.op, 40);
    if (m.o) { if (m.o.indexOf(op) < 0) op = m.o[0]; } else op = '';
    items.push({ id: String(it.id), name: m.n, qty: q, price: m.p, op: op, note: clean(it.nota, 120), promo: !!m.promo });
  });
  if (!items.length) return send(res, 400, { error: 'items' });

  const name = clean(b.name, 60), phone = clean(b.phone, 30);
  const tienda = b.servicio === 'tienda';
  const addr = tienda ? '' : clean(b.addr, 160), barrio = tienda ? '' : clean(b.barrio, 80);
  const pago = b.pago === 'contra' ? 'contra' : 'online';
  const pd = phone.replace(/\D/g, '').replace(/^57(?=3\d{9}$)/, '');
  if (!name || !/^3\d{9}$/.test(pd)) return send(res, 400, { error: 'contact' });
  if (!tienda && !addr) return send(res, 400, { error: 'addr' });

  // Los precios los calcula el servidor con el menú, nunca el celular del cliente
  const subtotal = items.reduce(function (s, i) { return s + i.qty * i.price; }, 0);
  const noPromo = items.filter(function (i) { return !i.promo; }).reduce(function (s, i) { return s + i.qty * i.price; }, 0);
  let cupon = '', pct = 0;
  const code = String(b.cupon || '').toUpperCase();
  const cp = Math.floor(Number(b.pct));
  if (/^[A-Z0-9]{3,16}$/.test(code) && cp >= 1 && cp <= 90) { cupon = code; pct = cp; }
  const discount = Math.round(noPromo * pct / 100);
  const total = subtotal - discount;

  // Evita pedidos repetidos por doble clic (mismo contenido en 2 minutos)
  const sig = crypto.createHash('sha1').update(JSON.stringify([pd, items, tienda, addr, total])).digest('hex').slice(0, 20);
  const dupKey = P + 'dup:' + sig;
  const [prev] = await db([['GET', dupKey]]);
  if (prev) return send(res, 200, { ok: true, number: Number(prev), dup: true });

  const [seq] = await db([['INCR', P + 'seq']]);
  const number = 1000 + Number(seq);
  const id = String(number);
  const ref = /^CRV-\d{5}$/.test(String(b.ref || '')) ? String(b.ref) : '';
  const origen = ['Instagram', 'TikTok', 'Facebook', 'WhatsApp'].indexOf(b.origen) >= 0 ? b.origen : '';
  const order = {
    id: id, number: number, ref: ref, ts: Date.now(), status: 'nuevo', paid: false,
    name: name, phone: pd, servicio: tienda ? 'tienda' : 'domicilio', addr: addr, barrio: barrio,
    pago: pago, items: items, units: items.reduce(function (s, i) { return s + i.qty; }, 0),
    subtotal: subtotal, cupon: cupon, pct: pct, discount: discount, total: total,
    origen: origen, adminNote: ''
  };
  await db([
    ['SET', P + 'order:' + id, JSON.stringify(order)],
    ['ZADD', P + 'orders', order.ts, id],
    ['SET', dupKey, number, 'EX', 120],
    ['INCR', P + 'ver']
  ]);
  return send(res, 200, { ok: true, number: number, total: total });
}

async function listOrders(req, res) {
  if (!(await checkAdmin(req, res))) return;
  const since = String((req.query && req.query.since) || '');
  const [ver] = await db([['GET', P + 'ver']]);
  const v = String(ver || '0');
  if (since && since === v) return send(res, 200, { ver: v, same: true });
  const [ids] = await db([['ZREVRANGE', P + 'orders', 0, KEEP - 1]]);
  let orders = [];
  if (ids && ids.length) {
    const rows = await db([['MGET'].concat(ids.map(function (i) { return P + 'order:' + i; }))]);
    orders = (rows[0] || []).filter(Boolean).map(function (s) { try { return JSON.parse(s); } catch (e) { return null; } }).filter(Boolean);
  }
  return send(res, 200, { ver: v, orders: orders });
}

async function patchOrder(req, res) {
  if (!sameOrigin(req)) return send(res, 403, { error: 'origin' });
  if (!(await checkAdmin(req, res))) return;
  const b = await readBody(req);
  const id = clean(b && b.id, 12);
  if (!/^\d{4,9}$/.test(id)) return send(res, 400, { error: 'id' });
  const [raw] = await db([['GET', P + 'order:' + id]]);
  if (!raw) return send(res, 404, { error: 'missing' });
  const o = JSON.parse(raw);
  if (b.status !== undefined) {
    if (STATUSES.indexOf(b.status) < 0) return send(res, 400, { error: 'status' });
    o.status = b.status;
  }
  if (b.paid !== undefined) o.paid = !!b.paid;
  if (b.adminNote !== undefined) o.adminNote = clean(b.adminNote, 300);
  o.updated = Date.now();
  await db([['SET', P + 'order:' + id, JSON.stringify(o)], ['INCR', P + 'ver']]);
  return send(res, 200, { ok: true, order: o });
}

async function deleteOrder(req, res) {
  if (!sameOrigin(req)) return send(res, 403, { error: 'origin' });
  if (!(await checkAdmin(req, res))) return;
  const id = clean(req.query && req.query.id, 12);
  if (!/^\d{4,9}$/.test(id)) return send(res, 400, { error: 'id' });
  await db([['DEL', P + 'order:' + id], ['ZREM', P + 'orders', id], ['INCR', P + 'ver']]);
  return send(res, 200, { ok: true });
}

module.exports = async function handler(req, res) {
  try {
    if (!HAS_DB) return send(res, 503, { error: 'db_not_configured' });
    if (req.method === 'POST') return await createOrder(req, res);
    if (req.method === 'GET') return await listOrders(req, res);
    if (req.method === 'PATCH') return await patchOrder(req, res);
    if (req.method === 'DELETE') return await deleteOrder(req, res);
    res.setHeader('Allow', 'GET, POST, PATCH, DELETE');
    return send(res, 405, { error: 'method' });
  } catch (e) {
    console.error('orders api error:', e && e.message);
    return send(res, 500, { error: 'server' });
  }
};
