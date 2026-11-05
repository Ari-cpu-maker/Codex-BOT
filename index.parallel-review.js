import makeWASocket, {
  useMultiFileAuthState, DisconnectReason, fetchLatestBaileysVersion, fetchLatestWaWebVersion, downloadMediaMessage
} from '@whiskeysockets/baileys';
import pino from 'pino';
import qrcode from 'qrcode-terminal';
import fs from 'node:fs/promises';
import { createWriteStream } from 'node:fs';
import { pipeline } from 'node:stream/promises';
import { Transform } from 'node:stream';
import path from 'node:path';
import os from 'node:os';
import { remux } from './remux.js';
import { checkAndUse, activatePremium, isJoined, markJoined } from './premium.js';

 // Membaca konfigurasi lokal dari .env tanpa dependensi tambahan.
try {
  const envText = await fs.readFile(new URL('./.env', import.meta.url), 'utf8');
  for (const line of envText.split(/\r?\n/)) {
    const match = line.match(/^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)\s*$/);
    if (match && process.env[match[1]] === undefined) {
      process.env[match[1]] = match[2].replace(/^(['"])(.*)\1$/, '$2');
    }
  }
} catch (error) {
  if (error.code !== 'ENOENT') throw error;
}

const ADMIN_NUMBERS = new Set(
  (process.env.ADMIN_NUMBERS || '')
    .split(',')
    .map(value => value.replace(/\D/g, ''))
    .filter(Boolean)
);

// ffmpeg bawaan paket (gratis). Bisa diganti dengan FFMPEG_PATH=/path/ke/ffmpeg
if (!process.env.FFMPEG_PATH) {
  try { const m = await import('ffmpeg-static'); if (m.default) process.env.FFMPEG_PATH = m.default; } catch { /* pakai ffmpeg di PATH */ }
}

const MAX_MB_RAW = Number(process.env.MAX_MB || 150);
const MAX_MB = Number.isFinite(MAX_MB_RAW) && MAX_MB_RAW > 0 && MAX_MB_RAW <= 1024
  ? Math.floor(MAX_MB_RAW) : 150;
const MAX_FILE_BYTES = MAX_MB * 1024 * 1024;
const VIDEO_EXT = new Set(['mov', 'mp4', 'm4v', '3gp', 'mkv', 'webm']);
const logger = pino({ level: 'silent' });

const TXT = {
  welcome: '👋 Hai! Selamat datang di Bot HD.\n\nUntuk membuat video HD, ketik MEDIA ya 😊',
  useDocument: '🎬 Silakan kirim video dalam bentuk Dokumen (Document), bukan sebagai video biasa di WhatsApp.\n\n📌 Cara ini membantu menjaga kualitas video agar tidak terlalu terkompresi.\n\n✨ Bot akan memproses dan mengirimkan kembali video HD.',
  notVideo: 'File ini bukan video. Kirim file video (MOV/MP4) sebagai Document.',
  tooBig: n => `File terlalu besar (maksimal ${n} MB).`,
  busy: 'Video sebelumnya masih diproses. Tunggu sebentar ya.',
  working: 'Memproses jadi HD…',
  sending: 'Menghantar…',
  progress: (filled, message) =>
    '🟩'.repeat(filled) + '⬜'.repeat(10 - filled) +
    ' ' + (filled * 10) + '%\n' + message,
  failed: e => 'Maaf, video ini belum bisa diproses: ' + e
};

let reconnectTimer = null;
let activeSock = null;
let reconnectAttempts = 0;

function scheduleReconnect() {
  if (reconnectTimer) return;

  const delay = Math.min(3000 * (2 ** reconnectAttempts), 60000);
  reconnectAttempts = Math.min(reconnectAttempts + 1, 5);

  console.log(`Mencoba menyambung kembali dalam ${Math.ceil(delay / 1000)} detik.`);

  reconnectTimer = setTimeout(async () => {
    reconnectTimer = null;
    try {
      await start();
    } catch (e) {
      console.error('Gagal memulai bot:', e);
      scheduleReconnect();
    }
  }, delay);
}

// Antrean pemrosesan paralel dan pengiriman FIFO
let socketReady = false;
const processQueue = [];
const sendQueue = [];

let processingWorkers = 0;
let sendingRunning = false;
const activeJobs = new Set();

const PROCESS_CONCURRENCY = 4;

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

// Perlindungan STOP dan penghentian darurat.
const OPT_OUT_FILE = path.resolve('./optouts.json');
const EMERGENCY_STOP_FILE = path.resolve('./BOT_PAUSE');
const optedOut = new Set();

try {
  const saved = JSON.parse(await fs.readFile(OPT_OUT_FILE, 'utf8'));
  if (Array.isArray(saved)) {
    for (const id of saved) if (typeof id === 'string') optedOut.add(id);
  }
} catch {}

async function saveOptOuts() {
  const temp = OPT_OUT_FILE + '.tmp';
  await fs.writeFile(temp, JSON.stringify([...optedOut]), { mode: 0o600 });
  await fs.rename(temp, OPT_OUT_FILE);
}

async function emergencyStopped() {
  try { await fs.access(EMERGENCY_STOP_FILE); return true; }
  catch { return false; }
}

async function cleanupJob(job) {
  if (!job.dir) return;
  const dir = job.dir;
  job.dir = null;
  await fs.rm(dir, { recursive: true, force: true }).catch(() => {});
}

async function cancelAllJobs() {
  processQueue.length = 0;
  sendQueue.length = 0;

  for (const job of [...activeJobs]) {
    job.cancelled = true;

    // Jangan membangunkan pengiriman sebelum proses file selesai.
    if (!job.processing && !job.sending) {
      job.resolveReady();
      await cleanupJob(job);
      activeJobs.delete(job);
    }
  }

  console.log('Semua antrean dibatalkan karena koneksi terputus.');
}

const liveSock = new Proxy({}, {
  get(_target, property) {
    const current = activeSock;
    if (!current) return undefined;
    const value = Reflect.get(current, property, current);
    return typeof value === 'function' ? value.bind(current) : value;
  }
});

async function enqueue(jid, msg, doc, statusKey = null, sourceSock = activeSock) {
  if (await emergencyStopped() || optedOut.has(jid)) return false;
  if (!socketReady || !activeSock || sourceSock !== activeSock) return false;
  let resolveReady;
  let readyResolved = false;

  const job = {
    jid, msg, doc, statusKey,
    result: null,
    failureText: null,
    dir: null,
    cancelled: false,
    processing: false,
    sending: false,
    ready: new Promise(resolve => { resolveReady = resolve; }),
    resolveReady: () => {
      if (!readyResolved) {
        readyResolved = true;
        resolveReady();
      }
    },
    edit: async () => {}
  };

  activeJobs.add(job);
  processQueue.push(job);
  sendQueue.push(job);

  void runProcessingBatches();
  void runSendingQueue();
  return true;
}


function runProcessingBatches() {
  if (!socketReady || !activeSock) return;

  while (
    processingWorkers < PROCESS_CONCURRENCY &&
    processQueue.length > 0
  ) {
    processingWorkers++;

    void (async () => {
      try {
        while (socketReady && activeSock && processQueue.length > 0) {
          const job = processQueue.shift();
          if (!job) continue;

          job.processing = true;

          try {
            if (!job.cancelled) {
              job.result = await prepareDocument(liveSock, job);
            }
          } catch (e) {
            if (!job.cancelled) {
              console.error('Gagal memproses media:', e);
              job.failureText = TXT.failed(
                String(e.message || e).slice(0, 200)
              );
            }
          } finally {
            job.processing = false;
            job.resolveReady();

            if (job.cancelled) {
              await cleanupJob(job);
              activeJobs.delete(job);
            }
          }
        }
      } finally {
        processingWorkers--;

        if (processQueue.length > 0 && socketReady && activeSock) {
          runProcessingBatches();
        }
      }
    })();
  }
}

async function runSendingQueue() {
  if (sendingRunning || !socketReady || !activeSock) return;
  sendingRunning = true;

  try {
    while (sendQueue.length > 0 && socketReady && activeSock) {
      if (await emergencyStopped()) {
        await cancelAllJobs();
        break;
      }
      const job = sendQueue.shift();
      await job.ready;

      if (job.cancelled || !socketReady || !activeSock) {
        job.cancelled = true;
        await cleanupJob(job);
        activeJobs.delete(job);
        continue;
      }

      job.sending = true;
      try {
        await sendPreparedDocument(liveSock, job);
      } catch (e) {
        if (!job.cancelled) {
          console.error('Gagal mengirim media:', e);
          await liveSock.sendMessage(job.jid, {
            text: TXT.failed(String(e.message || e).slice(0, 200))
          }).catch(err => console.error('Gagal mengirim pesan kesalahan:', err.message));
        }
      } finally {
        job.sending = false;
        await cleanupJob(job);
        activeJobs.delete(job);
      }
    }
  } finally {
    sendingRunning = false;
    if (sendQueue.length > 0 && socketReady && activeSock) {
      void runSendingQueue();
    }
  }
}

const lastWelcome = new Map();
const lastMediaRequest = new Map();
const lastMediaHelp = new Map();
const recentFileHashes = new Map();
const MAX_ACTIVE_JOBS = 12;
const REQUEST_COOLDOWN_MS = 5000;
const DUPLICATE_WINDOW_MS = 120000;

async function prepareDocument(sock, job) {
  const { msg, doc, jid } = job;
  if (job.cancelled) return null;
  const name = doc.fileName || 'media';
  const ext = (name.split('.').pop() || '').toLowerCase();
  const mime = (doc.mimetype || '').toLowerCase();

  const isVideo = mime.startsWith('video/') || VIDEO_EXT.has(ext);
  const imageExt = new Set(['jpg', 'jpeg', 'png', 'webp', 'heic', 'heif']);
  const isImage = mime.startsWith('image/') || imageExt.has(ext);

  if (!isVideo && !isImage) {
    job.failureText = '❌ File tidak didukung. Kirim video atau foto sebagai Document.';
    return null;
  }

  const declaredSize = Number(doc.fileLength || 0);
  if (Number.isFinite(declaredSize) && declaredSize > MAX_FILE_BYTES) {
    job.failureText = '❌ ' + TXT.tooBig(MAX_MB);
    return null;
  }

  const candidates = [
    msg.key.senderPn,
    msg.key.participantAlt,
    msg.key.remoteJidAlt,
    msg.key.participant,
    msg.key.remoteJid,
    jid
  ].filter(Boolean);

  const normalizeId = id =>
    id.includes('@') ? id : id.replace(/\D/g, '') + '@s.whatsapp.net';

  const usageIds = [...new Set(candidates.map(normalizeId))];

  // Identitas pengguna tidak dicetak ke log untuk menjaga privasi.

  const usage = await checkAndUse(usageIds[0], usageIds.slice(1));
  if (job.cancelled) return null;

  if (!usage.allowed) {
    job.failureText =
      '⚠️ Kuota gratis kamu sudah habis (3/3 percobaan).\n\n' +
      'Untuk menggunakan bot tanpa batas, silakan bergabung dengan komunitas premium dan hubungi admin untuk aktivasi unlimited.';
    return null;
  }

  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'wahd-'));
  job.dir = dir;

  const safeExt = isVideo
    ? (VIDEO_EXT.has(ext) ? ext : 'mov')
    : (imageExt.has(ext) ? ext : 'jpg');

  const inPath = path.join(dir, 'in.' + safeExt);
  const outPath = path.join(dir, 'out.mp4');

  let typingTimer;

  try {
    // Tidak mengirim indikator aktivitas berkala.

    const stream = await downloadMediaMessage(msg, 'stream', {}, {
      logger,
      reuploadRequest: sock.updateMediaMessage
    });

    let downloadedBytes = 0;
    const sizeGuard = new Transform({
      transform(chunk, encoding, callback) {
        downloadedBytes += chunk.length;
        if (downloadedBytes > MAX_FILE_BYTES) {
          callback(new Error(`Ukuran unduhan melewati batas ${MAX_MB} MB.`));
          return;
        }
        callback(null, chunk);
      }
    });
    await pipeline(stream, sizeGuard, createWriteStream(inPath, { flags: 'wx', mode: 0o600 }));
    if (job.cancelled) return null;

    await job.edit(TXT.progress(3, 'Sedang memproses media menjadi HD...'));

    if (isVideo) {
      await remux(inPath, outPath);
      if (job.cancelled) return null;

      return {
        type: 'video',
        path: outPath
      };
    }

    let imagePath = inPath;
    let imageMime = ({
      jpg: 'image/jpeg',
      jpeg: 'image/jpeg',
      png: 'image/png',
      webp: 'image/webp'
    })[safeExt] || 'image/jpeg';

    if (safeExt === 'heic' || safeExt === 'heif') {
      imagePath = path.join(dir, 'converted.jpg');

      const { execFile } = await import('node:child_process');

      await new Promise((resolve, reject) => {
        execFile('heif-convert', ['-q', '90', inPath, imagePath],
          (error, stdout, stderr) => {
            if (error) {
              return reject(new Error(
                (stderr || error.message || 'Konversi HEIC gagal').slice(0, 300)
              ));
            }
            resolve();
          }
        );
      });

      if (job.cancelled) return null;
      imageMime = 'image/jpeg';
    }

    return {
      type: 'image',
      path: imagePath,
      mime: imageMime
    };
  } finally {
    if (typingTimer) clearInterval(typingTimer);
  }
}

async function sendPreparedDocument(sock, job) {
  if (job.cancelled || !socketReady || !activeSock) return;
  if (await emergencyStopped() || optedOut.has(job.jid)) return;

  if (job.failureText) {
    await sock.sendMessage(job.jid, { text: job.failureText });
    return;
  }

  if (!job.result) {
    await sock.sendMessage(job.jid, {
      text: '❌ Media belum berhasil diproses. Silakan coba lagi.'
    });
    return;
  }

  await job.edit(TXT.progress(
    9,
    'Giliran Anda, sedang menyiapkan pengiriman media...'
  ));

  if (job.cancelled || !socketReady || !activeSock) return;
  if (await emergencyStopped() || optedOut.has(job.jid)) return;

  if (job.result.type === 'video') {
    await sock.sendMessage(job.jid, {
      video: { url: job.result.path },
      mimetype: 'video/mp4'
    });

    await sleep(500);

    await job.edit(TXT.progress(
      10,
      'Selesai! Video HD berhasil diproses dan dikirim. Silakan periksa media di atas.'
    ));
  } else {
    await sock.sendMessage(job.jid, {
      image: { url: job.result.path },
      mimetype: job.result.mime
    });

    await sleep(500);

    await job.edit(TXT.progress(
      10,
      'Selesai! Foto berhasil diproses dan dikirim. Silakan periksa media di atas.'
    ));
  }
}

async function start() {
  const { state, saveCreds } = await useMultiFileAuthState(process.env.AUTH_DIR || './auth');
  const { version } = await fetchLatestBaileysVersion();
  const sock = makeWASocket({ version, auth: state, logger, syncFullHistory: false });
  activeSock = sock;

  const pairNumber = (process.env.PAIR_NUMBER || '').replace(/[^0-9]/g, '');
  let pairingRequested = false;

  if (pairNumber && !state.creds.registered) {
    setTimeout(async () => {
      if (sock !== activeSock || pairingRequested) return;
      pairingRequested = true;
      try {
        const code = await sock.requestPairingCode(pairNumber);
        console.log('KODE TAUTAN WHATSAPP:', code);
        console.log('Masukkan kode ini di WhatsApp > Perangkat tertaut > Tautkan perangkat > Tautkan dengan nomor telepon.');
      } catch (e) {
        pairingRequested = false;
        console.error('Gagal meminta kode tautan:', e.message || e);
      }
    }, 3000);
  }

  sock.ev.on('connection.update', async ({ connection, lastDisconnect, qr }) => {
    if (sock !== activeSock) return;
    if (qr && !pairNumber) {
      console.log('Scan QR ini: WhatsApp > Perangkat tertaut > Tautkan perangkat');
      qrcode.generate(qr, { small: true });
    }
    if (connection === 'open') {
      socketReady = true;
      reconnectAttempts = 0;
      console.log('Bot siap.');
      void runProcessingBatches();
      void runSendingQueue();
    }
    if (connection === 'close') {
      socketReady = false;
      void cancelAllJobs();
      console.error('DETAIL KONEKSI TERPUTUS:', {
        statusCode: lastDisconnect?.error?.output?.statusCode,
        message: lastDisconnect?.error?.message,
        data: lastDisconnect?.error?.data,
        reason: lastDisconnect?.error?.output?.payload
      });
      const code = lastDisconnect?.error?.output?.statusCode;
      if (code === DisconnectReason.loggedOut) {
        console.dir(lastDisconnect?.error, { depth: 6 });
        process.exit(1);
      }
      if (sock !== activeSock) return;
      scheduleReconnect(3000);
    }
  });

  sock.ev.on('creds.update', saveCreds);

  sock.ev.on('messages.upsert', async ({ messages, type }) => {
    if (type !== 'notify') return;
    if (sock !== activeSock || !socketReady || await emergencyStopped()) return;
    for (const msg of messages) {
      try {
        if (sock !== activeSock || !socketReady) break;
        if (!msg.message || msg.key.fromMe) continue;
        const jid = msg.key.remoteJid;
        if (!jid || jid === 'status@broadcast' || jid.endsWith('@g.us')) continue; // hanya chat pribadi
        const m = msg.message;

        const textMsg =
          m.conversation ||
          m.extendedTextMessage?.text ||
          m.imageMessage?.caption ||
          m.videoMessage?.caption ||
          '';

        const senderNumber = (msg.key.senderPn || msg.key.participantAlt || msg.key.remoteJidAlt || msg.key.participant || jid).split('@')[0].replace(/\D/g, '');
        const identityCandidates = [
          msg.key.senderPn,
          msg.key.participantAlt,
          msg.key.remoteJidAlt,
          msg.key.participant,
          msg.key.remoteJid,
          jid
        ].filter(Boolean);

        const normalizeIdentity = id =>
          id.includes('@') ? id : id.replace(/\D/g, '') + '@s.whatsapp.net';

        const userIds = [...new Set(identityCandidates.map(normalizeIdentity))];
        const userKey = userIds[0] || jid;
        const command = textMsg.trim().toUpperCase();

        if (command === 'STOP') {
          optedOut.add(jid);
          await saveOptOuts();
          await sock.sendMessage(jid, { text: 'Permintaan berhenti diterima. Bot tidak akan memproses permintaan lagi di chat ini. Ketik START jika ingin mengaktifkan kembali.' });
          continue;
        }
        if (command === 'START') {
          optedOut.delete(jid);
          await saveOptOuts();
          await sock.sendMessage(jid, { text: 'Bot diaktifkan kembali untuk chat ini.' });
          continue;
        }
        if (optedOut.has(jid)) continue;

        const isAdmin = identityCandidates.some(id => {
          const number = id.split('@')[0].replace(/\D/g, '');
          return (!id.includes('@') || id.endsWith('@s.whatsapp.net')) &&
            ADMIN_NUMBERS.has(number);
        });

        if (isAdmin) {
          const cmd = textMsg.trim().match(/^AKTIFKAN\s+(\+?\d{8,15}|@[a-zA-Z0-9._-]{1,30}|\d{5,25}@lid)$/i);
          if (cmd) {
            let target;
            const requested = cmd[1];

            if (requested.startsWith('@')) {
              let usernames = {};
              try {
                usernames = JSON.parse(await fs.readFile('./usernames.json', 'utf8'));
              } catch {}

              target = usernames[requested.toLowerCase()];
              if (!target) {
                await sock.sendMessage(jid, { text: 'Username tidak ditemukan di usernames.json.' });
                continue;
              }
            } else if (requested.endsWith('@lid')) {
              target = requested;
            } else {
              target = requested.replace(/\D/g, '') + '@s.whatsapp.net';
            }

            await activatePremium(target);
            await sock.readMessages([msg.key]);
            continue;
          }
        }

        if (textMsg.trim().toUpperCase() === 'CEK LID') {
          const lid = [msg.key.remoteJid, msg.key.participant, msg.key.remoteJidAlt, msg.key.participantAlt]
            .find(id => typeof id === 'string' && id.endsWith('@lid'));
          if (lid) {
            await sock.sendMessage(jid, { text: 'Kode LID akun WhatsApp kamu:\n' + lid + '\n\nKirim kode ini kepada admin jika ingin aktivasi premium.' });
          } else {
            await sock.sendMessage(jid, { text: 'LID tidak tersedia pada pesan ini. Coba kirim pesan pribadi lagi atau minta admin memeriksa log bot.' });
          }
          continue;
        }

        if (textMsg.trim().toUpperCase() === 'SUDAH GABUNG') {
          await markJoined(userKey, userIds.slice(1));
          await sock.sendMessage(jid, { text: 'Terima kasih! Status bergabung dicatat. Sekarang kirim video atau gambar sebagai Dokumen.' });
          continue;
        }

        const joined = await isJoined(userKey, userIds.slice(1));
        if (sock !== activeSock || !socketReady) continue;
        if (!joined) {
          if (command === 'MEDIA') {
            await sock.sendMessage(jid, {
              text: 'Untuk menggunakan layanan, silakan bergabung dengan komunitas bot lalu ketik SUDAH GABUNG. Jika tidak ingin menerima pesan bot, ketik STOP.'
            });
          }
          continue;
        }

        const doc = m.documentMessage || m.documentWithCaptionMessage?.message?.documentMessage;
        if (doc) {
          const now = Date.now();

          for (const [key, time] of recentFileHashes) {
            if (now - time > DUPLICATE_WINDOW_MS) recentFileHashes.delete(key);
          }

          if ([...activeJobs].some(job => job.jid === jid)) {
            await sock.sendMessage(jid, { text: '⏳ Media kamu masih diproses. Tunggu sampai selesai ya.' });
            continue;
          }

          if (activeJobs.size >= MAX_ACTIVE_JOBS) {
            await sock.sendMessage(jid, { text: '📥 Antrean sedang penuh. Coba lagi beberapa saat ya.' });
            continue;
          }

          if (now - (lastMediaRequest.get(jid) || 0) < REQUEST_COOLDOWN_MS) {
            await sock.sendMessage(jid, { text: '⏱️ Tunggu 5 detik sebelum mengirim media lagi ya.' });
            continue;
          }

          let hashKey = null;
          if (doc.fileSha256) {
            try {
              hashKey = jid + ':' + Buffer.from(doc.fileSha256).toString('base64');
            } catch {}
          }

          if (hashKey && recentFileHashes.has(hashKey)) {
            await sock.sendMessage(jid, { text: '🔁 File yang sama baru saja dikirim. Coba lagi setelah 2 menit ya.' });
            continue;
          }
          const accepted = await enqueue(jid, msg, doc, null, sock);
          if (accepted && sock === activeSock && socketReady) {
            lastMediaRequest.set(jid, Date.now());
            if (hashKey) recentFileHashes.set(hashKey, Date.now());
          }
          continue;
        }

        if (m.videoMessage || m.imageMessage) {
          await sock.sendMessage(jid, { text: TXT.useDocument });
          continue;
        }

        if (textMsg.trim()) {
          const command = textMsg.trim().toUpperCase();
          const now = Date.now();

          if (command === 'MEDIA') {
            if (now - (lastMediaHelp.get(jid) || 0) >= REQUEST_COOLDOWN_MS) {
              lastMediaHelp.set(jid, now);
              await sock.sendMessage(jid, { text: TXT.useDocument });
            }
            continue;
          }

          continue;
        }

        // Tidak mengirim sambutan otomatis untuk pesan yang tidak meminta layanan.
      } catch (e) { console.error(e); }
    }
  });
}

start().catch(e => {
  console.error('Gagal memulai bot:', e);
  scheduleReconnect(5000);
});
