import fs from 'node:fs/promises';

const DATA_FILE = './users.json';
const TEMP_FILE = './users.json.tmp';
const LIMIT = 3;
const PERIOD = 24 * 60 * 60 * 1000;

let dataQueue = Promise.resolve();

function withDataLock(fn) {
  const task = dataQueue.then(fn, fn);
  dataQueue = task.catch(() => {});
  return task;
}

async function readData() {
  try {
    return JSON.parse(await fs.readFile(DATA_FILE, 'utf8'));
  } catch (error) {
    if (error.code === 'ENOENT') return {};
    throw error;
  }
}

async function writeData(data) {
  await fs.writeFile(TEMP_FILE, JSON.stringify(data, null, 2));
  await fs.rename(TEMP_FILE, DATA_FILE);
}

export function getUser(id) {
  return withDataLock(async () => {
    const data = await readData();
    return data[id] || { premium: false, uses: [] };
  });
}

export function checkAndUse(id, aliases = []) {
  return withDataLock(async () => {
    const data = await readData();
    const ids = [...new Set([id, ...aliases].filter(Boolean))];

    const premiumId = ids.find(key => data[key]?.premium === true);

    if (premiumId) {
      return { allowed: true, premium: true, remaining: Infinity };
    }

    const key = id;
    const user = data[key] || { premium: false, uses: [] };
    const now = Date.now();

    user.uses = user.uses || [];

    if (user.uses.length >= LIMIT) {
      data[key] = user;
      await writeData(data);

      return {
        allowed: false,
        premium: false,
        remaining: 0,
        resetAt: user.uses[0] + PERIOD
      };
    }

    user.uses.push(now);
    data[key] = user;
    await writeData(data);

    return {
      allowed: true,
      premium: false,
      remaining: LIMIT - user.uses.length
    };
  });
}

export function activatePremium(id) {
  return withDataLock(async () => {
    const data = await readData();
    const user = data[id] || { premium: false, uses: [] };

    user.premium = true;
    data[id] = user;

    await writeData(data);
  });
}

export function deactivatePremium(id) {
  return withDataLock(async () => {
    const data = await readData();

    if (!data[id]) return false;

    data[id].premium = false;
    await writeData(data);

    return true;
  });
}

export const FREE_LIMIT = LIMIT;

export function isJoined(id, aliases = []) {
  return withDataLock(async () => {
    const data = await readData();
    const ids = [...new Set([id, ...aliases].filter(Boolean))];

    return ids.some(key => data[key]?.joined === true);
  });
}

export function markJoined(id, aliases = []) {
  return withDataLock(async () => {
    const data = await readData();
    const ids = [...new Set([id, ...aliases].filter(Boolean))];

    for (const key of ids) {
      const user = data[key] || { premium: false, uses: [] };
      user.joined = true;
      data[key] = user;
    }

    await writeData(data);
  });
}
