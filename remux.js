import { spawn } from 'node:child_process';
import fs from 'node:fs/promises';
import path from 'node:path';

const ffmpegBin = () => process.env.FFMPEG_PATH || 'ffmpeg';

function run(args, timeoutMs = 120000) {
  return new Promise(resolve => {
    let settled = false;
    let timedOut = false;
    const finish = result => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve(result);
    };

    let p;
    try {
      p = spawn(ffmpegBin(), args, {
        stdio: ['ignore', 'ignore', 'pipe'],
        windowsHide: true
      });
    } catch (e) {
      finish({ code: -1, err: String(e) });
      return;
    }

    let err = '';
    const timer = setTimeout(() => {
      timedOut = true;
      p.kill('SIGKILL');
    }, timeoutMs);

    p.stderr.on('data', d => {
      err += d;
      if (err.length > 12000) err = err.slice(-12000);
    });

    p.on('error', e => finish({ code: -1, err: String(e) }));
    p.on('close', code => finish({
      code: timedOut ? -2 : code,
      err: timedOut ? 'FFmpeg melewati batas waktu 120 detik. ' + err : err
    }));
  });
}

function boxes(buf, start, end) {
  const result = [];

  for (let p = start; p + 8 <= end;) {
    let size = buf.readUInt32BE(p);
    const type = buf.toString('ascii', p + 4, p + 8);
    let header = 8;

    if (size === 1) {
      size = Number(buf.readBigUInt64BE(p + 8));
      header = 16;
    } else if (size === 0) {
      size = end - p;
    }

    if (size < header || p + size > end) break;

    result.push({ type, start: p, end: p + size, header });
    p += size;
  }

  return result;
}

async function readExact(handle, length, position) {
  const buffer = Buffer.alloc(length);
  let done = 0;

  while (done < length) {
    const result = await handle.read(buffer, done, length - done, position + done);
    if (!result.bytesRead) throw new Error('Metadata MP4 terpotong.');
    done += result.bytesRead;
  }
  return buffer;
}

async function readMoov(file) {
  const handle = await fs.open(file, 'r');
  try {
    const stat = await handle.stat();
    let position = 0;

    while (position + 8 <= stat.size) {
      const header = await readExact(handle, 8, position);
      let size = header.readUInt32BE(0);
      const type = header.toString('ascii', 4, 8);
      let headerSize = 8;

      if (size === 1) {
        const extended = await readExact(handle, 8, position + 8);
        const bigSize = extended.readBigUInt64BE(0);
        if (bigSize > BigInt(Number.MAX_SAFE_INTEGER)) {
          throw new Error('Ukuran atom MP4 terlalu besar.');
        }
        size = Number(bigSize);
        headerSize = 16;
      } else if (size === 0) {
        size = stat.size - position;
      }

      if (size < headerSize || position + size > stat.size) {
        throw new Error('Struktur atom MP4 tidak valid.');
      }

      if (type === 'moov') {
        if (size > 64 * 1024 * 1024) {
          throw new Error('Metadata MOOV melebihi batas 64 MB.');
        }
        return {
          data: await readExact(handle, size, position),
          offset: position
        };
      }

      position += size;
    }

    throw new Error('Atom MOOV tidak ditemukan.');
  } finally {
    await handle.close();
  }
}

function getMovieTimescale(buf) {
  const moov = boxes(buf, 0, buf.length).find(b => b.type === 'moov');
  if (!moov) throw new Error('MOOV tidak ditemukan.');

  const mvhd = boxes(buf, moov.start + moov.header, moov.end)
    .find(b => b.type === 'mvhd');
  if (!mvhd) throw new Error('MVHD tidak ditemukan.');

  const version = buf[mvhd.start + 8];
  const offset = version === 1 ? mvhd.start + 28 : mvhd.start + 20;
  const timescale = buf.readUInt32BE(offset);

  if (!timescale) throw new Error('Movie timescale tidak valid.');
  return timescale;
}

function getTrackEdits(buf) {
  const moov = boxes(buf, 0, buf.length)
    .find(b => b.type === 'moov');

  if (!moov) throw new Error('MOOV MP4 tidak ditemukan.');

  return boxes(buf, moov.start + moov.header, moov.end)
    .filter(b => b.type === 'trak')
    .map(trak => {
      const tb = boxes(buf, trak.start + trak.header, trak.end);
      const mdia = tb.find(b => b.type === 'mdia');
      const edts = tb.find(b => b.type === 'edts');

      if (!mdia) throw new Error('MDIA tidak ditemukan.');

      const hdlr = boxes(buf, mdia.start + mdia.header, mdia.end)
        .find(b => b.type === 'hdlr');

      if (!hdlr) throw new Error('HDLR tidak ditemukan.');

      const handler = buf.toString(
        'ascii', hdlr.start + 16, hdlr.start + 20
      );

      if (!edts) return { handler, entries: null };

      const elst = boxes(buf, edts.start + edts.header, edts.end)
        .find(b => b.type === 'elst');

      if (!elst) return { handler, entries: null };

      const version = buf[elst.start + 8];
      const count = buf.readUInt32BE(elst.start + 12);

      if (version !== 0 && version !== 1) {
        throw new Error(`Versi edit list ${handler} tidak didukung.`);
      }

      const entries = [];
      let p = elst.start + 16;

      for (let i = 0; i < count; i++) {
        let durationOffset;
      let mediaOffset;
      let duration;
      let mediaTime;

      if (version === 0) {
        if (p + 12 > elst.end) throw new Error("Edit list terpotong.");
        durationOffset = p;
        duration = BigInt(buf.readUInt32BE(p));
        mediaOffset = p + 4;
        mediaTime = BigInt(buf.readInt32BE(mediaOffset));
        p += 12;
      } else {
        if (p + 20 > elst.end) throw new Error("Edit list terpotong.");
        durationOffset = p;
        duration = buf.readBigUInt64BE(p);
        mediaOffset = p + 8;
        mediaTime = buf.readBigInt64BE(mediaOffset);
        p += 20;
      }

      entries.push({ durationOffset, duration, mediaOffset, mediaTime });
      }

      return { handler, version, entries };
    });
}

function restoreEditLists(source, target) {
  const sourceTracks = getTrackEdits(source);
  const targetTracks = getTrackEdits(target);

  const sourceScale = BigInt(getMovieTimescale(source));
  const targetScale = BigInt(getMovieTimescale(target));

  for (const t of targetTracks) {
    const s = sourceTracks.find(x => x.handler === t.handler);
    if (!s) throw new Error('Track asli ' + t.handler + ' tidak ditemukan.');
    if (!s.entries) continue;
    if (!t.entries) throw new Error('Edit list hasil ' + t.handler + ' tidak ditemukan.');

    const sourceEntry = s.entries.find(x => x.mediaTime !== -1n);
    if (!sourceEntry) continue;

    const targetEntry = t.entries.find(x => x.mediaTime !== -1n);
    if (!targetEntry) throw new Error('Entry media ' + t.handler + ' tidak ditemukan.');

    const value = sourceEntry.mediaTime;

    if (t.version === 0) {
      if (value < -2147483648n || value > 2147483647n) {
        throw new Error('Nilai media_time terlalu besar.');
      }
      target.writeInt32BE(Number(value), targetEntry.mediaOffset);
    } else {
      target.writeBigInt64BE(value, targetEntry.mediaOffset);
    }

    console.log('[EDIT LIST] ' + t.handler + ': media_time=' + value);

    // Durasi audio dikonversi ke movie timescale milik file hasil.
    if (t.handler === 'soun') {
      const numerator = sourceEntry.duration * targetScale;
      const duration = (numerator + sourceScale / 2n) / sourceScale;

      if (duration < 0n) throw new Error('Durasi audio tidak valid.');

      if (t.version === 0) {
        if (duration > 0xffffffffn) throw new Error('Durasi audio melebihi batas versi 0.');
        target.writeUInt32BE(Number(duration), targetEntry.durationOffset);
      } else {
        if (duration > 0xffffffffffffffffn) throw new Error('Durasi audio melebihi batas versi 1.');
        target.writeBigUInt64BE(duration, targetEntry.durationOffset);
      }

      console.log(
        '[EDIT LIST] audio duration: ' +
        sourceEntry.duration + ' -> ' + duration + ' ticks'
      );
    }
  }
}

export async function probe(file) {
  const { code, err } = await run(['-hide_banner', '-i', file]);

  const streams = [...err.matchAll(
    /Stream #\d+:\d+[^\n]*?(Video|Audio):\s*([a-zA-Z0-9_]+)/gi
  )];

  const video = streams.find(
    m => m[1].toLowerCase() === 'video'
  );

  return {
    hasVideo: !!video,
    codec: video ? video[2].toLowerCase() : null,
    hasAudio: streams.some(
      m => m[1].toLowerCase() === 'audio'
    ),
    probeError: !video && code !== 0
  };
}

export async function remux(input, output) {
  if (path.resolve(input) === path.resolve(output)) {
    throw new Error('File input dan output tidak boleh sama.');
  }

  const info = await probe(input);

  if (!info.hasVideo) {
    throw new Error('Tidak ditemukan track video yang valid.');
  }

  const temp = `${output}.repack-tmp.mp4`;
  await fs.rm(temp, { force: true });

  const args = [
    '-hide_banner',
    '-y',
    '-ignore_editlist', '1',
    '-i', input,
    '-map', '0:v:0'
  ];

  if (info.hasAudio) args.push('-map', '0:a:0');
  if (info.codec === 'hevc') args.push('-tag:v', 'hvc1');

  args.push(
    '-c', 'copy',
    '-map_metadata', '-1',
    '-map_chapters', '-1',
    '-metadata', 'encoder=',
    '-brand', 'isom',
    '-use_editlist', '1',
    '-movflags', '+faststart',
    temp
  );

  try {
    const result = await run(args);

    if (result.code !== 0) {
      throw new Error(
        'Repack FFmpeg gagal: ' +
        result.err.split('\n').filter(Boolean).slice(-5).join(' ')
      );
    }

    const sourceMoov = await readMoov(input);
    const targetMoov = await readMoov(temp);

    restoreEditLists(sourceMoov.data, targetMoov.data);

    const handle = await fs.open(temp, 'r+');
    try {
      let written = 0;
      while (written < targetMoov.data.length) {
        const result = await handle.write(
          targetMoov.data,
          written,
          targetMoov.data.length - written,
          targetMoov.offset + written
        );
        if (!result.bytesWritten) throw new Error('Gagal menulis metadata MOOV.');
        written += result.bytesWritten;
      }
    } finally {
      await handle.close();
    }
    await fs.rename(temp, output);

    return info;
  } catch (error) {
    await fs.rm(temp, { force: true });
    throw error;
  }
}
