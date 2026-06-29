import express from 'express';
import http from 'http';
import path from 'path';
import fs from 'fs';
import { WebSocketServer, WebSocket } from 'ws';
import { Player, ChessGame, LeaderboardEntry, WsMessage } from './types.js';
import pg from 'pg';
import nodemailer from 'nodemailer';
const { Pool } = pg;


// In-memory data persistence
const players: Record<string, Player & { ws: WebSocket }> = {};
const games: Record<string, ChessGame> = {};
const pendingChallenges: Record<string, string> = {}; // key: targetId (challenged player), value: challengerId

// Logger system
interface LogEntry {
  timestamp: string;
  level: 'info' | 'warn' | 'error';
  message: string;
}

const appLogs: LogEntry[] = [];

function addLog(message: string, level: 'info' | 'warn' | 'error' = 'info') {
  const timestamp = new Date().toISOString();
  console.log(`[${timestamp}] [${level.toUpperCase()}] ${message}`);
  appLogs.push({ timestamp, level, message });
  if (appLogs.length > 500) {
    appLogs.shift();
  }
}

// User Database representation
interface DbUser {
  id: string;
  username: string;
  email: string;
  password?: string;
  rating: number;
  wins: number;
  losses: number;
  createdAt?: string;
  isVerified?: boolean;
  verificationCode?: string;
}

// Load/save leaderboard from a local file so ratings are persistent
const LEADERBOARD_FILE = path.join(process.cwd(), 'leaderboard_data.json');
let leaderboard: LeaderboardEntry[] = [];

try {
  if (fs.existsSync(LEADERBOARD_FILE)) {
    leaderboard = JSON.parse(fs.readFileSync(LEADERBOARD_FILE, 'utf-8'));
  } else {
    // Generate some highly realistic pre-existing master wooden chess AI rankings for the leaderboard
    leaderboard = [
      { name: 'Garry Kasparov', rating: 2812, wins: 154, losses: 12 },
      { name: 'Magnus Carlsen', rating: 2882, wins: 180, losses: 8 },
      { name: 'Bobby Fischer', rating: 2785, wins: 112, losses: 14 },
      { name: 'Deep Blue AI', rating: 2720, wins: 95, losses: 40 },
      { name: 'WoodMaster_99', rating: 1650, wins: 22, losses: 18 },
      { name: 'Rook_Whisperer', rating: 1580, wins: 15, losses: 14 }
    ];
    fs.writeFileSync(LEADERBOARD_FILE, JSON.stringify(leaderboard, null, 2));
  }
} catch (e) {
  console.error('Error handling leaderboard persistence:', e);
}

function saveLeaderboard() {
  try {
    fs.writeFileSync(LEADERBOARD_FILE, JSON.stringify(leaderboard, null, 2));
  } catch (e) {
    console.error('Failed to save leaderboard:', e);
  }
}

// Persistent Admin Database
let adminDb = {
  admin: {
    username: 'admin',
    password: 'chessadmin2026'
  },
  databaseUrl: '',
  smtp: {
    host: '',
    port: '',
    user: '',
    pass: '',
    from: ''
  },
  musicTracks: [] as { id: string; name: string; url: string; isLocal?: boolean }[],
  users: [] as DbUser[]
};

const ADMIN_DB_FILE = path.join(process.cwd(), 'admin_db.json');

try {
  if (fs.existsSync(ADMIN_DB_FILE)) {
    const loaded = JSON.parse(fs.readFileSync(ADMIN_DB_FILE, 'utf-8'));
    adminDb = {
      ...adminDb,
      ...loaded,
      admin: { ...adminDb.admin, ...loaded.admin },
      smtp: { ...adminDb.smtp, ...loaded.smtp }
    };
    if (!adminDb.users) {
      adminDb.users = [];
    }
  } else {
    fs.writeFileSync(ADMIN_DB_FILE, JSON.stringify(adminDb, null, 2));
  }
} catch (e) {
  console.error('Error loading admin database:', e);
}

function saveAdminDb() {
  try {
    fs.writeFileSync(ADMIN_DB_FILE, JSON.stringify(adminDb, null, 2));
  } catch (e) {
    console.error('Failed to save admin database:', e);
  }
}

// PostgreSQL Connection Pool management (lazily initialized)
let pgPool: any = null;

function getPgPool() {
  const connectionString = process.env.DATABASE_URL || adminDb.databaseUrl;
  if (!connectionString) {
    return null;
  }

  if (!pgPool) {
    try {
      pgPool = new Pool({
        connectionString,
        ssl: connectionString.includes('sslmode=') ? undefined : { rejectUnauthorized: false },
        connectionTimeoutMillis: 5000,
      });
      console.log('PostgreSQL Connection Pool initialized.');
    } catch (err) {
      console.error('Failed to create PostgreSQL Pool:', err);
    }
  }
  return pgPool;
}

// Reset pgPool when connection string changes
function resetPgPool() {
  if (pgPool) {
    try {
      pgPool.end();
    } catch (e) {
      console.error('Error closing old PG Pool:', e);
    }
    pgPool = null;
  }
}

// Verify or create database table in PostgreSQL
async function initPgDb() {
  const pool = getPgPool();
  if (!pool) {
    addLog('Nessun DATABASE_URL configurato ancora. In esecuzione in modalità database JSON locale.', 'warn');
    return;
  }

  try {
    const client = await pool.connect();
    try {
      await client.query(`
        CREATE TABLE IF NOT EXISTS music_tracks (
          id VARCHAR(50) PRIMARY KEY,
          name VARCHAR(255) NOT NULL,
          url TEXT NOT NULL,
          is_local BOOLEAN DEFAULT FALSE
        );
      `);
      addLog('Tabella music_tracks verificata/creata in PostgreSQL.');

      await client.query(`
        CREATE TABLE IF NOT EXISTS users (
          id VARCHAR(50) PRIMARY KEY,
          username VARCHAR(100) UNIQUE NOT NULL,
          email VARCHAR(255) UNIQUE NOT NULL,
          password VARCHAR(255) NOT NULL,
          rating INTEGER DEFAULT 1500,
          wins INTEGER DEFAULT 0,
          losses INTEGER DEFAULT 0,
          is_verified BOOLEAN DEFAULT FALSE,
          verification_code VARCHAR(10),
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
      `);
      
      // Safe dynamic column creation for pre-existing tables
      try {
        await client.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS is_verified BOOLEAN DEFAULT FALSE;`);
        await client.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS verification_code VARCHAR(10);`);
      } catch (colErr: any) {
        addLog('Nota: Colonne di verifica già presenti o errore innocuo: ' + colErr.message, 'info');
      }

      addLog('Tabella users verificata/creata in PostgreSQL.');
    } finally {
      client.release();
    }
  } catch (err: any) {
    addLog('Impossibile inizializzare lo schema delle tabelle PostgreSQL: ' + err.message, 'error');
  }
}

// User helper methods mapping to PostgreSQL/JSON fallback
async function getUsers(): Promise<DbUser[]> {
  const pool = getPgPool();
  if (pool) {
    try {
      const res = await pool.query('SELECT id, username, email, password, rating, wins, losses, is_verified as "isVerified", verification_code as "verificationCode", created_at as "createdAt" FROM users ORDER BY username ASC');
      return res.rows;
    } catch (err: any) {
      addLog('Errore durante la lettura degli utenti da PostgreSQL, provo fallback locale: ' + err.message, 'error');
    }
  }
  if (!adminDb.users) adminDb.users = [];
  return adminDb.users;
}

async function getUserByUsername(username: string): Promise<DbUser | null> {
  const pool = getPgPool();
  if (pool) {
    try {
      const res = await pool.query('SELECT id, username, email, password, rating, wins, losses, is_verified as "isVerified", verification_code as "verificationCode", created_at as "createdAt" FROM users WHERE LOWER(username) = LOWER($1)', [username.trim()]);
      if (res.rows.length > 0) return res.rows[0];
      return null;
    } catch (err: any) {
      addLog('Errore durante la ricerca dell\'utente in PostgreSQL: ' + err.message, 'error');
    }
  }
  if (!adminDb.users) adminDb.users = [];
  const matched = adminDb.users.find(u => u.username.toLowerCase() === username.trim().toLowerCase());
  return matched || null;
}

async function addUser(user: DbUser): Promise<void> {
  const pool = getPgPool();
  if (pool) {
    try {
      await pool.query(
        'INSERT INTO users (id, username, email, password, rating, wins, losses, is_verified, verification_code) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)',
        [user.id, user.username, user.email, user.password || '', user.rating || 1500, user.wins || 0, user.losses || 0, user.isVerified || false, user.verificationCode || '']
      );
      addLog(`Utente inserito in PostgreSQL con successo: ${user.username}`);
      return;
    } catch (err: any) {
      addLog('Errore durante l\'aggiunta dell\'utente in PostgreSQL: ' + err.message, 'error');
      throw err;
    }
  }

  if (!adminDb.users) adminDb.users = [];
  const exists = adminDb.users.some(u => u.username.toLowerCase() === user.username.toLowerCase() || u.email.toLowerCase() === user.email.toLowerCase());
  if (exists) {
    throw new Error('Username o Email già in uso.');
  }

  adminDb.users.push({
    ...user,
    createdAt: user.createdAt || new Date().toISOString()
  });
  saveAdminDb();
  addLog(`Utente inserito nel database JSON locale con successo: ${user.username}`);
}

async function updateUserVerification(userId: string, isVerified: boolean, verificationCode: string): Promise<boolean> {
  const pool = getPgPool();
  if (pool) {
    try {
      const res = await pool.query(
        'UPDATE users SET is_verified = $1, verification_code = $2 WHERE id = $3',
        [isVerified, verificationCode, userId]
      );
      addLog(`Stato di verifica aggiornato in PostgreSQL per utente ID ${userId}: verified=${isVerified}`);
      return (res.rowCount ?? 0) > 0;
    } catch (err: any) {
      addLog(`Errore aggiornamento verifica PostgreSQL per utente ID ${userId}: ${err.message}`, 'error');
    }
  }

  // Fallback local JSON DB
  if (!adminDb.users) adminDb.users = [];
  const u = adminDb.users.find(usr => usr.id === userId);
  if (u) {
    u.isVerified = isVerified;
    u.verificationCode = verificationCode;
    saveAdminDb();
    addLog(`Stato di verifica aggiornato in JSON per utente ID ${userId}: verified=${isVerified}`);
    return true;
  }
  return false;
}

async function updateUser(user: DbUser): Promise<boolean> {
  const pool = getPgPool();
  if (pool) {
    try {
      const res = await pool.query(
        'UPDATE users SET username = $1, email = $2, password = $3, rating = $4, wins = $5, losses = $6 WHERE id = $7',
        [user.username, user.email, user.password || '', user.rating, user.wins, user.losses, user.id]
      );
      addLog(`Utente aggiornato in PostgreSQL: ${user.username}`);
      return (res.rowCount ?? 0) > 0;
    } catch (err: any) {
      addLog('Errore durante l\'aggiornamento dell\'utente in PostgreSQL: ' + err.message, 'error');
      throw err;
    }
  }

  if (!adminDb.users) adminDb.users = [];
  const idx = adminDb.users.findIndex(u => u.id === user.id);
  if (idx === -1) return false;

  const duplicate = adminDb.users.some(u => u.id !== user.id && (u.username.toLowerCase() === user.username.toLowerCase() || u.email.toLowerCase() === user.email.toLowerCase()));
  if (duplicate) {
    throw new Error('Username o Email già in uso da un altro utente.');
  }

  adminDb.users[idx] = {
    ...adminDb.users[idx],
    username: user.username,
    email: user.email,
    password: user.password !== undefined ? user.password : adminDb.users[idx].password,
    rating: user.rating,
    wins: user.wins,
    losses: user.losses
  };
  saveAdminDb();
  addLog(`Utente aggiornato nel database JSON locale: ${user.username}`);
  return true;
}

async function deleteUser(id: string): Promise<boolean> {
  const pool = getPgPool();
  if (pool) {
    try {
      const res = await pool.query('DELETE FROM users WHERE id = $1', [id]);
      addLog(`Utente eliminato da PostgreSQL: ${id}`);
      return (res.rowCount ?? 0) > 0;
    } catch (err: any) {
      addLog('Errore durante l\'eliminazione dell\'utente in PostgreSQL: ' + err.message, 'error');
      throw err;
    }
  }

  if (!adminDb.users) adminDb.users = [];
  const idx = adminDb.users.findIndex(u => u.id === id);
  if (idx === -1) return false;

  const username = adminDb.users[idx].username;
  adminDb.users.splice(idx, 1);
  saveAdminDb();
  addLog(`Utente eliminato dal database JSON locale: ${username}`);
  return true;
}

async function updateUserStatsByUsername(username: string, rating: number, wins: number, losses: number) {
  const pool = getPgPool();
  if (pool) {
    try {
      await pool.query(
        'UPDATE users SET rating = $1, wins = $2, losses = $3 WHERE LOWER(username) = LOWER($4)',
        [rating, wins, losses, username.trim()]
      );
      addLog(`Stats di gioco aggiornate in Postgres per ${username}: rating=${rating}, wins=${wins}, losses=${losses}`);
      return;
    } catch (err: any) {
      addLog(`Errore aggiornamento stats Postgres per ${username}: ${err.message}`, 'error');
    }
  }

  // Fallback to local JSON DB
  if (!adminDb.users) adminDb.users = [];
  const u = adminDb.users.find(usr => usr.username.toLowerCase() === username.trim().toLowerCase());
  if (u) {
    u.rating = rating;
    u.wins = wins;
    u.losses = losses;
    saveAdminDb();
    addLog(`Stats di gioco aggiornate in JSON per ${username}: rating=${rating}, wins=${wins}, losses=${losses}`);
  }
}

// Database abstractions to read/write tracks
async function getTracks(): Promise<{ id: string; name: string; url: string; isLocal?: boolean }[]> {
  const pool = getPgPool();
  if (pool) {
    try {
      const res = await pool.query('SELECT id, name, url, is_local as "isLocal" FROM music_tracks ORDER BY id DESC');
      return res.rows;
    } catch (err) {
      console.error('Failed to query tracks from PostgreSQL, falling back to local database:', err);
    }
  }
  return adminDb.musicTracks || [];
}

async function addTrack(track: { id: string; name: string; url: string; isLocal?: boolean }): Promise<void> {
  const pool = getPgPool();
  if (pool) {
    try {
      await pool.query(
        'INSERT INTO music_tracks (id, name, url, is_local) VALUES ($1, $2, $3, $4)',
        [track.id, track.name, track.url, track.isLocal || false]
      );
      return;
    } catch (err) {
      console.error('Failed to insert track into PostgreSQL:', err);
      throw err;
    }
  }
  
  if (!adminDb.musicTracks) adminDb.musicTracks = [];
  adminDb.musicTracks.push(track);
  saveAdminDb();
}

async function deleteTrack(id: string): Promise<boolean> {
  const pool = getPgPool();
  if (pool) {
    try {
      const res = await pool.query('DELETE FROM music_tracks WHERE id = $1', [id]);
      return (res.rowCount ?? 0) > 0;
    } catch (err) {
      console.error('Failed to delete track from PostgreSQL:', err);
      throw err;
    }
  }

  if (!adminDb.musicTracks) adminDb.musicTracks = [];
  const trackIndex = adminDb.musicTracks.findIndex(t => t.id === id);
  if (trackIndex === -1) return false;
  adminDb.musicTracks.splice(trackIndex, 1);
  saveAdminDb();
  return true;
}

let adminSessionToken: string | null = null;

const isTokenValid = (req: express.Request): boolean => {
  const token = req.headers['x-admin-token'] || req.body?.token || req.query?.token;
  return adminSessionToken !== null && token === adminSessionToken;
};

async function startServer() {
  const app = express();
  const PORT = Number(process.env.PORT) || 3000;

  // Initialize PostgreSQL schema if DATABASE_URL is present
  await initPgDb();

  // Configure larger limits for base64 file uploads
  app.use(express.json({ limit: '50mb' }));
  app.use(express.urlencoded({ limit: '50mb', extended: true }));

  // Ensure uploads directory exists and is served statically
  const UPLOADS_DIR = path.join(process.cwd(), 'uploads');
  if (!fs.existsSync(UPLOADS_DIR)) {
    fs.mkdirSync(UPLOADS_DIR, { recursive: true });
  }
  app.use('/uploads', express.static(UPLOADS_DIR));

  app.get('/api/health', (req, res) => {
    res.json({ status: 'ok', onlinePlayersCount: Object.keys(players).length });
  });

  // Music REST API Route - fetches tracks from PostgreSQL or local database
  app.get('/api/music', async (req, res) => {
    try {
      const currentTracks = await getTracks();
      res.json({ success: true, tracks: currentTracks });
    } catch (err) {
      console.error('Error fetching tracks:', err);
      res.status(500).json({ success: false, message: 'Errore nel caricamento delle musiche.' });
    }
  });

  // Admin File Upload endpoint (Secure - checks token)
  app.post('/api/music/upload', async (req, res) => {
    if (!isTokenValid(req)) {
      return res.status(403).json({ success: false, message: 'Non autorizzato. Solo gli amministratori possono caricare file.' });
    }
    const { name, base64 } = req.body;
    if (!name || !base64) {
      return res.status(400).json({ success: false, message: 'Nome e file audio base64 sono obbligatori.' });
    }

    try {
      const currentTracks = await getTracks();
      if (currentTracks.length >= 5) {
        return res.status(400).json({ 
          success: false, 
          message: 'Limite massimo di 5 tracce audio raggiunto. Per favore, elimina una traccia esistente prima di caricarne una nuova.' 
        });
      }

      const commaIndex = base64.indexOf(',');
      const base64Data = commaIndex !== -1 ? base64.substring(commaIndex + 1) : base64;
      const buffer = Buffer.from(base64Data, 'base64');

      // Sanitise and generate safe filename
      const cleanName = name.replace(/[^a-zA-Z0-9.\-_]/g, '_');
      const uniqueFilename = `${Date.now()}_${cleanName}`;
      const filePath = path.join(UPLOADS_DIR, uniqueFilename);

      fs.writeFileSync(filePath, buffer);

      const trackUrl = `/uploads/${uniqueFilename}`;
      const newTrack = {
        id: `local_${Math.random().toString(36).substring(2, 9)}`,
        name: name.replace(/\.mp3$/i, '').trim(),
        url: trackUrl,
        isLocal: true
      };

      await addTrack(newTrack);

      res.json({ success: true, track: newTrack });
    } catch (err: any) {
      console.error('File write error:', err);
      res.status(500).json({ success: false, message: 'Impossibile salvare il file MP3: ' + err.message });
    }
  });

  // Admin Delete endpoint (Secure - checks token)
  app.delete('/api/music/:id', async (req, res) => {
    if (!isTokenValid(req)) {
      return res.status(403).json({ success: false, message: 'Non autorizzato. Solo gli amministratori possono eliminare file.' });
    }
    const { id } = req.params;
    try {
      const currentTracks = await getTracks();
      const track = currentTracks.find(t => t.id === id);
      if (!track) {
        return res.status(404).json({ success: false, message: 'Traccia non trovata.' });
      }

      // Clean up physical file if it resides in /uploads
      if (track.url.startsWith('/uploads/')) {
         const filename = path.basename(track.url);
        const filePath = path.join(UPLOADS_DIR, filename);
        if (fs.existsSync(filePath)) {
          try {
            fs.unlinkSync(filePath);
          } catch (err) {
            console.error('Failed to delete file:', filePath, err);
          }
        }
      }

      const deleted = await deleteTrack(id);
      if (deleted) {
        res.json({ success: true, message: 'Traccia eliminata con successo.' });
      } else {
        res.status(400).json({ success: false, message: 'Errore durante l\'eliminazione della traccia.' });
      }
    } catch (err: any) {
      console.error('Error deleting track:', err);
      res.status(500).json({ success: false, message: 'Errore durante l\'eliminazione: ' + err.message });
    }
  });

  // CAPTCHA Storage in memory
  const activeCaptchas: Record<string, { answer: string; expiresAt: number }> = {};

  // Clean expired captchas periodically
  setInterval(() => {
    const now = Date.now();
    for (const id in activeCaptchas) {
      if (activeCaptchas[id].expiresAt < now) {
        delete activeCaptchas[id];
      }
    }
  }, 60000);

  function generateCaptchaSvg(text: string): string {
    const width = 160;
    const height = 55;
    let lines = '';
    for (let i = 0; i < 6; i++) {
      const x1 = Math.floor(Math.random() * width);
      const y1 = Math.floor(Math.random() * height);
      const x2 = Math.floor(Math.random() * width);
      const y2 = Math.floor(Math.random() * height);
      lines += `<line x1="${x1}" y1="${y1}" x2="${x2}" y2="${y2}" stroke="rgba(245, 158, 11, 0.25)" stroke-width="1.5" />`;
    }
    
    // Some background noise circles
    let circles = '';
    for (let i = 0; i < 15; i++) {
      const cx = Math.floor(Math.random() * width);
      const cy = Math.floor(Math.random() * height);
      const r = Math.floor(Math.random() * 3) + 1;
      circles += `<circle cx="${cx}" cy="${cy}" r="${r}" fill="rgba(245, 158, 11, 0.15)" />`;
    }

    let chars = '';
    for (let i = 0; i < text.length; i++) {
      const char = text[i];
      const x = 18 + i * 26;
      const y = 35 + (Math.random() * 10 - 5);
      const rotate = Math.floor(Math.random() * 30 - 15);
      chars += `<text x="${x}" y="${y}" fill="#f59e0b" font-size="28" font-weight="900" font-family="Courier New, monospace" transform="rotate(${rotate} ${x} ${y})">${char}</text>`;
    }
    
    return `<svg width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" xmlns="http://www.w3.org/2000/svg" style="background:#090504; border:1.5px solid #78350f; border-radius:12px;">
      ${circles}
      ${lines}
      ${chars}
    </svg>`;
  }

  async function sendVerificationEmail(email: string, username: string, code: string): Promise<boolean> {
    const host = process.env.SMTP_HOST || adminDb.smtp?.host;
    const port = process.env.SMTP_PORT || adminDb.smtp?.port;
    const user = process.env.SMTP_USER || adminDb.smtp?.user;
    const pass = process.env.SMTP_PASS || adminDb.smtp?.pass;
    const from = process.env.SMTP_FROM || adminDb.smtp?.from || '"Circolo degli Scacchi" <no-reply@circoloscacchi.it>';

    const htmlContent = `
      <div style="background-color: #0c0806; color: #f5f5f4; font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif; padding: 40px; border-radius: 16px; max-width: 600px; margin: 0 auto; border: 2px solid #78350f;">
        <div style="text-align: center; margin-bottom: 30px;">
          <h2 style="color: #f59e0b; font-family: serif; font-size: 28px; margin: 0; text-transform: uppercase; letter-spacing: 2px;">Circolo degli Scacchi</h2>
          <p style="color: #a8a29e; font-size: 14px; margin-top: 5px;">Codice di Sicurezza e Verifica Account</p>
        </div>
        <div style="background-color: #1c1917; padding: 30px; border-radius: 12px; border: 1px solid #44403c; text-align: center;">
          <p style="font-size: 16px; margin-top: 0;">Ciao <strong style="color: #f59e0b;">${username}</strong>,</p>
          <p style="font-size: 14px; color: #d6d3d1; line-height: 1.6;">Benvenuto nel club! Per completare la tua iscrizione ed abilitare le partite multiplayer classificate e il tracciamento del punteggio Elo, inserisci il seguente codice di sicurezza nell'applicazione:</p>
          <div style="background-color: #0c0806; padding: 15px 30px; border-radius: 8px; border: 1px solid #78350f; display: inline-block; margin: 25px 0; letter-spacing: 8px; font-size: 32px; font-weight: bold; font-family: monospace; color: #f59e0b;">
            ${code}
          </div>
          <p style="font-size: 12px; color: #78716c; margin-bottom: 0;">Questo codice scadrà tra 15 minuti. Se non hai richiesto tu questa iscrizione, ignora semplicemente questo messaggio.</p>
        </div>
        <div style="text-align: center; margin-top: 30px; color: #57534e; font-size: 11px;">
          Circolo degli Scacchi d'Elite • Gioca gratis online su Android e iOS • © 2026
        </div>
      </div>
    `;

    addLog(`[SICUREZZA] Generato codice di verifica per ${username} (${email}): ${code}`, 'info');

    if (host && port && user && pass) {
      try {
        const transporter = nodemailer.createTransport({
          host,
          port: parseInt(port),
          secure: parseInt(port) === 465,
          auth: { user, pass }
        });
        await transporter.sendMail({
          from,
          to: email,
          subject: `Codice di Verifica Circolo degli Scacchi: ${code}`,
          html: htmlContent
        });
        addLog(`[EMAIL INVIATA] Email di verifica inviata con successo a ${email}`, 'info');
        return true;
      } catch (err: any) {
        addLog(`[ERRORE SMTP] Impossibile inviare email a ${email} tramite SMTP: ${err.message}. Fallback su stampa nei logs.`, 'warn');
      }
    } else {
      addLog(`[MOCK EMAIL] Configurazione SMTP assente. Per scopi di test in ambiente sandbox, inserisci il codice: ${code}`, 'warn');
    }
    return false;
  }

  // Get a CAPTCHA challenge
  app.get('/api/auth/captcha', (req, res) => {
    const chars = '23456789ABCDEFGHJKLMNPQRSTUVWXYZ'; // easily readable chars, excluding 1, 0, I, O
    let captchaText = '';
    for (let i = 0; i < 5; i++) {
      captchaText += chars[Math.floor(Math.random() * chars.length)];
    }
    const captchaId = `cap_${Math.random().toString(36).substring(2, 9)}`;
    activeCaptchas[captchaId] = {
      answer: captchaText.toLowerCase(),
      expiresAt: Date.now() + 5 * 60 * 1000 // 5 minutes
    };
    
    const svg = generateCaptchaSvg(captchaText);
    res.json({
      success: true,
      captchaId,
      svg
    });
  });

  // Verify User Email with Code
  app.post('/api/auth/verify', async (req, res) => {
    const { userId, code } = req.body;
    if (!userId || !code) {
      return res.status(400).json({ success: false, message: 'Dati mancanti per la verifica.' });
    }
    
    try {
      // Find user
      const users = await getUsers();
      const user = users.find(u => u.id === userId);
      if (!user) {
        return res.status(404).json({ success: false, message: 'Utente non trovato.' });
      }
      
      if (user.isVerified) {
        return res.json({ success: true, message: 'Questo account è già stato verificato con successo!' });
      }

      if (user.verificationCode !== code.trim()) {
        return res.status(400).json({ success: false, message: 'Il codice inserito non è valido.' });
      }

      // Mark verified
      await updateUserVerification(user.id, true, '');
      addLog(`Account verificato con successo per l'utente: ${user.username}`, 'info');

      res.json({
        success: true,
        message: 'Email verificata con successo! Ora puoi accedere.',
        user: {
          id: user.id,
          username: user.username,
          email: user.email,
          rating: user.rating,
          wins: user.wins,
          losses: user.losses
        }
      });
    } catch (err: any) {
      res.status(500).json({ success: false, message: 'Errore di verifica: ' + err.message });
    }
  });

  // Public User Auth API - Register
  app.post('/api/auth/register', async (req, res) => {
    const { username, email, password, captchaId, captchaAnswer } = req.body;
    if (!username || !email || !password || !captchaId || !captchaAnswer) {
      return res.status(400).json({ success: false, message: 'Tutti i campi (username, email, password, captcha) sono obbligatori.' });
    }

    // Verify Captcha
    const storedCaptcha = activeCaptchas[captchaId];
    if (!storedCaptcha || storedCaptcha.expiresAt < Date.now()) {
      return res.status(400).json({ success: false, message: 'Il CAPTCHA è scaduto o non valido. Riprova.' });
    }
    if (storedCaptcha.answer !== captchaAnswer.trim().toLowerCase()) {
      return res.status(400).json({ success: false, message: 'Codice CAPTCHA non corretto.' });
    }
    // Delete captcha after single use
    delete activeCaptchas[captchaId];

    const cleanUsername = username.trim();
    const cleanEmail = email.trim();
    const cleanPassword = password.trim();

    if (cleanUsername.length < 2) {
      return res.status(400).json({ success: false, message: 'L\'username deve contenere almeno 2 caratteri.' });
    }

    try {
      const existing = await getUserByUsername(cleanUsername);
      if (existing) {
        return res.status(400).json({ success: false, message: 'Questo username è già registrato.' });
      }

      const users = await getUsers();
      const emailTaken = users.some(u => u.email.toLowerCase() === cleanEmail.toLowerCase());
      if (emailTaken) {
        return res.status(400).json({ success: false, message: 'Questa email è già registrata.' });
      }

      // Generate 6-digit verification code
      const code = Math.floor(100000 + Math.random() * 900000).toString();

      const newUser: DbUser = {
        id: `usr_${Math.random().toString(36).substring(2, 9)}`,
        username: cleanUsername,
        email: cleanEmail,
        password: cleanPassword,
        rating: 1500,
        wins: 0,
        losses: 0,
        isVerified: false,
        verificationCode: code
      };

      await addUser(newUser);
      
      // Send Email in background
      sendVerificationEmail(cleanEmail, cleanUsername, code);

      addLog(`Nuovo utente registrato (in attesa di verifica): ${cleanUsername} (${cleanEmail})`, 'info');
      res.json({ 
        success: true, 
        message: 'Registrazione avvenuta! Inserisci il codice di sicurezza inviato via email per verificare il tuo account.', 
        unverified: true,
        userId: newUser.id,
        email: cleanEmail
      });
    } catch (err: any) {
      addLog(`Errore durante la registrazione di ${cleanUsername}: ` + err.message, 'error');
      res.status(500).json({ success: false, message: 'Errore durante la registrazione: ' + err.message });
    }
  });

  // Public User Auth API - Login
  app.post('/api/auth/login', async (req, res) => {
    const { username, password } = req.body;
    if (!username || !password) {
      return res.status(400).json({ success: false, message: 'Username e password sono richiesti.' });
    }
    try {
      const user = await getUserByUsername(username.trim());
      if (!user || user.password !== password.trim()) {
        addLog(`Tentativo di login fallito per l'utente: ${username}`, 'warn');
        return res.status(401).json({ success: false, message: 'Username o password non validi.' });
      }

      // Check if verified!
      if (!user.isVerified) {
        // Resend verification code
        const code = Math.floor(100000 + Math.random() * 900000).toString();
        await updateUserVerification(user.id, false, code);
        sendVerificationEmail(user.email, user.username, code);
        
        addLog(`Utente non verificato ha tentato il login: ${user.username}. Nuovo codice inviato.`, 'warn');
        return res.json({
          success: true,
          unverified: true,
          userId: user.id,
          email: user.email,
          message: 'Questo account non è verificato. Abbiamo inviato un nuovo codice di verifica alla tua email.'
        });
      }

      addLog(`Utente loggato con successo: ${user.username}`, 'info');
      res.json({
        success: true,
        user: {
          id: user.id,
          username: user.username,
          email: user.email,
          rating: user.rating,
          wins: user.wins,
          losses: user.losses
        }
      });
    } catch (err: any) {
      addLog(`Errore durante il login di ${username}: ` + err.message, 'error');
      res.status(500).json({ success: false, message: 'Errore durante il login: ' + err.message });
    }
  });

  // Admin API - Get Application Operations Log
  app.get('/api/admin/logs', (req, res) => {
    if (!isTokenValid(req)) {
      return res.status(403).json({ success: false, message: 'Non autorizzato.' });
    }
    res.json({ success: true, logs: appLogs });
  });

  // Admin API - Get list of registered chess players
  app.get('/api/admin/users', async (req, res) => {
    if (!isTokenValid(req)) {
      return res.status(403).json({ success: false, message: 'Non autorizzato.' });
    }
    try {
      const allUsers = await getUsers();
      res.json({ success: true, users: allUsers });
    } catch (err: any) {
      res.status(500).json({ success: false, message: 'Errore nel recupero degli utenti: ' + err.message });
    }
  });

  // Admin API - Create/Upload a user manually
  app.post('/api/admin/users', async (req, res) => {
    if (!isTokenValid(req)) {
      return res.status(403).json({ success: false, message: 'Non autorizzato.' });
    }
    const { username, email, password, rating, wins, losses } = req.body;
    if (!username || !email || !password) {
      return res.status(400).json({ success: false, message: 'Username, Email e Password sono obbligatori.' });
    }
    try {
      const existing = await getUserByUsername(username.trim());
      if (existing) {
        return res.status(400).json({ success: false, message: 'Username già in uso.' });
      }
      const users = await getUsers();
      if (users.some(u => u.email.toLowerCase() === email.trim().toLowerCase())) {
        return res.status(400).json({ success: false, message: 'Email già registrata.' });
      }

      const newUser: DbUser = {
        id: `usr_${Math.random().toString(36).substring(2, 9)}`,
        username: username.trim(),
        email: email.trim(),
        password: password.trim(),
        rating: Number(rating) || 1500,
        wins: Number(wins) || 0,
        losses: Number(losses) || 0
      };
      await addUser(newUser);
      addLog(`Amministratore ha creato l'utente: ${newUser.username}`, 'info');
      res.json({ success: true, user: newUser, message: 'Utente creato con successo!' });
    } catch (err: any) {
      res.status(500).json({ success: false, message: 'Errore nella creazione dell\'utente: ' + err.message });
    }
  });

  // Admin API - Modify user details
  app.put('/api/admin/users/:id', async (req, res) => {
    if (!isTokenValid(req)) {
      return res.status(403).json({ success: false, message: 'Non autorizzato.' });
    }
    const { id } = req.params;
    const { username, email, password, rating, wins, losses } = req.body;
    if (!username || !email) {
      return res.status(400).json({ success: false, message: 'Username ed Email sono obbligatori.' });
    }
    try {
      const users = await getUsers();
      const current = users.find(u => u.id === id);
      if (!current) {
        return res.status(404).json({ success: false, message: 'Utente non trovato.' });
      }

      const updatedUser: DbUser = {
        id,
        username: username.trim(),
        email: email.trim(),
        password: password !== undefined ? password.trim() : current.password,
        rating: rating !== undefined ? Number(rating) : current.rating,
        wins: wins !== undefined ? Number(wins) : current.wins,
        losses: losses !== undefined ? Number(losses) : current.losses
      };
      const success = await updateUser(updatedUser);
      if (success) {
        addLog(`Amministratore ha modificato l'utente: ${updatedUser.username}`, 'info');
        res.json({ success: true, user: updatedUser, message: 'Utente aggiornato con successo!' });
      } else {
        res.status(400).json({ success: false, message: 'Errore nell\'aggiornamento dell\'utente.' });
      }
    } catch (err: any) {
      res.status(500).json({ success: false, message: 'Errore durante la modifica dell\'utente: ' + err.message });
    }
  });

  // Admin API - Delete user
  app.delete('/api/admin/users/:id', async (req, res) => {
    if (!isTokenValid(req)) {
      return res.status(403).json({ success: false, message: 'Non autorizzato.' });
    }
    const { id } = req.params;
    try {
      const success = await deleteUser(id);
      if (success) {
        addLog(`Amministratore ha eliminato l'utente con ID: ${id}`, 'info');
        res.json({ success: true, message: 'Utente eliminato con successo.' });
      } else {
        res.status(404).json({ success: false, message: 'Utente non trovato.' });
      }
    } catch (err: any) {
      res.status(500).json({ success: false, message: 'Errore nell\'eliminazione dell\'utente: ' + err.message });
    }
  });

  // Admin API - Export Users to CSV file
  app.get('/api/admin/users/csv', async (req, res) => {
    if (!isTokenValid(req)) {
      return res.status(403).send('Non autorizzato.');
    }
    try {
      const allUsers = await getUsers();
      let csvContent = '\uFEFFID,Username,Email,Password,Rating,Wins,Losses,CreatedAt\n';
      allUsers.forEach(u => {
        const row = [
          u.id,
          `"${(u.username || '').replace(/"/g, '""')}"`,
          `"${(u.email || '').replace(/"/g, '""')}"`,
          `"${(u.password || '').replace(/"/g, '""')}"`,
          u.rating,
          u.wins,
          u.losses,
          u.createdAt || ''
        ].join(',');
        csvContent += row + '\n';
      });

      addLog('Amministratore ha esportato la lista utenti in formato CSV.', 'info');
      res.setHeader('Content-Type', 'text/csv; charset=utf-8');
      res.setHeader('Content-Disposition', 'attachment; filename=utenti_scacchi_legno.csv');
      res.status(200).send(csvContent);
    } catch (err: any) {
      res.status(500).send('Errore durante l\'esportazione CSV: ' + err.message);
    }
  });

  app.post('/api/admin/login', (req, res) => {
    const { username, password } = req.body;
    if (
      username === adminDb.admin.username &&
      password === adminDb.admin.password
    ) {
      adminSessionToken = Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
      res.json({ success: true, token: adminSessionToken, username: adminDb.admin.username });
    } else {
      res.status(401).json({ success: false, message: 'Credenziali di accesso non valide.' });
    }
  });

  app.post('/api/admin/change-credentials', (req, res) => {
    if (!isTokenValid(req)) {
      return res.status(403).json({ success: false, message: 'Non autorizzato. Sessione scaduta.' });
    }
    const { username, password } = req.body;
    if (!username || !password || username.trim() === '' || password.trim() === '') {
      return res.status(400).json({ success: false, message: 'Username e Password non possono essere vuoti.' });
    }
    adminDb.admin.username = username.trim();
    adminDb.admin.password = password.trim();
    saveAdminDb();
    res.json({ success: true, message: 'Credenziali amministratore aggiornate con successo nel database.' });
  });

  // Admin Config - Save PostgreSQL Connection String
  app.post('/api/admin/config/database', async (req, res) => {
    if (!isTokenValid(req)) {
      return res.status(403).json({ success: false, message: 'Non autorizzato.' });
    }
    const { connectionString } = req.body;
    if (connectionString === undefined) {
      return res.status(400).json({ success: false, message: 'La stringa di connessione è obbligatoria.' });
    }

    try {
      if (connectionString.trim() === '') {
        adminDb.databaseUrl = '';
        saveAdminDb();
        resetPgPool();
        return res.json({ success: true, message: 'Connessione a PostgreSQL disattivata. Fallback su database JSON locale.' });
      }

      // Test connection
      const testPool = new Pool({
        connectionString: connectionString.trim(),
        ssl: connectionString.trim().includes('sslmode=') ? undefined : { rejectUnauthorized: false },
        connectionTimeoutMillis: 5000,
      });

      const client = await testPool.connect();
      try {
        await client.query('SELECT NOW()');
        // Initialize schema
        await client.query(`
          CREATE TABLE IF NOT EXISTS music_tracks (
            id VARCHAR(50) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            url TEXT NOT NULL,
            is_local BOOLEAN DEFAULT FALSE
          );
        `);
      } finally {
        client.release();
        await testPool.end();
      }

      adminDb.databaseUrl = connectionString.trim();
      saveAdminDb();
      resetPgPool(); // Reset pool so it recreates with new URL
      await initPgDb(); // Reinitialize pool schema

      res.json({ success: true, message: 'Connessione a PostgreSQL stabilita e inizializzata con successo!' });
    } catch (err: any) {
      console.error('PostgreSQL connection test failed:', err);
      res.status(500).json({ success: false, message: 'Errore di connessione a PostgreSQL: ' + err.message });
    }
  });

  // Admin Config - Get PostgreSQL status
  app.get('/api/admin/config/status', (req, res) => {
    if (!isTokenValid(req)) {
      return res.status(403).json({ success: false, message: 'Non autorizzato.' });
    }

    const isEnvVar = !!process.env.DATABASE_URL;
    const connectionString = process.env.DATABASE_URL || adminDb.databaseUrl || '';
    const configured = !!connectionString;
    
    // Mask password in string for safety
    let masked = '';
    if (connectionString) {
      try {
        const urlObj = new URL(connectionString);
        if (urlObj.password) {
          urlObj.password = '********';
        }
        masked = urlObj.toString();
      } catch (e) {
        masked = 'Configurata (stringa non valida o non-URL standard)';
      }
    }

    res.json({
      success: true,
      postgresConfigured: configured,
      postgresUrl: masked,
      isEnvVar
    });
  });

  // Admin Config - Get SMTP configuration
  app.get('/api/admin/config/smtp', (req, res) => {
    if (!isTokenValid(req)) {
      return res.status(403).json({ success: false, message: 'Non autorizzato.' });
    }
    const smtp = adminDb.smtp || { host: '', port: '', user: '', pass: '', from: '' };
    res.json({
      success: true,
      smtp: {
        host: smtp.host || '',
        port: smtp.port || '',
        user: smtp.user || '',
        pass: smtp.pass || '',
        from: smtp.from || ''
      },
      isEnvVar: !!(process.env.SMTP_HOST && process.env.SMTP_PORT && process.env.SMTP_USER && process.env.SMTP_PASS)
    });
  });

  // Admin Config - Update SMTP configuration
  app.post('/api/admin/config/smtp', (req, res) => {
    if (!isTokenValid(req)) {
      return res.status(403).json({ success: false, message: 'Non autorizzato.' });
    }
    const { host, port, user, pass, from } = req.body;
    
    adminDb.smtp = {
      host: (host || '').trim(),
      port: (port || '').trim(),
      user: (user || '').trim(),
      pass: (pass || '').trim(),
      from: (from || '').trim()
    };
    
    saveAdminDb();
    addLog(`[CONFIG SMTP] Configurazione SMTP aggiornata dall'amministratore`, 'info');
    res.json({ success: true, message: 'Configurazione SMTP salvata con successo!' });
  });

  app.post('/api/admin/music', async (req, res) => {
    if (!isTokenValid(req)) {
      return res.status(403).json({ success: false, message: 'Non autorizzato.' });
    }
    const { name, url } = req.body;
    if (!name || !url || name.trim() === '' || url.trim() === '') {
      return res.status(400).json({ success: false, message: 'Il nome e l\'URL della musica sono obbligatori.' });
    }
    try {
      const currentTracks = await getTracks();
      if (currentTracks.length >= 5) {
        return res.status(400).json({ 
          success: false, 
          message: 'Limite massimo di 5 tracce raggiunto. Elimina una traccia prima di aggiungerne una nuova.' 
        });
      }
      const newTrack = {
        id: Math.random().toString(36).substring(2, 9),
        name: name.trim(),
        url: url.trim()
      };
      await addTrack(newTrack);
      res.json({ success: true, track: newTrack });
    } catch (err: any) {
      res.status(500).json({ success: false, message: 'Errore nel salvataggio della traccia: ' + err.message });
    }
  });

  app.delete('/api/admin/music/:id', async (req, res) => {
    if (!isTokenValid(req)) {
      return res.status(403).json({ success: false, message: 'Non autorizzato.' });
    }
    const { id } = req.params;
    try {
      const currentTracks = await getTracks();
      const track = currentTracks.find(t => t.id === id);
      if (!track) {
        return res.status(404).json({ success: false, message: 'Musica non trovata.' });
      }

      if (track.url.startsWith('/uploads/')) {
        const filename = path.basename(track.url);
        const filePath = path.join(UPLOADS_DIR, filename);
        if (fs.existsSync(filePath)) {
          try {
            fs.unlinkSync(filePath);
          } catch (err) {
            console.error('Failed to delete file:', filePath, err);
          }
        }
      }

      const deleted = await deleteTrack(id);
      if (deleted) {
        res.json({ success: true, message: 'Musica d\'atmosfera eliminata con successo.' });
      } else {
        res.status(400).json({ success: false, message: 'Errore nell\'eliminazione della traccia.' });
      }
    } catch (err: any) {
      res.status(500).json({ success: false, message: 'Errore: ' + err.message });
    }
  });

  // Create the standard HTTP server
  const server = http.createServer(app);

  // Initialize WebSockets server
  const wss = new WebSocketServer({ server });

  // Helper code to broadcast message to all connected and registered users
  const broadcast = (message: any) => {
    const stringMessage = JSON.stringify(message);
    Object.values(players).forEach(p => {
      if (p.ws.readyState === WebSocket.OPEN) {
        p.ws.send(stringMessage);
      }
    });
  };

  // Helper code to send message to a specific player
  const sendTo = (ws: WebSocket, message: any) => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(message));
    }
  };

  // Fetch only active public player entries
  const getOnlinePlayersList = (): Player[] => {
    return Object.values(players).map(({ id, name, status, rating, wins, losses, lastActive }) => ({
      id,
      name,
      status,
      rating,
      wins,
      losses,
      lastActive
    }));
  };

  wss.on('connection', (ws: WebSocket) => {
    // Generate unique ID and a default Guest identity immediately on connection
    const id = Math.random().toString(36).substring(2, 9);
    const guestNum = Math.floor(1000 + Math.random() * 9000);
    const name = `Ospite_${guestNum}`;
    let currentPlayerId: string | null = id;

    const newPlayer: Player = {
      id,
      name,
      status: 'attesa',
      rating: 1500,
      wins: 0,
      losses: 0,
      lastActive: Date.now()
    };

    // Register player immediately so they are listed online
    players[id] = { ...newPlayer, ws };

    // Send immediate connect ack containing their default guest profile
    sendTo(ws, {
      type: 'connect_ack',
      payload: {
        player: newPlayer,
        leaderboard: [...leaderboard].sort((a, b) => b.rating - a.rating),
        onlinePlayers: getOnlinePlayersList(),
        activeGamesCount: Object.keys(games).length
      }
    });

    // Notify all other online players of this new guest
    broadcast({
      type: 'players_update',
      payload: { onlinePlayers: getOnlinePlayersList() }
    });

    ws.on('message', async (rawData: string) => {
      try {
        const message = JSON.parse(rawData);
        const { type, payload } = message;

        // 1. Initial connection with Nickname (acts as rename / custom profile configuration now!)
        if (type === 'register_player') {
          const name = payload.name.trim();

          // Check for empty or illegal name
          if (!name) {
            sendTo(ws, { type: 'error', payload: { message: 'Il nome non può essere vuoto.' } });
            return;
          }

          // Check uniqueness (excluding current connection)
          const isTaken = Object.values(players).some(p => p.id !== currentPlayerId && p.name.toLowerCase() === name.toLowerCase());
          if (isTaken) {
            sendTo(ws, { type: 'error', payload: { message: 'Questo nome è già in uso. Scegline un altro.' } });
            return;
          }

          if (currentPlayerId && players[currentPlayerId]) {
            const p = players[currentPlayerId];
            p.name = name;

            // Fetch actual database details if exists
            getUserByUsername(name).then(dbUser => {
              if (dbUser) {
                p.rating = dbUser.rating;
                p.wins = dbUser.wins;
                p.losses = dbUser.losses;
              } else {
                p.rating = 1500;
                p.wins = 0;
                p.losses = 0;
              }

              addLog(`Giocatore connesso alla lobby degli scacchi: ${p.name} (${p.rating} Elo)`, 'info');

              // Check if player already existed in leaderboard to retrieve their score
              const matchedEntry = leaderboard.find(e => e.name.toLowerCase() === name.toLowerCase());
              if (matchedEntry) {
                matchedEntry.rating = p.rating;
                matchedEntry.wins = p.wins;
                matchedEntry.losses = p.losses;
              } else {
                // Register them in the leaderboard
                leaderboard.push({ name, rating: p.rating, wins: p.wins, losses: p.losses });
              }
              saveLeaderboard();

              // Acknowledge connection with new name
              sendTo(ws, {
                type: 'connect_ack',
                payload: {
                  player: p,
                  leaderboard: [...leaderboard].sort((a, b) => b.rating - a.rating),
                  onlinePlayers: getOnlinePlayersList(),
                  activeGamesCount: Object.keys(games).length
                }
              });

              // Broadcast players list update
              broadcast({
                type: 'players_update',
                payload: { onlinePlayers: getOnlinePlayersList() }
              });
              broadcast({
                type: 'leaderboard_update',
                payload: { leaderboard: [...leaderboard].sort((a, b) => b.rating - a.rating) }
              });
            }).catch(err => {
              console.error('Error fetching player stats during websocket join:', err);
            });
          }
        }

        // 2. Issuing a challenge
        else if (type === 'challenge_player') {
          if (!currentPlayerId) return;
          const { targetId } = payload;
          const sender = players[currentPlayerId];
          const target = players[targetId];

          if (!sender) return;
          if (!target) {
            sendTo(ws, { type: 'error', payload: { message: 'Giocatore non trovato.' } });
            return;
          }

          if (target.status === 'occupato') {
            sendTo(ws, { type: 'error', payload: { message: "Il giocatore è occupato in un'altra partita." } });
            return;
          }

          // Register pending challenge
          pendingChallenges[target.id] = sender.id;

          // Notify the challenged target
          sendTo(target.ws, {
            type: 'incoming_challenge',
            payload: {
              challengerId: sender.id,
              challengerName: sender.name
            }
          });

          // Confirm to challenger that the challenge is pending
          sendTo(sender.ws, {
            type: 'challenge_pending',
            payload: {
              targetId: target.id,
              targetName: target.name
            }
          });
        }

        // 2a. Accepting a challenge
        else if (type === 'accept_challenge') {
          if (!currentPlayerId) return;
          const challengerId = pendingChallenges[currentPlayerId];
          if (!challengerId) {
            sendTo(ws, { type: 'error', payload: { message: 'Nessuna sfida in sospeso trovata.' } });
            return;
          }

          const sender = players[challengerId]; // The challenger
          const target = players[currentPlayerId];  // The challengee (current player)

          if (!sender) {
            sendTo(ws, { type: 'error', payload: { message: 'Lo sfidante non è più online.' } });
            delete pendingChallenges[currentPlayerId];
            return;
          }
          if (!target) return;

          if (sender.status === 'occupato') {
            sendTo(ws, { type: 'error', payload: { message: 'Lo sfidante è ora occupato in un\'altra partita.' } });
            delete pendingChallenges[currentPlayerId];
            return;
          }

          // Generate an active chess game room
          const gameId = `game_${Math.random().toString(36).substring(2, 9)}`;
          
          // Randomly assign white and black players
          const isSenderWhite = Math.random() > 0.5;
          const whitePlayer = isSenderWhite ? sender : target;
          const blackPlayer = isSenderWhite ? target : sender;

          const newGame: ChessGame = {
            id: gameId,
            whitePlayer: { id: whitePlayer.id, name: whitePlayer.name, status: 'occupato', rating: whitePlayer.rating, wins: whitePlayer.wins, losses: whitePlayer.losses, lastActive: Date.now() },
            blackPlayer: { id: blackPlayer.id, name: blackPlayer.name, status: 'occupato', rating: blackPlayer.rating, wins: blackPlayer.wins, losses: blackPlayer.losses, lastActive: Date.now() },
            status: 'attivo', // starts instantly
            fen: 'rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1',
            turn: 'w',
            moves: [],
            chat: [
              {
                id: 'welcome',
                senderName: 'Tavolo',
                text: `La sfida è iniziata tra ${sender.name} e ${target.name}! Buona fortuna!`,
                timestamp: Date.now()
              }
            ],
            createdAt: Date.now(),
            updatedAt: Date.now(),
            isVsComputer: false
          };

          // Save game
          games[gameId] = newGame;

          // Change player statuses to busy representatively
          sender.status = 'occupato';
          target.status = 'occupato';

          // Clean up challenge
          delete pendingChallenges[currentPlayerId];

          // Notify both players of game start
          sendTo(sender.ws, { type: 'game_start', payload: { game: newGame } });
          sendTo(target.ws, { type: 'game_start', payload: { game: newGame } });

          // Broadcast lobby state updates
          broadcast({
            type: 'players_update',
            payload: { onlinePlayers: getOnlinePlayersList() }
          });
        }

        // 2b. Declining a challenge
        else if (type === 'decline_challenge') {
          if (!currentPlayerId) return;
          const challengerId = pendingChallenges[currentPlayerId];
          if (challengerId) {
            const sender = players[challengerId];
            if (sender) {
              sendTo(sender.ws, {
                type: 'challenge_declined',
                payload: {
                  targetId: currentPlayerId,
                  targetName: players[currentPlayerId]?.name || 'Il giocatore'
                }
              });
            }
          }
          delete pendingChallenges[currentPlayerId];
        }

        // 3. Modifying active chess game with a move
        else if (type === 'game_move') {
          if (!currentPlayerId) return;
          const { gameId, from, to, fen, moves, turn, status, winnerId, opponentSound } = payload;
          const game = games[gameId];

          if (!game) {
            sendTo(ws, { type: 'error', payload: { message: 'Partita non trovata.' } });
            return;
          }

          // Apply state to our backend authoritative store
          game.fen = fen;
          game.turn = turn;
          game.moves = moves;
          game.status = status;
          game.updatedAt = Date.now();

          // Let's check for terminal statuses to award points!
          if (status === 'scacco_matto' && winnerId) {
            game.winnerId = winnerId;

            // Handle rating additions/subtractions in leaderboard
            const wPlayerId = game.whitePlayer.id;
            const bPlayerId = game.blackPlayer.id;
            const isWhiteWinner = winnerId === wPlayerId;

            const winnerObj = players[winnerId];
            const loserId = isWhiteWinner ? bPlayerId : wPlayerId;
            const loserObj = players[loserId];

            if (winnerObj && loserObj) {
              // Standard Elo computation basics
              const rWinner = winnerObj.rating;
              const rLoser = loserObj.rating;
              
              const expectedWinner = 1 / (1 + Math.pow(10, (rLoser - rWinner) / 400));
              const expectedLoser = 1 / (1 + Math.pow(10, (rWinner - rLoser) / 400));

              const ratingDelta = Math.round(32 * (1 - expectedWinner));
              
              winnerObj.rating += ratingDelta;
              winnerObj.wins += 1;

              loserObj.rating = Math.max(100, loserObj.rating - ratingDelta);
              loserObj.losses += 1;

              // Save to Database
              updateUserStatsByUsername(winnerObj.name, winnerObj.rating, winnerObj.wins, winnerObj.losses);
              updateUserStatsByUsername(loserObj.name, loserObj.rating, loserObj.wins, loserObj.losses);

              // Send player_update message to notify clients of their updated stats
              if (winnerObj.ws) {
                sendTo(winnerObj.ws, {
                  type: 'player_update',
                  payload: {
                    player: {
                      id: winnerObj.id,
                      name: winnerObj.name,
                      status: winnerObj.status,
                      rating: winnerObj.rating,
                      wins: winnerObj.wins,
                      losses: winnerObj.losses,
                      lastActive: winnerObj.lastActive
                    }
                  }
                });
              }
              if (loserObj.ws) {
                sendTo(loserObj.ws, {
                  type: 'player_update',
                  payload: {
                    player: {
                      id: loserObj.id,
                      name: loserObj.name,
                      status: loserObj.status,
                      rating: loserObj.rating,
                      wins: loserObj.wins,
                      losses: loserObj.losses,
                      lastActive: loserObj.lastActive
                    }
                  }
                });
              }

              // Save in leaderboard list
              const winEntry = leaderboard.find(e => e.name.toLowerCase() === winnerObj.name.toLowerCase());
              if (winEntry) {
                winEntry.rating = winnerObj.rating;
                winEntry.wins = winnerObj.wins;
              }
              const loseEntry = leaderboard.find(e => e.name.toLowerCase() === loserObj.name.toLowerCase());
              if (loseEntry) {
                loseEntry.rating = loserObj.rating;
                loseEntry.losses = loserObj.losses;
              }
              saveLeaderboard();

              // Send system notes to game's chat
              game.chat.push({
                id: `system_${Date.now()}`,
                senderName: 'Tavolo',
                text: `${winnerObj.name} ha vinto per scacco matto! (+${ratingDelta} Elo)`,
                timestamp: Date.now()
              });
            }

            // Restore waiting status
            if (players[wPlayerId]) players[wPlayerId].status = 'attesa';
            if (players[bPlayerId]) players[bPlayerId].status = 'attesa';

            // Clean up game after delay
            setTimeout(() => {
              delete games[gameId];
            }, 5000);
          } else if (status === 'patta') {
            const wPlayerId = game.whitePlayer.id;
            const bPlayerId = game.blackPlayer.id;

            game.chat.push({
              id: `system_${Date.now()}`,
              senderName: 'Tavolo',
              text: 'La partita è finita in parità.',
              timestamp: Date.now()
            });

            if (players[wPlayerId]) players[wPlayerId].status = 'attesa';
            if (players[bPlayerId]) players[bPlayerId].status = 'attesa';

            setTimeout(() => {
              delete games[gameId];
            }, 5000);
          }

          // Transmit move and evaluation to both players
          const whiteWs = players[game.whitePlayer.id]?.ws;
          const blackWs = players[game.blackPlayer.id]?.ws;

          const updateMessage = { 
            type: 'game_update', 
            payload: { game, opponentSound } 
          };

          if (whiteWs) sendTo(whiteWs, updateMessage);
          if (blackWs) sendTo(blackWs, updateMessage);

          // Update lobby scores
          broadcast({
            type: 'players_update',
            payload: { onlinePlayers: getOnlinePlayersList() }
          });
          broadcast({
            type: 'leaderboard_update',
            payload: { leaderboard: [...leaderboard].sort((a, b) => b.rating - a.rating) }
          });
        }

        // 4. In-Game Chat message transmission
        else if (type === 'game_chat') {
          if (!currentPlayerId) return;
          const { gameId, text } = payload;
          const game = games[gameId];

          if (!game) return;

          const sender = players[currentPlayerId];
          if (!sender) return;

          const newMsg = {
            id: `msg_${Math.random().toString(36).substring(2, 9)}`,
            senderName: sender.name,
            text: text.substring(0, 200), // restrict length
            timestamp: Date.now()
          };

          game.chat.push(newMsg);

          // Re-send to opponents in room
          const whiteWs = players[game.whitePlayer.id]?.ws;
          const blackWs = players[game.blackPlayer.id]?.ws;

          const chatUpdate = { type: 'game_update', payload: { game } };
          if (whiteWs) sendTo(whiteWs, chatUpdate);
          if (blackWs) sendTo(blackWs, chatUpdate);
        }

        // 5. Surrendered game (Abbandono)
        else if (type === 'game_resign') {
          if (!currentPlayerId) return;
          const { gameId } = payload;
          const game = games[gameId];

          if (!game) return;

          const loserId = currentPlayerId;
          const wPlayerId = game.whitePlayer.id;
          const bPlayerId = game.blackPlayer.id;
          const winnerId = loserId === wPlayerId ? bPlayerId : wPlayerId;

          game.status = 'abbandono';
          game.winnerId = winnerId;

          const winnerObj = players[winnerId];
          const loserObj = players[loserId];

          if (winnerObj && loserObj) {
            const rWinner = winnerObj.rating;
            const rLoser = loserObj.rating;
            
            const expectedWinner = 1 / (1 + Math.pow(10, (rLoser - rWinner) / 400));
            const ratingDelta = Math.round(32 * (1 - expectedWinner));

            winnerObj.rating += ratingDelta;
            winnerObj.wins += 1;
            loserObj.rating = Math.max(100, loserObj.rating - ratingDelta);
            loserObj.losses += 1;

            // Save to Database
            updateUserStatsByUsername(winnerObj.name, winnerObj.rating, winnerObj.wins, winnerObj.losses);
            updateUserStatsByUsername(loserObj.name, loserObj.rating, loserObj.wins, loserObj.losses);

            // Send player_update message to notify clients of their updated stats
            if (winnerObj.ws) {
              sendTo(winnerObj.ws, {
                type: 'player_update',
                payload: {
                  player: {
                    id: winnerObj.id,
                    name: winnerObj.name,
                    status: winnerObj.status,
                    rating: winnerObj.rating,
                    wins: winnerObj.wins,
                    losses: winnerObj.losses,
                    lastActive: winnerObj.lastActive
                  }
                }
              });
            }
            if (loserObj.ws) {
              sendTo(loserObj.ws, {
                type: 'player_update',
                payload: {
                  player: {
                    id: loserObj.id,
                    name: loserObj.name,
                    status: loserObj.status,
                    rating: loserObj.rating,
                    wins: loserObj.wins,
                    losses: loserObj.losses,
                    lastActive: loserObj.lastActive
                  }
                }
              });
            }

            const winEntry = leaderboard.find(e => e.name.toLowerCase() === winnerObj.name.toLowerCase());
            if (winEntry) {
              winEntry.rating = winnerObj.rating;
              winEntry.wins = winnerObj.wins;
            }
            const loseEntry = leaderboard.find(e => e.name.toLowerCase() === loserObj.name.toLowerCase());
            if (loseEntry) {
              loseEntry.rating = loserObj.rating;
              loseEntry.losses = loserObj.losses;
            }
            saveLeaderboard();

            game.chat.push({
              id: `system_${Date.now()}`,
              senderName: 'Tavolo',
              text: `${loserObj.name} ha abbandonato. ${winnerObj.name} vince! (+${ratingDelta} Elo)`,
              timestamp: Date.now()
            });
          }

          if (players[wPlayerId]) players[wPlayerId].status = 'attesa';
          if (players[bPlayerId]) players[bPlayerId].status = 'attesa';

          // Broadcast state changes
          const whiteWs = players[wPlayerId]?.ws;
          const blackWs = players[bPlayerId]?.ws;

          const resignUpdate = { type: 'game_update', payload: { game } };
          if (whiteWs) sendTo(whiteWs, resignUpdate);
          if (blackWs) sendTo(blackWs, resignUpdate);

          broadcast({
            type: 'players_update',
            payload: { onlinePlayers: getOnlinePlayersList() }
          });
          broadcast({
            type: 'leaderboard_update',
            payload: { leaderboard: [...leaderboard].sort((a, b) => b.rating - a.rating) }
          });

          // Delete game from store
          setTimeout(() => {
            delete games[gameId];
          }, 3000);
        }

        // 6. Computer Game Over (Update wins/losses and elo)
        else if (type === 'computer_game_over') {
          if (!currentPlayerId) return;
          const { won } = payload;
          const p = players[currentPlayerId];
          if (p) {
            if (won) {
              p.wins += 1;
              p.rating += 10; // Simple reward for beating the machine!
              addLog(`Giocatore ${p.name} ha vinto contro il Computer (+10 Elo)`, 'info');
            } else {
              p.losses += 1;
              p.rating = Math.max(100, p.rating - 10);
              addLog(`Giocatore ${p.name} ha perso contro il Computer (-10 Elo)`, 'info');
            }
            // Update Postgres / JSON DB
            await updateUserStatsByUsername(p.name, p.rating, p.wins, p.losses);
            
            // Broadcast leaderboard/players list changes
            const matchedEntry = leaderboard.find(e => e.name.toLowerCase() === p.name.toLowerCase());
            if (matchedEntry) {
              matchedEntry.rating = p.rating;
              matchedEntry.wins = p.wins;
              matchedEntry.losses = p.losses;
            } else {
              leaderboard.push({ name: p.name, rating: p.rating, wins: p.wins, losses: p.losses });
            }
            saveLeaderboard();

            // Notify client of their new stats
            sendTo(ws, {
              type: 'player_update',
              payload: {
                player: {
                  id: p.id,
                  name: p.name,
                  status: p.status,
                  rating: p.rating,
                  wins: p.wins,
                  losses: p.losses,
                  lastActive: p.lastActive
                }
              }
            });

            // Broadcast updates
            broadcast({
              type: 'players_update',
              payload: { onlinePlayers: getOnlinePlayersList() }
            });
            broadcast({
              type: 'leaderboard_update',
              payload: { leaderboard: [...leaderboard].sort((a, b) => b.rating - a.rating) }
            });
          }
        }

      } catch (err) {
        console.error('Error handling WebSocket message:', err);
      }
    });

    // Handle client disconnecting cleanly
    ws.on('close', () => {
      if (currentPlayerId && players[currentPlayerId]) {
        const p = players[currentPlayerId];
        console.log(`Player disconnected: ${p.name}`);

        // Handle any ongoing games that player was in
        Object.keys(games).forEach(gameId => {
          const game = games[gameId];
          if (game.whitePlayer.id === currentPlayerId || game.blackPlayer.id === currentPlayerId) {
            const opponentId = game.whitePlayer.id === currentPlayerId ? game.blackPlayer.id : game.whitePlayer.id;
            const oppPlayer = players[opponentId];
            
            if (oppPlayer) {
              oppPlayer.status = 'attesa';
              sendTo(oppPlayer.ws, {
                type: 'game_update',
                payload: {
                  game: {
                    ...game,
                    status: 'abbandono',
                    chat: [...game.chat, {
                      id: `abort_${Date.now()}`,
                      senderName: 'Tavolo',
                      text: 'Il tuo avversario si è disconnesso.',
                      timestamp: Date.now()
                    }]
                  }
                }
              });
            }
            delete games[gameId];
          }
        });

        // Delete player
        delete players[currentPlayerId];

        // Broadcast list updates
        broadcast({
          type: 'players_update',
          payload: { onlinePlayers: getOnlinePlayersList() }
        });
      }
    });
  });

  // Vite development integration
  if (process.env.NODE_ENV !== 'production') {
    const { createServer: createViteServer } = await import('vite');
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: 'spa',
    });
    app.use(vite.middlewares);
  } else {
    const distPath = path.join(process.cwd(), 'dist');
    app.use(express.static(distPath));
    app.get('*', (req, res) => {
      const indexPath = path.join(distPath, 'index.html');
      if (fs.existsSync(indexPath)) {
        res.sendFile(indexPath);
      } else {
        res.send('Chess Game Backend is running successfully!');
      }
    });
  }

  // Use the standard http server instance to listen to both express routes and WS upgrade requests!
  server.listen(PORT, '0.0.0.0', () => {
    console.log(`[ChessServer] Running and listening on http://0.0.0.0:${PORT}`);
  });
}

startServer();
