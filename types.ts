/**
 * Versione: 1.0.0
 * Data e Ora Modifica: 02/07/2026 10:26:03 (Ora di Roma)
 * Problema Risolto: Revisione e inserimento dell'orario di modifica attuale di Roma e versione 1.0.0 in tutti i file.
 */

export type PlayerStatus = 'attesa' | 'occupato';

export interface Player {
  id: string; // Socket ID or unique generated string
  name: string;
  status: PlayerStatus;
  rating: number;
  wins: number;
  losses: number;
  lastActive: number;
}

export interface ChatMessage {
  id: string;
  senderName: string;
  text: string;
  timestamp: number;
}

export type ChessColor = 'w' | 'b';

export interface ChessPiece {
  type: 'p' | 'r' | 'n' | 'b' | 'q' | 'k'; // pawn, rook, knight, bishop, queen, king
  color: ChessColor;
}

export type Square = string; // e.g., 'e4'

export interface ChessMove {
  from: Square;
  to: Square;
  piece: ChessPiece;
  captured?: ChessPiece;
  promotion?: ChessPiece['type'];
  notation: string;
}

export interface ChessGame {
  id: string;
  whitePlayer: Player;
  blackPlayer: Player | { id: 'computer'; name: 'Computer AI'; rating: 1500; status: 'occupato'; wins: 0; losses: 0 };
  status: 'attesa' | 'attivo' | 'patta' | 'scacco_matto' | 'abbandono';
  winnerId?: string;
  fen: string; // Forsyth-Edwards Notation
  turn: ChessColor;
  moves: ChessMove[];
  chat: ChatMessage[];
  createdAt: number;
  updatedAt: number;
  isVsComputer: boolean;
  difficulty?: 'facile' | 'medio' | 'difficile';
}

export interface LeaderboardEntry {
  name: string;
  rating: number;
  wins: number;
  losses: number;
}

// WebSocket Event types
export type WsMessage =
  | { type: 'connect_ack'; payload: { player: Player; leaderboard: LeaderboardEntry[]; onlinePlayers: Player[]; activeGamesCount: number } }
  | { type: 'players_update'; payload: { onlinePlayers: Player[] } }
  | { type: 'leaderboard_update'; payload: { leaderboard: LeaderboardEntry[] } }
  | { type: 'game_update'; payload: { game: ChessGame } }
  | { type: 'game_invite'; payload: { from: Player; gameId: string } }
  | { type: 'game_start'; payload: { game: ChessGame } }
  | { type: 'error'; payload: { message: string } };
