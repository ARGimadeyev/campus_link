package storage

import "database/sql"

type MessageRepo struct{ db *sql.DB }

func NewMessageRepo(db *sql.DB) *MessageRepo { return &MessageRepo{db: db} }

func (r *MessageRepo) SaveMessage(id, chatID, senderNick, body string, createdAt, deliveredAt int64) error {
	_, err := r.db.Exec(`INSERT OR REPLACE INTO messages(id, chat_id, sender_nick, body, created_at, delivered_at) VALUES(?,?,?,?,?,?)`, id, chatID, senderNick, body, createdAt, deliveredAt)
	return err
}

func (r *MessageRepo) MarkReadUpToLatest(chatID, readerNick string, updatedAt int64) error {
	var lastID string
	if err := r.db.QueryRow(`SELECT COALESCE((SELECT id FROM messages WHERE chat_id=? ORDER BY id DESC LIMIT 1),'')`, chatID).Scan(&lastID); err != nil {
		return err
	}
	_, err := r.db.Exec(`INSERT INTO read_state(chat_id, reader_nick, last_read_message_id, updated_at) VALUES(?,?,?,?) ON CONFLICT(chat_id, reader_nick) DO UPDATE SET last_read_message_id=excluded.last_read_message_id, updated_at=excluded.updated_at`, chatID, readerNick, lastID, updatedAt)
	return err
}
