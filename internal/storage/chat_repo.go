package storage

import "database/sql"

type ChatRepo struct{ db *sql.DB }

func NewChatRepo(db *sql.DB) *ChatRepo { return &ChatRepo{db: db} }

func (r *ChatRepo) EnsureChat(id, chatType, name string, createdAt int64) error {
	_, err := r.db.Exec(`INSERT OR IGNORE INTO chats(id, type, name, created_at) VALUES(?,?,?,?)`, id, chatType, name, createdAt)
	return err
}
func (r *ChatRepo) UpsertParticipant(chatID, nick, participantID string) error {
	_, err := r.db.Exec(`INSERT INTO chat_participants(chat_id, participant_nick, participant_id) VALUES(?,?,?) ON CONFLICT(chat_id, participant_nick) DO UPDATE SET participant_id=excluded.participant_id`, chatID, nick, participantID)
	return err
}

type ChatUnread struct {
	ChatID, Name string
	Unread       int64
}

func (r *ChatRepo) ListAllWithUnread(readerNick string, onlyUnread bool) ([]ChatUnread, error) {
	q := `SELECT c.id, c.name, COALESCE((SELECT COUNT(1) FROM messages m WHERE m.chat_id = c.id AND m.id > COALESCE((SELECT rs.last_read_message_id FROM read_state rs WHERE rs.chat_id = c.id AND rs.reader_nick = ?), '')),0) AS unread FROM chats c`
	if onlyUnread {
		q += ` WHERE COALESCE((SELECT COUNT(1) FROM messages m WHERE m.chat_id = c.id AND m.id > COALESCE((SELECT rs.last_read_message_id FROM read_state rs WHERE rs.chat_id = c.id AND rs.reader_nick = ?), '')),0) > 0`
	}
	q += ` ORDER BY unread DESC, c.created_at DESC`
	args := []any{readerNick}
	if onlyUnread {
		args = append(args, readerNick)
	}
	rows, err := r.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ChatUnread
	for rows.Next() {
		var c ChatUnread
		if err := rows.Scan(&c.ChatID, &c.Name, &c.Unread); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
