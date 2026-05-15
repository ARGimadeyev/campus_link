package storage

import "database/sql"

func Migrate(db *sql.DB) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS chats (id TEXT PRIMARY KEY,type TEXT NOT NULL,name TEXT NOT NULL,created_at INTEGER NOT NULL);`,
		`CREATE TABLE IF NOT EXISTS chat_participants (chat_id TEXT NOT NULL,participant_nick TEXT NOT NULL,participant_id TEXT NOT NULL,PRIMARY KEY (chat_id, participant_nick));`,
		`CREATE TABLE IF NOT EXISTS messages (id TEXT PRIMARY KEY,chat_id TEXT NOT NULL,sender_nick TEXT NOT NULL,body TEXT NOT NULL,created_at INTEGER NOT NULL,delivered_at INTEGER NOT NULL);`,
		`CREATE TABLE IF NOT EXISTS read_state (chat_id TEXT NOT NULL,reader_nick TEXT NOT NULL,last_read_message_id TEXT NOT NULL,updated_at INTEGER NOT NULL,PRIMARY KEY (chat_id, reader_nick));`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}
