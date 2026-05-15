package storage

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

const defaultDSN = "file:campuslink.db?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)"

func OpenSQLite(dsn string) (*sql.DB, error) {
	if dsn == "" {
		dsn = defaultDSN
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}
	return db, nil
}
