package sqlite

import (
	"database/sql"
	"errors"

	"github.com/fgrzl/kv"
	_ "github.com/mattn/go-sqlite3"
)

type store struct {
	db *sql.DB
}

func NewSQLiteStore(path string) (*store, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}

	// Create table if not exists
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS kv_store (key TEXT PRIMARY KEY, value BLOB)`)
	if err != nil {
		return nil, err
	}

	return &store{db: db}, nil
}

func (s *store) Put(item *kv.Item) error {
	_, err := s.db.Exec(`INSERT OR REPLACE INTO kv_store (key, value) VALUES (?, ?)`, item.Key, item.Value)
	return err
}

func (s *store) PutNodes(items []*kv.Item) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT OR REPLACE INTO kv_store (key, value) VALUES (?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, item := range items {
		if _, err := stmt.Exec(item.Key, item.Value); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (s *store) Remove(key string) error {
	_, err := s.db.Exec(`DELETE FROM kv_store WHERE key = ?`, key)
	return err
}

func (s *store) RemoveItems(keys ...string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`DELETE FROM kv_store WHERE key = ?`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, key := range keys {
		if _, err := stmt.Exec(key); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (s *store) GetItem(id string) (*kv.Item, error) {
	row := s.db.QueryRow(`SELECT value FROM kv_store WHERE key = ?`, id)
	var value []byte
	err := row.Scan(&value)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	return &kv.Item{Key: id, Value: value}, nil
}

func (s *store) Query(queryArgs *kv.QueryArgs) ([]*kv.Item, error) {
	var results []*kv.Item
	var query string
	var args []interface{}

	// Build the query based on QueryArgs
	if queryArgs.StartKey != "" && queryArgs.EndKey != "" {
		query = `SELECT key, value FROM kv_store WHERE key BETWEEN ? AND ?`
		args = append(args, queryArgs.StartKey, queryArgs.EndKey)
	} else if queryArgs.StartKey != "" {
		query = `SELECT key, value FROM kv_store WHERE key >= ?`
		args = append(args, queryArgs.StartKey)
	} else if queryArgs.EndKey != "" {
		query = `SELECT key, value FROM kv_store WHERE key <= ?`
		args = append(args, queryArgs.EndKey)
	} else {
		query = `SELECT key, value FROM kv_store`
	}

	// Add sorting direction
	if queryArgs.Direction == kv.Descending {
		query += ` ORDER BY key DESC`
	} else {
		query += ` ORDER BY key ASC`
	}

	// Add limit if specified
	if queryArgs.Limit > 0 {
		query += ` LIMIT ?`
		args = append(args, queryArgs.Limit)
	}

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var key string
		var value []byte
		if err := rows.Scan(&key, &value); err != nil {
			return nil, err
		}

		// Apply KeyMatch function if provided
		if queryArgs.KeyMatch != nil && !queryArgs.KeyMatch(key) {
			continue
		}

		results = append(results, &kv.Item{Key: key, Value: value})
	}

	return results, nil
}

func (s *store) Close() error {
	return s.db.Close()
}
