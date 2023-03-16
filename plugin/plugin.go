//go:build !noplugin
// +build !noplugin

package plugin

import (
	"fmt"
	"net/url"

	"github.com/hashicorp/go-plugin"
	"github.com/smallstep/nosql/database"
	"github.com/smallstep/nosql/plugin/shared"
)

// DB is a wrapper over *sql.DB,
type DB struct {
	client *plugin.Client
	db     shared.DBPlugin
}

func NewPluginDB() *DB {
	return &DB{}
}

// Open creates a Driver and connects to the database with the given address
// and access details.
func (db *DB) Open(dataSourceName string, opt ...database.Option) error {
	u, err := url.Parse(dataSourceName)
	if err != nil {
		return err
	}
	opts, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return err
	}
	if len(opts["cmd"]) == 0 {
		return fmt.Errorf("cmd parameter is mandatory %v", u)
	}
	cmd := opts["cmd"][0]
	client, s, err := NewPluginClient(cmd, "nosql_grpc")
	if err != nil {
		return err
	}
	db.db = s
	db.client = client
	return nil
}

// Close shutsdown the database driver.
func (db *DB) Close() error {
	db.client.Kill()
	return nil
}

// Get retrieves the column/row with given key.
func (db *DB) Get(bucket, key []byte) ([]byte, error) {
	return db.db.Get(bucket, key)
}

// Set inserts the key and value into the given bucket(column).
func (db *DB) Set(bucket, key, value []byte) error {
	return db.db.Set(bucket, key, value)
}

// Del deletes a row from the database.
func (db *DB) Del(bucket, key []byte) error {
	return db.db.Del(bucket, key)
}

// List returns the full list of entries in a column.
func (db *DB) List(bucket []byte) ([]*database.Entry, error) {
	return db.db.List(bucket)
}

// CmpAndSwap modifies the value at the given bucket and key (to newValue)
// only if the existing (current) value matches oldValue.
func (db *DB) CmpAndSwap(bucket, key, oldValue, newValue []byte) ([]byte, bool, error) {
	return db.db.CmpAndSwap(bucket, key, oldValue, newValue)
}

// Update performs multiple commands on one read-write transaction.
func (db *DB) Update(tx *database.Tx) error {
	return db.db.Update(tx)
}

// CreateTable creates a table in the database.
func (db *DB) CreateTable(bucket []byte) error {
	return db.db.CreateTable(bucket)
}

// DeleteTable deletes a table in the database.
func (db *DB) DeleteTable(bucket []byte) error {
	return db.db.DeleteTable(bucket)
}
