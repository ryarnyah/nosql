package shared

import (
	"github.com/hashicorp/go-plugin"
	"github.com/smallstep/nosql/database"
)

// Handshake is a common handshake that is shared by plugin and host.
var Handshake = plugin.HandshakeConfig{
	// This isn't required when using VersionedPlugins
	ProtocolVersion:  1,
	MagicCookieKey:   "SMALLSTEP_NOSQL_PLUGIN",
	MagicCookieValue: "smallstep-nosql",
}

// PluginMap is the map of plugins we can dispense.
var PluginMap = map[string]plugin.Plugin{
	"nosql_grpc": &NOSQLGRPCPlugin{},
}

// DBPlugin is a subset of database.DB
type DBPlugin interface {
	// Get returns the value stored in the given table/bucket and key.
	Get(bucket, key []byte) (ret []byte, err error)
	// Set sets the given value in the given table/bucket and key.
	Set(bucket, key, value []byte) error
	// CmpAndSwap swaps the value at the given bucket and key if the current
	// value is equivalent to the oldValue input. Returns 'true' if the
	// swap was successful and 'false' otherwise.
	CmpAndSwap(bucket, key, oldValue, newValue []byte) ([]byte, bool, error)
	// Del deletes the data in the given table/bucket and key.
	Del(bucket, key []byte) error
	// List returns a list of all the entries in a given table/bucket.
	List(bucket []byte) ([]*database.Entry, error)
	// Update performs a transaction with multiple read-write commands.
	Update(tx *database.Tx) error
	// CreateTable creates a table or a bucket in the database.
	CreateTable(bucket []byte) error
	// DeleteTable deletes a table or a bucket in the database.
	DeleteTable(bucket []byte) error
}

// NOSQLGRPCPlugin go-plugin struct to make of plugin
type NOSQLGRPCPlugin struct {
	// GRPCPlugin must still implement the Plugin interface
	plugin.Plugin
	// Concrete implementation, written in Go. This is only used for plugins
	// that are written in Go.
	Impl DBPlugin
}
