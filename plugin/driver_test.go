package plugin

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/hashicorp/go-plugin"
	"github.com/smallstep/nosql/database"
	"github.com/smallstep/nosql/plugin/shared"
)

func TestNewPluginClient(t *testing.T) {
	cmd := fmt.Sprintf("%s -test.run=TestHelperProcess -- mock", os.Args[0])
	client, _, err := NewPluginClient(cmd, "nosql_grpc")
	if err != nil {
		t.Fatal(err)
	}
	client.Kill()
}

func TestCallDatabase(t *testing.T) {
	cmd := fmt.Sprintf("%s -test.run=TestHelperProcess -- mock", os.Args[0])
	client, db, err := NewPluginClient(cmd, "nosql_grpc")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Kill()

	if _, err := db.Get([]byte("inexistant_bucket"), []byte("test")); err == nil {
		t.Fail()
	}
	if err := db.CreateTable([]byte("test_table")); err != nil {
		t.Fatal(err)
	}
	if err := db.Set([]byte("test_table"), []byte("test_key"), []byte("test_value")); err != nil {
		t.Fatal(err)
	}
	if v, err := db.Get([]byte("test_table"), []byte("test_key")); err != nil || !bytes.Equal(v, []byte("test_value")) {
		t.Fail()
	}
	if v, swapped, err := db.CmpAndSwap([]byte("test_table"), []byte("test_key"), []byte("test"), []byte("")); err != nil || swapped || !bytes.Equal(v, []byte("test_value")) {
		t.Fail()
	}
	if v, swapped, err := db.CmpAndSwap([]byte("test_table"), []byte("test_key"), []byte("test_value"), []byte("")); err != nil || !swapped || !bytes.Equal(v, []byte("")) {
		t.Fail()
	}
	if err := db.Del([]byte("test_table"), []byte("test_key")); err != nil {
		t.Fatal(err)
	}
	if err := db.DeleteTable([]byte("test_table")); err != nil {
		t.Fatal(err)
	}
}

func TestHelperProcess(t *testing.T) {
	args := os.Args
	for len(args) > 0 {
		if args[0] == "--" {
			args = args[1:]
			break
		}

		args = args[1:]
	}

	if len(args) == 0 {
		return
	}

	cmd, _ := args[0], args[1:]
	if cmd == "mock" {
		plugin.Serve(&plugin.ServeConfig{
			HandshakeConfig: shared.Handshake,
			Plugins: map[string]plugin.Plugin{
				"nosql_grpc": &shared.NOSQLGRPCPlugin{Impl: newTestDB()},
			},
			GRPCServer: plugin.DefaultGRPCServer,
		})
	}
	// Shouldn't reach here but make sure we exit anyways
	os.Exit(0)
}

type testDB struct {
	buckets map[string]map[string][]byte
}

func newTestDB() *testDB {
	return &testDB{
		buckets: map[string]map[string][]byte{},
	}
}

func (db *testDB) Get(bucket, key []byte) (ret []byte, err error) {
	m, ok := db.buckets[string(bucket)]
	if !ok {
		return nil, errors.New("bucket not found")
	}
	v, ok := m[string(key)]
	if !ok {
		return nil, errors.New("key not found")
	}
	return v, nil
}
func (db *testDB) Set(bucket, key, value []byte) error {
	m, ok := db.buckets[string(bucket)]
	if !ok {
		return errors.New("bucket not found")
	}
	m[string(key)] = value
	return nil
}
func (db *testDB) CmpAndSwap(bucket, key, oldValue, newValue []byte) ([]byte, bool, error) {
	m, ok := db.buckets[string(bucket)]
	if !ok {
		return nil, false, errors.New("bucket not found")
	}
	v, ok := m[string(key)]
	if !ok {
		return nil, false, errors.New("key not found")
	}
	if !bytes.Equal(v, oldValue) {
		return v, false, nil
	}
	m[string(key)] = newValue
	return newValue, true, nil
}
func (db *testDB) Del(bucket, key []byte) error {
	m, ok := db.buckets[string(bucket)]
	if !ok {
		return errors.New("bucket not found")
	}
	delete(m, string(key))
	return nil
}
func (db *testDB) List(bucket []byte) ([]*database.Entry, error) {
	m, ok := db.buckets[string(bucket)]
	if !ok {
		return nil, errors.New("bucket not found")
	}
	i := 0
	res := make([]*database.Entry, len(m))
	for k, v := range m {
		res[i] = &database.Entry{
			Bucket: bucket,
			Key:    []byte(k),
			Value:  v,
		}
		i++
	}
	return res, nil
}
func (db *testDB) Update(tx *database.Tx) error {
	for _, operation := range tx.Operations {
		var err error
		switch operation.Cmd {
		case database.CreateTable:
			err = db.CreateTable(operation.Bucket)
		case database.DeleteTable:
			err = db.DeleteTable(operation.Bucket)
		case database.Get:
			operation.Result, err = db.Get(operation.Bucket, operation.Key)
		case database.Set:
			err = db.Set(operation.Bucket, operation.Key, operation.Value)
		case database.Delete:
			err = db.Del(operation.Bucket, operation.Key)
		case database.CmpAndSwap:
			operation.Result, operation.Swapped, err = db.CmpAndSwap(operation.Bucket, operation.Key, operation.CmpValue, operation.Value)
		default:
			return database.ErrOpNotSupported
		}
		if err != nil {
			return err
		}
	}
	return nil
}
func (db *testDB) CreateTable(bucket []byte) error {
	_, ok := db.buckets[string(bucket)]
	if ok {
		return nil
	}
	db.buckets[string(bucket)] = map[string][]byte{}
	return nil
}
func (db *testDB) DeleteTable(bucket []byte) error {
	_, ok := db.buckets[string(bucket)]
	if !ok {
		return errors.New("bucket not found")
	}
	delete(db.buckets, string(bucket))
	return nil
}
