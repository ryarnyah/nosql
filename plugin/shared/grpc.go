package shared

import (
	"context"

	"github.com/hashicorp/go-plugin"
	"github.com/smallstep/nosql/database"
	"github.com/smallstep/nosql/plugin/proto"
	"google.golang.org/grpc"
)

// GRPCClient is an implementation of KV that talks over RPC.
type GRPCClient struct {
	client proto.DBClient
}

// GRPCServer gRPC server that GRPCClient talks to.
type GRPCServer struct {
	// This is the real implementation
	Impl DBPlugin

	proto.UnimplementedDBServer
}

// GRPCServer register kafka plugin over GRPC
func (p *NOSQLGRPCPlugin) GRPCServer(broker *plugin.GRPCBroker, s *grpc.Server) error {
	proto.RegisterDBServer(s, &GRPCServer{Impl: p.Impl})
	return nil
}

// GRPCClient build GRPC client over go-plugin
func (p *NOSQLGRPCPlugin) GRPCClient(ctx context.Context, broker *plugin.GRPCBroker, c *grpc.ClientConn) (any, error) {
	return &GRPCClient{client: proto.NewDBClient(c)}, nil
}

func (p *GRPCClient) Get(bucket, key []byte) (ret []byte, err error) {
	response, err := p.client.Get(context.Background(), &proto.GetRequest{
		Bucket: bucket,
		Key:    key,
	})
	if err != nil {
		return nil, err
	}
	return response.Value, nil
}
func (p *GRPCClient) Set(bucket, key, value []byte) error {
	_, err := p.client.Set(context.Background(), &proto.SetRequest{
		Bucket: bucket,
		Key:    key,
		Value:  value,
	})
	if err != nil {
		return err
	}
	return nil
}
func (p *GRPCClient) CmpAndSwap(bucket, key, oldValue, newValue []byte) ([]byte, bool, error) {
	response, err := p.client.CmpAndSwap(context.Background(), &proto.CmpAndSwapRequest{
		Bucket:   bucket,
		Key:      key,
		OldValue: oldValue,
		NewValue: newValue,
	})
	if err != nil {
		return nil, false, err
	}
	return response.Value, response.Swapped, nil
}
func (p *GRPCClient) Del(bucket, key []byte) error {
	_, err := p.client.Del(context.Background(), &proto.DelRequest{
		Bucket: bucket,
		Key:    key,
	})
	if err != nil {
		return err
	}
	return nil
}
func (p *GRPCClient) List(bucket []byte) ([]*database.Entry, error) {
	response, err := p.client.List(context.Background(), &proto.ListRequest{
		Bucket: bucket,
	})
	if err != nil {
		return nil, err
	}
	entries := make([]*database.Entry, len(response.Entries))
	for i, entry := range response.Entries {
		entries[i] = &database.Entry{
			Bucket: entry.Bucket,
			Key:    entry.Key,
			Value:  entry.Value,
		}
	}
	return entries, nil
}
func (p *GRPCClient) Update(tx *database.Tx) error {
	protoTx := &proto.Tx{}
	protoTx.Operations = make([]*proto.TxEntry, len(tx.Operations))
	for i, operation := range tx.Operations {
		entry := &proto.TxEntry{
			Bucket:   operation.Bucket,
			Key:      operation.Key,
			Value:    operation.Value,
			CmpValue: operation.CmpValue,
			Result:   operation.Result,
			Swapped:  operation.Swapped,
		}
		switch operation.Cmd {
		case database.CreateTable:
			entry.Cmd = proto.TxCmd_CreateTable
		case database.DeleteTable:
			entry.Cmd = proto.TxCmd_DeleteTable
		case database.Get:
			entry.Cmd = proto.TxCmd_Get
		case database.Set:
			entry.Cmd = proto.TxCmd_Set
		case database.Delete:
			entry.Cmd = proto.TxCmd_Delete
		case database.CmpAndSwap:
			entry.Cmd = proto.TxCmd_CmpAndSwap
		case database.CmpOrRollback:
			entry.Cmd = proto.TxCmd_CmpOrRollback
		default:
			return database.ErrOpNotSupported
		}
		protoTx.Operations[i] = entry
	}
	_, err := p.client.Update(context.Background(), &proto.UpdateRequest{
		Tx: protoTx,
	})
	if err != nil {
		return err
	}
	return nil
}
func (p *GRPCClient) CreateTable(bucket []byte) error {
	_, err := p.client.CreateTable(context.Background(), &proto.CreateTableRequest{
		Bucket: bucket,
	})
	if err != nil {
		return err
	}
	return nil
}
func (p *GRPCClient) DeleteTable(bucket []byte) error {
	_, err := p.client.DeleteTable(context.Background(), &proto.DeleteTableRequest{
		Bucket: bucket,
	})
	if err != nil {
		return err
	}
	return nil
}

func (s *GRPCServer) Get(ctx context.Context, request *proto.GetRequest) (*proto.GetResponse, error) {
	value, err := s.Impl.Get(request.Bucket, request.Key)
	if err != nil {
		return nil, err
	}
	return &proto.GetResponse{
		Value: value,
	}, nil
}
func (s *GRPCServer) Set(ctx context.Context, request *proto.SetRequest) (*proto.Empty, error) {
	err := s.Impl.Set(request.Bucket, request.Key, request.Value)
	if err != nil {
		return nil, err
	}
	return &proto.Empty{}, nil
}
func (s *GRPCServer) CmpAndSwap(ctx context.Context, request *proto.CmpAndSwapRequest) (*proto.CmpAndSwapResponse, error) {
	value, swapped, err := s.Impl.CmpAndSwap(request.Bucket, request.Key, request.OldValue, request.NewValue)
	if err != nil {
		return nil, err
	}
	return &proto.CmpAndSwapResponse{
		Value:   value,
		Swapped: swapped,
	}, nil
}
func (s *GRPCServer) Del(ctx context.Context, request *proto.DelRequest) (*proto.Empty, error) {
	err := s.Impl.Del(request.Bucket, request.Key)
	if err != nil {
		return nil, err
	}
	return &proto.Empty{}, nil
}
func (s *GRPCServer) List(ctx context.Context, request *proto.ListRequest) (*proto.ListResponse, error) {
	entries, err := s.Impl.List(request.Bucket)
	if err != nil {
		return nil, err
	}
	protoEntries := make([]*proto.Entry, len(entries))
	for i, entry := range entries {
		protoEntries[i] = &proto.Entry{
			Bucket: entry.Bucket,
			Key:    entry.Key,
			Value:  entry.Value,
		}
	}
	return &proto.ListResponse{
		Entries: protoEntries,
	}, nil
}
func (s *GRPCServer) Update(ctx context.Context, request *proto.UpdateRequest) (*proto.Empty, error) {
	operations := make([]*database.TxEntry, len(request.Tx.Operations))
	for i, operation := range request.Tx.Operations {
		entry := &database.TxEntry{
			Bucket:   operation.Bucket,
			Key:      operation.Key,
			Value:    operation.Value,
			CmpValue: operation.CmpValue,
			Result:   operation.Result,
			Swapped:  operation.Swapped,
		}
		switch operation.Cmd {
		case proto.TxCmd_CreateTable:
			entry.Cmd = database.CreateTable
		case proto.TxCmd_DeleteTable:
			entry.Cmd = database.DeleteTable
		case proto.TxCmd_Get:
			entry.Cmd = database.Get
		case proto.TxCmd_Set:
			entry.Cmd = database.Set
		case proto.TxCmd_Delete:
			entry.Cmd = database.Delete
		case proto.TxCmd_CmpAndSwap:
			entry.Cmd = database.CmpAndSwap
		case proto.TxCmd_CmpOrRollback:
			entry.Cmd = database.CmpOrRollback
		default:
			return nil, database.ErrOpNotSupported
		}
		operations[i] = entry
	}
	err := s.Impl.Update(&database.Tx{
		Operations: operations,
	})
	if err != nil {
		return nil, err
	}
	return &proto.Empty{}, nil
}
func (s *GRPCServer) CreateTable(ctx context.Context, request *proto.CreateTableRequest) (*proto.Empty, error) {
	err := s.Impl.CreateTable(request.Bucket)
	if err != nil {
		return nil, err
	}
	return &proto.Empty{}, nil
}
func (s *GRPCServer) DeleteTable(ctx context.Context, request *proto.DeleteTableRequest) (*proto.Empty, error) {
	err := s.Impl.DeleteTable(request.Bucket)
	if err != nil {
		return nil, err
	}
	return &proto.Empty{}, nil
}
