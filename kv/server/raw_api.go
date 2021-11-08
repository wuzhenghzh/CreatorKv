package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}

	value, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return &kvrpcpb.RawGetResponse{
			Error: err.Error(),
		}, nil
	}

	resp := &kvrpcpb.RawGetResponse{
		Value:    value,
		NotFound: true,
	}

	if value == nil {
		resp.NotFound = true
	}

	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	putModify := storage.Modify{
		Data: storage.Put{
			Key:   req.Key,
			Value: req.Value,
			Cf:    req.Cf,
		},
	}

	err := server.storage.Write(nil, []storage.Modify{putModify})
	if err != nil {
		return &kvrpcpb.RawPutResponse{
			Error: err.Error(),
		}, nil
	}

	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	deleteModify := storage.Modify{
		Data: storage.Delete{
			Key: req.Key,
			Cf:  req.Cf,
		},
	}

	err := server.storage.Write(nil, []storage.Modify{deleteModify})
	if err != nil {
		return &kvrpcpb.RawDeleteResponse{
			Error: err.Error(),
		}, nil
	}

	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}

	it := reader.IterCF(req.Cf)
	var kvPairList []*kvrpcpb.KvPair

	for it.Seek(req.StartKey); it.Valid(); it.Next() {
		key := it.Item().Key()
		value, err := it.Item().Value()
		if err != nil {
			return &kvrpcpb.RawScanResponse{
				Error: err.Error(),
			}, nil
		}

		kvPairList = append(kvPairList, &kvrpcpb.KvPair{
			Key:   key,
			Value: value,
		})
		if uint32(len(kvPairList)) == req.Limit {
			break
		}
	}

	return &kvrpcpb.RawScanResponse{
		Kvs: kvPairList,
	}, nil
}
