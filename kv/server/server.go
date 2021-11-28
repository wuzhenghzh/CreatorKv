package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.GetResponse{}
	reader, txn, err := server.getReader(req.Version, req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	// Check lock conflict
	key := req.Key
	lock, err := txn.GetLock(key)
	if err != nil {
		return nil, err
	}
	if lock != nil && lock.Ts <= req.Version {
		return &kvrpcpb.GetResponse{Error: &kvrpcpb.KeyError{
			Locked: &kvrpcpb.LockInfo{
				PrimaryLock: lock.Primary,
				LockVersion: lock.Ts,
				Key:         key,
				LockTtl:     lock.Ttl,
			},
		}}, nil
	}

	// Get value
	value, err := txn.GetValue(key)
	if err != nil {
		return resp, err
	}
	resp.Value = value
	if value == nil {
		resp.NotFound = true
	}
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.PrewriteResponse{}

	mutations := req.Mutations
	reader, txn, err := server.getReader(req.StartVersion, req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	keyErrors := make([]*kvrpcpb.KeyError, 0)
	for _, mutation := range mutations {
		key := mutation.Key
		// Check that another transaction has not locked or written to the same key.
		conflictErr := server.checkWriteConflict(txn, key, req.StartVersion)
		if conflictErr != nil {
			keyErrors = append(keyErrors, conflictErr)
			continue
		}

		op := mutation.Op
		txn.PutLock(key, &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind:    mvcc.WriteKind(op + 1),
		})
		switch op {
		case kvrpcpb.Op_Put:
			txn.PutValue(key, mutation.Value)
		case kvrpcpb.Op_Del:
			txn.DeleteValue(key)
		}
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return nil, err
	}
	if len(keyErrors) > 0 {
		resp.Errors = keyErrors
	}
	return resp, nil
}

func (Server *Server) checkWriteConflict(txn *mvcc.MvccTxn, key []byte, startVersion uint64) *kvrpcpb.KeyError {
	write, commitTs, _ := txn.MostRecentWrite(key)
	if write != nil && startVersion < commitTs {
		err := &kvrpcpb.KeyError{
			Conflict: &kvrpcpb.WriteConflict{
				StartTs:    startVersion,
				Key:        key,
				ConflictTs: commitTs,
			},
		}
		return err
	}
	if lock, _ := txn.GetLock(key); lock != nil && lock.Ts != startVersion {
		err := &kvrpcpb.KeyError{
			Conflict: &kvrpcpb.WriteConflict{
				StartTs:    startVersion,
				Key:        key,
				ConflictTs: lock.Ts,
			},
		}
		return err
	}
	return nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	resp := &kvrpcpb.CommitResponse{}
	reader, txn, err := server.getReader(req.StartVersion, req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	keys := req.Keys
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	for _, key := range keys {
		lock, err := txn.GetLock(key)
		if err != nil {
			return nil, err
		}
		if lock == nil {
			return resp, nil
		}
		if lock.Ts != req.StartVersion {
			return &kvrpcpb.CommitResponse{Error: &kvrpcpb.KeyError{
				Retryable: "true",
			}}, nil
		}
		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    lock.Kind,
		})
		txn.DeleteLock(key)
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	reader, txn, err := server.getReader(req.Version, req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	scanner := mvcc.NewScanner(req.StartKey, txn)
	defer scanner.Close()

	var pairs []*kvrpcpb.KvPair
	for i := 0; i < int(req.Limit); i++ {
		key, value, err := scanner.Next()
		if err != nil {
			continue
		}
		if key == nil || value == nil {
			break
		}
		pairs = append(pairs, &kvrpcpb.KvPair{
			Key:   key,
			Value: value,
		})
	}
	return &kvrpcpb.ScanResponse{
		Pairs: pairs,
	}, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	resp := &kvrpcpb.CheckTxnStatusResponse{}
	startTs := req.LockTs
	key := req.PrimaryKey

	reader, txn, err := server.getReader(startTs, req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	// Check whether this txn has benn committed or rollBack
	w, commitTs, err := txn.CurrentWrite(key)
	if err != nil {
		return nil, err
	}
	if w != nil {
		if w.Kind != mvcc.WriteKindRollback {
			resp.CommitVersion = commitTs
		}
		return resp, nil
	}

	// Check lock
	lock, err := txn.GetLock(req.PrimaryKey)
	if lock == nil {
		// If lock is nil, it means we should roll back
		txn.PutWrite(key, startTs, &mvcc.Write{
			Kind:    mvcc.WriteKindRollback,
			StartTS: startTs,
		})
		_ = server.storage.Write(req.Context, txn.Writes())
		resp.Action = kvrpcpb.Action_LockNotExistRollback
		return resp, nil
	}

	// Check lock ttl
	if mvcc.PhysicalTime(lock.Ts)+lock.Ttl <= mvcc.PhysicalTime(req.CurrentTs) {
		txn.DeleteLock(key)
		txn.DeleteValue(key)
		txn.PutWrite(key, startTs, &mvcc.Write{
			Kind:    mvcc.WriteKindRollback,
			StartTS: startTs,
		})
		_ = server.storage.Write(req.Context, txn.Writes())
		resp.Action = kvrpcpb.Action_TTLExpireRollback
		return resp, nil
	}
	resp.Action = kvrpcpb.Action_NoAction
	return resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) getReader(startTs uint64, ctx *kvrpcpb.Context) (storage.StorageReader, *mvcc.MvccTxn, error) {
	reader, err := server.storage.Reader(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, startTs)
	return reader, txn, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
