package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	storage "github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pkg/errors"
	"path/filepath"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
// StandAloneStorage 就是对 'badger' 这个单机键值引擎的封装
type StandAloneStorage struct {
	// Your Data Here (1).
	engine *engine_util.Engines
	config *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{
		config: conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	conf := s.config
	path := conf.DBPath
	kvPath := filepath.Join(path, "kv")
	raftPath := filepath.Join(path, "raft")

	kvDB := engine_util.CreateDB(kvPath, false)
	raftDB := engine_util.CreateDB(raftPath, true)
	s.engine = engine_util.NewEngines(kvDB, raftDB, kvPath, raftPath)
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.engine.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	if s.engine == nil {
		return nil, errors.New("The store engine is not init")
	}

	kvTxn := s.engine.Kv.NewTransaction(false)
	raftTxn := s.engine.Raft.NewTransaction(false)
	return BadgerReader{
		kvTxn:   kvTxn,
		raftTxn: raftTxn,
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	if s.engine == nil {
		return errors.New("The store engine is not init")
	}
	for _, modify := range batch {
		cfKey := engine_util.KeyWithCF(modify.Cf(), modify.Key())
		db := s.engine.Kv
		err := db.Update(func(txn *badger.Txn) error {
			switch modify.Data.(type) {
			case storage.Delete:
				return txn.Delete(cfKey)
			case storage.Put:
				return txn.Set(cfKey, modify.Value())
			}
			return nil
		})
		println("key is : % s, values is : %s", modify.Key(), modify.Value())
		if err != nil {
			return err
		}
	}
	return nil
}

type BadgerReader struct {
	kvTxn   *badger.Txn
	raftTxn *badger.Txn
}

func (b BadgerReader) GetCF(cf string, key []byte) ([]byte, error) {
	txn := b.kvTxn
	value, err := engine_util.GetCFFromTxn(txn, cf, key)
	if err != nil && err == badger.ErrKeyNotFound {
		// That means the key is not existed
		return nil, nil
	}
	return value, nil
}

func (b BadgerReader) IterCF(cf string) engine_util.DBIterator {
	txn := b.kvTxn
	return engine_util.NewCFIterator(cf, txn)
}

func (b BadgerReader) Close() {
	if b.kvTxn != nil {
		b.kvTxn.Discard()
	}
	if b.raftTxn != nil {
		b.raftTxn.Discard()
	}
}
