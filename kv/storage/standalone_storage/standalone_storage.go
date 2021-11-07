package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	storage "github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pkg/errors"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
// StandAloneStorage 就是对 'badger' 这个单机键值引擎的封装
type StandAloneStorage struct {
	// Your Data Here (1).
	db   *badger.DB
	path string
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{
		path: conf.DBPath,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	opt := badger.DefaultOptions
	opt.Dir = s.path
	opt.ValueDir = s.path
	db, err := badger.Open(opt)
	s.db = db
	return err
}

func (s *StandAloneStorage) Stop() error {
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	if s.db == nil {
		return nil, errors.New("The badger db is not init")
	}
	txn := s.db.NewTransaction(false)
	return BadgerReader{
		txn: txn,
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	if s.db == nil {
		return errors.New("The badger db is not init")
	}
	for _, modify := range batch {
		cfKey := buildKey(modify.Cf(), string(modify.Key()))
		err := s.db.Update(func(txn *badger.Txn) error {
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
	txn *badger.Txn
}

func (b BadgerReader) GetCF(cf string, key []byte) ([]byte, error) {
	cfKey := buildKey(cf, string(key))
	item, err := b.txn.Get(cfKey)
	if err != nil {
		// That means the key is not existed
		return nil, nil
	}

	value, err := item.ValueCopy(nil)
	if err != nil {
		return nil, err
	}

	return value, nil
}

func (b BadgerReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, b.txn)
}

func (b BadgerReader) Close() {
	if b.txn != nil {
		b.txn.Discard()
	}
}

func buildKey(cf string, key string) []byte {
	cfKey := cf + "_" + key
	return []byte(cfKey)
}
