package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pkg/errors"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
// StandAloneStorage is a wrapper of badger, it creates two badger instance, see lab-2B
type StandAloneStorage struct {
	// Your Data Here (1).
	db     *badger.DB
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
	s.db = engine_util.CreateDB(path, conf.Raft)
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	if s.db == nil {
		return nil, errors.New("The store db is not init")
	}

	return BadgerReader{
		txn: s.db.NewTransaction(false),
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	if s.db == nil {
		return errors.New("The store db is not init")
	}
	writeBatch := new(engine_util.WriteBatch)
	for _, x := range batch {
		switch x.Data.(type) {
		case storage.Put:
			writeBatch.SetCF(x.Cf(), x.Key(), x.Value())
		case storage.Delete:
			writeBatch.DeleteCF(x.Cf(), x.Key())
		}
	}
	return writeBatch.WriteToDB(s.db)
}

type BadgerReader struct {
	txn *badger.Txn
}

func (b BadgerReader) GetCF(cf string, key []byte) ([]byte, error) {
	txn := b.txn
	value, err := engine_util.GetCFFromTxn(txn, cf, key)
	if err != nil && err == badger.ErrKeyNotFound {
		// That means the key is not existed
		return nil, nil
	}
	return value, nil
}

func (b BadgerReader) IterCF(cf string) engine_util.DBIterator {
	txn := b.txn
	return engine_util.NewCFIterator(cf, txn)
}

func (b BadgerReader) Close() {
	if b.txn != nil {
		b.txn.Discard()
	}
}
