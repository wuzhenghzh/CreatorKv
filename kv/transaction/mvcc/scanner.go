package mvcc

import (
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"reflect"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	startKey []byte
	txn      *MvccTxn
	iter     engine_util.DBIterator
	lastKey  []byte
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	iter := txn.Reader.IterCF(engine_util.CfDefault)
	iter.Seek(startKey)
	return &Scanner{
		startKey: startKey,
		txn:      txn,
		iter:     iter,
	}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	iter := scan.iter
	for {
		if !iter.Valid() {
			iter.Next()
			break
		}
		item := iter.Item()
		if item == nil {
			break
		}

		key := DecodeUserKey(item.Key())
		if scan.lastKey != nil && reflect.DeepEqual(key, scan.lastKey) {
			iter.Next()
			continue
		}
		if key != nil {
			value, err := scan.txn.GetValue(key)
			if err != nil {
				iter.Next()
				return nil, nil, err
			}
			if value != nil {
				iter.Next()
				scan.lastKey = key
				return key, value, nil
			}
		}
		iter.Next()
	}
	return nil, nil, nil
}
