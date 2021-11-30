# 任务概述

project1 需要我们基于 'badger' 这个单机 KV 存储引擎实现 standalone_storage

主要分为两个步骤：

- 在 standalone_storage 封装 badger, 实现 Storage 接口, storage 接口如下:

  ```
  
  type Storage interface {
     Start() error
     Stop() error
     Write(ctx *kvrpcpb.Context, batch []Modify) error
     Reader(ctx *kvrpcpb.Context) (StorageReader, error)
  }
  
  type StorageReader interface {
     // When the key doesn't exist, return nil for the value
     GetCF(cf string, key []byte) ([]byte, error)
     IterCF(cf string) engine_util.DBIterator
     Close()
  }
  ```

- 在 raw_api 中基于 standalone_storage 实现接口, 包括 raw_get, raw_put 等

需要注意的是， 指导书提到了 ‘Column famliy’ ：相同的 key 在不同的 cf 下是不同的， 并且在 project4 中会用到这个 cf

然而 badger 本身并没有 cf 的概念可以使用, 因此, 我们存储时， 需要用一个前缀来存储 key :

也即 &{cf}_${key},  这样, 相同 cf 的 key 就会被存储在一起了

另外， 关于 badger 的用法， [点这里](./lab1-badger.md)

# 实验步骤

## standalone_storage

对 badger 的封装无非分为几个点:

- start : 打开一个 badger db
- write: 往 badgerDB 写入数据
- reader: 封装实现一个基于 badger  的 StorageReader

### open engine -- start

start() 就是通过 engine_util 创建一个 badgerdb 即可

```
type StandAloneStorage struct {
	// Your Data Here (1).
	db     *badger.DB
	config *config.Config
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	conf := s.config
	path := conf.DBPath
	s.db = engine_util.CreateDB(path, conf.Raft)
	return nil
}
```

### write

该方法接收一个 'storage.Modify' 数组, 每个 Modify 是 Delete 或者 Put 操作:

实验指导书告诉我们可以使用 writeBatch 来暂存这些 更改操作, 并且统一调用一次 writeBatch.WriteToDB 将更改操作写入  badgerdb

```
func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	.........
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
```

### reader

reader 方法需要返回一个 'storage.StorageReader', 因此, 我们需要自己实现一个 storageReader, 这里我给之取名为 BadgerReader

需要注意的是, badger 的事务可以提供一个一致性快照, 也即在某一个时刻创建的事务, 只能读取到这之前的数据, 而无法看到后续的修改.

这正是 project4 中要求的 mvcc 多版本并发控制的基础.

因此, 我们需要在 BadgerReader 传入一个 badger.Txn, 保证当前 reader 读取的数据的正确性

```
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return BadgerReader{
		txn: s.db.NewTransaction(false),
	}, nil
}
```

```
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

```

## raw_api

实际上, raw_api 是 server.go 中的方法, 但是为了和 transaction_api 区分开来, 专门将这几个方法放在了 raw_api 中

raw_api 需要实现 raw_get, raw_put, raw_delete, raw_scan 等方法

我们已经在 上面实现了 基于 badger 的 standalone_storage, 那么 raw_api 只需要调用 standalone_storage 的 api 就可以了..

比如, raw_get , raw_put, raw_delete 的实现如下:

```
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
```

关于 raw_scan, 需要用到 reader.IterCF 方法来获取一个 DBIterator, 并通过 iter.seek() 在定义第一个数据:

```
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}

	it := reader.IterCF(req.Cf)
	var kvPairList []*kvrpcpb.KvPair

	// seek, 提供定位的功能
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
		
		// 到达 limit 限制后, break
		if uint32(len(kvPairList)) == req.Limit {
			break
		}
	}

	return &kvrpcpb.RawScanResponse{
		Kvs: kvPairList,
	}, nil
}
```