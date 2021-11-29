# 一 实验概述

project4 的实验要求我们实现 tinykv 中的 percolator 模型

因此, 在描述解题思路前, 我们需要先了解一下 二阶段提交算法 和 percolator 算法





# 二 分布式事务模型 - percolator 算法

percolator 算法是由分布式事务中二阶段提交算法进化而来的, 其是对二阶段提交的改进和优化

## 二阶段提交算法

假设，小明和小红的数据分别被保存在数据库 D1 和 D2 上。

我们还是先讲正常的处理流程。

![image-20211129144435761](https://gitee.com/zisuu/mypicture/raw/master/image-20211129144435761.png)

第一阶段是准备阶段，事务管理器首先向所有参与者发送待执行的 SQL，并询问是否做好提交事务的准备（Prepare）；参与者记录日志、分别锁定了小明和小红的账户，并做出应答，协调者接收到反馈 Yes，准备阶段结束。

第二阶段是提交阶段，如果所有数据库的反馈都是 Yes，则事务管理器会发出提交（Commit）指令。这些数据库接受指令后，会进行本地操作，正式提交更新余额，给小明的账户扣减 2,000 元，给小红的账户增加 2,000 元，然后向协调者返回 Yes，事务结束。

那如果小明的账户出了问题，导致转账失败，处理过程会是怎样呢？

![image-20211129144451305](https://gitee.com/zisuu/mypicture/raw/master/image-20211129144451305.png)

第一阶段，事务管理器向所有数据库发送待执行的 SQL，并询问是否做好提交事务的准备。

由于小明之前在木瓜银行购买了基金定投产品，按照约定，每月银行会自动扣款购买基金，刚好这个自动扣款操作正在执行，先一步锁定了账户。数据库 D1 发现无法锁定小明的账户，只能向事务管理器返回失败。

第二阶段，因为事务管理器发现数据库 D1 不具备执行事务的条件，只能向所有数据库发出“回滚”（Rollback）指令。所有数据库接收到指令后撤销第一阶段的操作，释放资源，并向协调者返回 Yes，事务结束。小明和小红的账户余额均保持不变。

## 二阶段提交的缺陷

2PC 的优点是借助了数据库的提交和回滚操作，不侵入业务逻辑。但是，它也存在一些明显的问题：

**同步阻塞**

执行过程中，数据库要锁定对应的数据行。如果其他事务刚好也要操作这些数据行，那它们就只能等待。其实同步阻塞只是设计方式，真正的问题在于这种设计会导致分布式事务出现高延迟和性能的显著下降。

**单点故障**

事务管理器非常重要，一旦发生故障，数据库会一直阻塞下去。尤其是在第二阶段发生故障的话，所有数据库还都处于锁定事务资源的状态中，从而无法继续完成事务操作。

**数据不一致**

在第二阶段，当事务管理器向参与者发送 Commit 请求之后，发生了局部网络异常，导致只有部分数据库接收到请求，但是其他数据库未接到请求所以无法提交事务，整个系统就会出现数据不一致性的现象。比如，小明的余额已经能够扣减，但是小红的余额没有增加，这样就不符合原子性的要求了。

所以，网上很多文章会建议你避免使用 2PC，替换为 TCC 或者其他事务框架。

事实上，多数分布式数据库都是在 2PC 协议基础上改进，来保证分布式事务的原子性。

我们本次实验要实现的 percolator 算法, 就是二阶段提交算法的改进和优化。

## percolator 算法

### 基础概念

Percolator 来自 Google 的论文“Large-scale Incremental Processing Using Distributed Transactions and Notifications”，因为它是基于分布式存储系统 BigTable 建立的模型，所以可以和 NewSQL 无缝链接。

使用 Percolator 模型的前提是事务的参与者，即数据库，要支持多版本并发控制（MVCC）。

在 percolator 模型中, 会涉及到三种类型的数据, 存储在三个不同的 column_family 中:

| Column_family |  key   |  value  |
| :-----------: | :----: | :-----: |
|   Write_cf    | Ming:6 | data @5 |
|    Lock_cf    |  Ming  | Primary |
|  Default_cf   | Ming:5 |  4900   |

其中:

- Default_cf 中存储的是数据行, key 的组成为 {key}_{startTs}, value 为具体的值

​		startTs 指事务的开始版本

- Lock_cf 中存储的是锁的记录, key 的组成为 {key}, value 为指向 primary 主锁的记录 LockInfo

​		LockInfo 包括事务开始版本 startTs, 事务的主键  primaryKey, 锁的存活时间 ttl 等.......

- Write_cf 中存储的是提交/回滚的记录, key 的组成为{key}_{commitTs}, value 为 writeInfo

  writeInfo 包括该事务的开始版本 startTs, 提交类型 writeKind(PUT /DELETE/ Roll_bakc)

需要注意的是, default_cf 中的一条记录, 只有其含有 write_cf 指向该记录, 也即只有该事务提交了, 该记录对于别的事务才是有可能可见的

**下面举个例子来说明 percolator:**

在转账事务开始前，小明和小红的账户分别存储在分片 P1 和 P2 上, 如下图所示:

现在的需求是小明要转账2000给小红

**分片p1**

| Column_family |  key   | value  |
| :-----------: | :----: | :----: |
|   Write_cf    | Ming:6 | 5, put |
|  Default_cf   | Ming:5 |  4900  |

**分片p2**

| Column_family |  key   | value  |
| :-----------: | :----: | :----: |
|   Write_cf    | Hong:6 | 5, put |
|  Default_cf   | Hong:5 |  300   |

### 准备阶段

第一，准备阶段，事务管理器向分片发送 Prepare 请求，包含了具体的数据操作要求。

分片接到请求后要做两件事，写数据日志和加锁, 加上锁后的数据还是私有版本的, 对别的事务不可见

**分片p1**

| Column_family |  key   |         value         |
| :-----------: | :----: | :-------------------: |
|    Lock_cf    |  Ming  | primary, 7, ttl...... |
|  Default_cf   | Ming:7 |         2900          |
|   Write_cf    | Ming:6 |        5, put         |
|  Default_cf   | Ming:5 |         4900          |

**分片p2**

| Column_family |  key   |         value         |
| :-----------: | :----: | :-------------------: |
|    Lock_cf    |  Hong  | primary, 7, ttl...... |
|  Default_cf   | Hong:7 |         2300          |
|   Write_cf    | Hong:6 |        5, put         |
|  Default_cf   | Hong:5 |          300          |

以上为第一阶段后的分片记录, p1和 p2 都增加了两行记录

- 一行是 lock_cf 中的锁记录, Value 包含 Primary, startTs, ttl 等
- 一行是 default_cf 中的记录, 包含转账后的数据

这里的 primary 是指这个事务的一批写操作中, 其中一个会被选为 primary key, 会在提交阶段中用到,

我们假设 Ming 的记录被选为 primary

### 提交阶段

第二，提交阶段，事务管理器只需要和拥有主锁的分片通讯，发送 Commit 指令，且不用附带其他信息

在这个例子中, 就是往分片 p1 提交

也即, 分片 p1 中会新增一条 write 记录, 同时 lock 的记录会被删除

但是, 分片 p2 中的lock 记录却还没被清楚, 也没有代表事务提交的 write 记录出现

**分片p1**

| Column_family |  key   | value  |
| :-----------: | :----: | :----: |
|   Write_cf    | Ming:8 | 7, put |
|    Lock_cf    |        |        |
|  Default_cf   | Ming:7 |  2900  |
|   Write_cf    | Ming:6 | 5, put |
|  Default_cf   | Ming:5 |  4900  |

**分片p2**

| Column_family |  key   |         value         |
| :-----------: | :----: | :-------------------: |
|    Lock_cf    |  Hong  | primary, 7, ttl...... |
|  Default_cf   | Hong:7 |         2300          |
|   Write_cf    | Hong:6 |        5, put         |
|  Default_cf   | Hong:5 |          300          |

你或许要问，为什么在提交阶段不用更新小红的记录？

Percolator 最有趣的设计就是这里，只需要和主锁所在的分片提交, 无需和其他分片通信, 可以减少二阶段提交带来的通信开销, 同时往一个分片提交的行为本身就是原子的.

那其他分片的锁记录怎么办? 因为分片 P2 的最后一条记录，保存了 lockInfo, lockInfo 中包含了事务的启示版本 startTs, 在这里就是7.

当事务结束后, 会有一个后台线程发送 KvCheckTxnStatus (PrimaryKey) 请求给服务器, 检测该事务是否已经提交了, 如果已经提交了, 该线程还会发送一个 KvResolveLock (startVersion, commitVersion) 请求给服务器, 服务器会先查找包含该 startVersion 的锁记录, 然后将这些锁记录清除, 同时新增 write , 代表这些记录都已经提交了

通过这种方式, percolator 就达到了优化二阶段提交的目的





# 三 代码

## lab4A - mvcc - transaction

lab4A 引导我们实现 mvcc / transaction / MvccTxn 里的几个方法: putLock, putValue, putWrite , getValue 等....

这几个方法会在 lab4b , 4c 中被 transaction_api 使用到

通过上文的分析, 以及实验知道书中的记录, 我们知道总共需要存储三种类型的数据:

- Default_cf 中存储的是数据行, key 的组成为 {key}_{startTs}, value 为具体的值

​		startTs 指事务的开始版本

- Lock_cf 中存储的是锁的记录, key 的组成为 {key}, value 为指向 primary 主锁的记录 LockInfo

​		LockInfo 包括事务开始版本 startTs, 事务的主键  primaryKey, 锁的存活时间 ttl 等.......

- Write_cf 中存储的是提交/回滚的记录, key 的组成为{key}_{commitTs}, value 为 writeInfo

  writeInfo 包括该事务的开始版本 startTs, 提交类型 writeKind(PUT /DELETE/ Roll_bakc)

### putLock / putWrite ..

put xxx 比较简单, 就是往 mvccTxn.writes 里 append 一个 modify 就可以了:

```
// PutWrite records a write at key and ts.
// 这里的 ts 指的是 commitTs
func (txn *MvccTxn) PutWrite(key []byte, ts uint64, write *Write) {
   // Your Code Here (4A).
   putModify := storage.Modify{
      Data: storage.Put{
         Cf:    engine_util.CfWrite,
         Key:   EncodeKey(key, ts),
         Value: write.ToBytes(),
      },
   }
   txn.writes = append(txn.writes, putModify)
}

// PutLock adds a key/lock to this transaction.
func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	// Your Code Here (4A).
	putModify := storage.Modify{
		Data: storage.Put{
			Cf:    engine_util.CfLock,
			Key:   key,
			Value: lock.ToBytes(),
		},
	}
	txn.writes = append(txn.writes, putModify)
}

```

### GetValue

GetValue 的实现比较有挑战, 因为基于 mvcc 多版本控制条件下, 我们读取的数据必须是能被当前这个事务看到的,

而判断记录是否可见的依据是其 commitTs (提交版本时间戳)

也即数据的 commitTs 必须 <= 事务的 startTs

思路:

- 因为数据的组织是根据 key 升序排序, 时间戳降序排序, 也即同一个 key 的不同版本会降序排序
- 因此, 在 write_cf 中, 通过 iterator.seek, 定位 (key, txn.StartTs), 就会自动找到第一个 commitTs 版本小于等于 txn.StartTs 的 write 记录
- 我们根据这个 write 记录中的 startTS, 就可以定位该行数据了

```
func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) {
   // Your Code Here (4A).
   iter := txn.Reader.IterCF(engine_util.CfWrite)
   defer iter.Close()
   // 1.Get write --> get startTs
   for iter.Seek(EncodeKey(key, txn.StartTS)); iter.Valid(); iter.Next() {
      item := iter.Item()
      userKey := DecodeUserKey(item.Key())
      if !reflect.DeepEqual(userKey, key) {
         continue
      }

      writeValue, err := item.Value()
      if err != nil {
         return nil, err
      }
      write, err := ParseWrite(writeValue)
      if err != nil || write == nil {
         return nil, err
      }
      return txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
   }
   return nil, nil
}
```





### CurrentWrite / MostRecentWrite

currentWrite 要求查找的是当前事务关于该 key 的 write 记录, 需要比对 writeInfo 中的 startTs 是否和 txn.startTs 相同:

```
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
   // Your Code Here (4A).
   iter := txn.Reader.IterCF(engine_util.CfWrite)
   defer iter.Close()
   for iter.Seek(key); iter.Valid(); iter.Next() {
      item := iter.Item()
      writeData, err := item.Value()
      if err != nil {
         continue
      }

      userKey := DecodeUserKey(item.Key())
      if !reflect.DeepEqual(key, userKey) {
         continue
      }
      write, err := ParseWrite(writeData)
      if err != nil {
         continue
      }
      
      // 如果不同, 就查找下一条记录
      if write.StartTS != txn.StartTS {
         continue
      }

      commitTimeStamp := decodeTimestamp(item.Key())
      return write, commitTimeStamp, nil
   }
   return nil, 0, nil
}
```



MostRecentWrite 要求查找的是关于该 key 的最近一条 write 记录, 无需关系 startTs 是否匹配:

```
func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
   // Your Code Here (4A).
   iter := txn.Reader.IterCF(engine_util.CfWrite)
   defer iter.Close()
   for iter.Seek(key); iter.Valid(); iter.Next() {
      item := iter.Item()
      writeData, err := item.Value()
      if err != nil {
         continue
      }
      userKey := DecodeUserKey(item.Key())
      if !reflect.DeepEqual(key, userKey) {
         continue
      }

      write, err := ParseWrite(writeData)
      if err != nil {
         continue
      }
      commitTimeStamp := decodeTimestamp(item.Key())
      return write, commitTimeStamp, nil
   }
   return nil, 0, nil
}
```

## lab4B - percolator

在这一节中, 我们需要实现 server.go 中的 kvGet, kvPrewrite, kvCommit 这三个函数

### kvGet

kvGet 要求返回对当前事务可见的数据, 也即要用到 lab4A 中写的 GetValue()

当然, 在读取前, 还要检测该 key 是否被 lock 了, 也即被其他事务锁住了, 是的话要返回一个 error

```
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
```

如果没有被锁住, 则直接调用 txn.getValue() 读取即可:

```
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
```

### kvPreWrite

在第二小节的 percolator 算法介绍中, 我们知道, perWrite 阶段做两件事情:

- 检测事务冲突
- 写如锁记录和数据记录

检测事务冲突, 包括:

- 是否有更高版本的事务写入了该 key
- 该 key 是否被锁住了

```
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
```

写入数据:

```
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
```

需要注意的是, 最后要把 txn 中的 modify 写入到 storage 中:

```
err = server.storage.Write(req.Context, txn.Writes())
```

**整体代码:**

```
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
	  // Put lock and value
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

   // Write to storage
   err = server.storage.Write(req.Context, txn.Writes())
   if err != nil {
      return nil, err
   }
   if len(keyErrors) > 0 {
      resp.Errors = keyErrors
   }
   return resp, nil
}
```



### kvCommit

commit 阶段就是提交事务, 在 tinykv 中, 提交事务的方式就是:

- 删除 lock 记录
- 写入 write 记录, 代表提交

```
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
```



## lab4c - commit / rollback

这个小节, 我们继续完成 percolator 模型中剩下的函数:

- KvScan
- KvCheckTxnStatus: 检测事务的状态, 是否已经提交/回滚, 检测锁是否过期了
- KvBatchRollback: 回滚事务
- KvResolveLock: 提交 或者 回滚事务

### kvScan

kvScan 读取到的数据有两个要求:

- 基于 mvcc 读取, 也即只能读当前事务可见的数据
- 不能重复读取

kvScan 要求我们先实现 scanner 这个扫描器, 其中包含了 iter, txn , startKey 等

```
type Scanner struct {
   // Your Data Here (4C).
   startKey []byte
   txn      *MvccTxn
   iter     engine_util.DBIterator
   lastKey  []byte
}
```

newScanner 中可以先 seek(startKey):

```
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
```

关键在于 next() 函数, 其中包含了去重的逻辑:

```
func (scan *Scanner) Next() ([]byte, []byte, error) {
   // Your Code Here (4C).
   iter := scan.iter
   for {
    
      item := iter.Item()
      .............
      key := DecodeUserKey(item.Key())
      // 数据重复了
      if scan.lastKey != nil && reflect.DeepEqual(key, scan.lastKey) {
         iter.Next()
         continue
      }
      if key != nil {
      	 // 通过 txn, 读取当前可见的数据
         value, err := scan.txn.GetValue(key)
         if err != nil {
            iter.Next()
            return nil, nil, err
         }
         if value != nil {
            iter.Next()
            // 记录 lastKey
            scan.lastKey = key
            return key, value, nil
         }
      }
      iter.Next()
   }
   return nil, nil, nil
}
```

有了 scanner, kvScan 就简单了:

```
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
```



### KvCheckTxnStatus

KvCheckTxnStatus 检测:

**事务的状态(是否已经提交)**

```
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
```

**如果没有提交, 则检测锁是否过期了**

```
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

// Check lock ttl, if timeout, then delete lock info and rollback
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
```

### KvBatchRollback

KvBatchRollback 要求批量回滚事务, 也即删除每个事务的锁记录, 值记录以及增加一条 write (rollback 类型)的记录

回滚前, 需要先检测该事务已经被提交/回滚了:

```
w, _, err := txn.CurrentWrite(key)
if err != nil {
   continue
}
if w != nil {
   if w.Kind != mvcc.WriteKindRollback {
      resp.Error = &kvrpcpb.KeyError{
         Abort: "The key is committed, Just abort Key",
      }
      return resp, nil
   }
   continue
}
```

没有提交, 则回滚事务:

```
// Check lock and remove keys and values
lock, err := txn.GetLock(key)
if err != nil {
   continue
}
if lock == nil || lock.Ts != startTs {
   txn.PutWrite(key, startTs, &mvcc.Write{
      StartTS: startTs,
      Kind:    mvcc.WriteKindRollback,
   })
   continue
}
txn.DeleteLock(key)
txn.DeleteValue(key)
// Put a rollback indicator as a write.
txn.PutWrite(key, startTs, &mvcc.Write{
   StartTS: startTs,
   Kind:    mvcc.WriteKindRollback,
})
```



### KvResolveLock

KvResolveLock 要求, 如果 commitTs == 0, 则回滚事务, 否则提交事务:

```
// Roll back
if req.CommitVersion == 0 {
   for _, key := range keys {
      txn.DeleteLock(key)
      txn.DeleteValue(key)
      // Put a rollback indicator as a write.
      txn.PutWrite(key, startTs, &mvcc.Write{
         StartTS: startTs,
         Kind:    mvcc.WriteKindRollback,
      })
   }
} else {
   // commit
   for _, key := range keys {
      txn.DeleteLock(key)
      txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
         Kind:    mvcc.WriteKindPut,
         StartTS: startTs,
      })
   }
}
```









