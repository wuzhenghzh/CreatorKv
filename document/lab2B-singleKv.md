# 一 实验概述

project2B 要求我们实现一个基于 raft 的 分布式 kv 存储系统

然而, 实际上, 代码要完成的工作量比较少, 但是代码阅读量大, 需要对 tinykv 的整体架构有一个清晰的理解, 才能完成

project2B 乃至后续的所有实验

在第二节, 我会详细的描述 tinykv multi-raft 的架构和本次实验涉及到的重要的类

第三节, 完成本次实验的代码



# 二 Tinykv multi-raft 整体架构

## Multi-raft

所谓的 multi-raft, 其核心是把数据进行分区(如 hash 分区 和 range 分区), 我们称一个分区为一个 region , 每个 region 有至少三个的副本, 并由一个 raft-group 负责

如图所示, 共有三个 region: region1, region2, region3

region1 的三个分布分布在 nodeA, nodeB, nodeD, 上, 这三个副本由一个 raft group 所管理

在后续的 project3 中, 我们会接触到更多的 multi-raft 的核心内容, 包括成员变更, leader transfer 和 pd region - scheduler

在 project2 中, 我们只关注在一个 region 上, 基于 raft 实现 分布式 kv 存储系统.

![image-20211201144741591](https://gitee.com/zisuu/mypicture/raw/master/image-20211201144741591.png)

## 实现 multi-raft 的核心点

(1) 数据何如分片:

- 基于 range 分片, 好处是对于范围请求比较友好, 也有利于底层的 rocksdb 这种基于 lsm tree 的存储

(2) 分片中的数据越来越大，需要分裂产生更多的分片，组成更多 Raft-Group:

- 用一个定时器,  split_checker, 定时的检测每个 region 的大小是否超过了限制, 如果超过了, 需要和 pd 上报 split 的请求, 让 pd 生成全局唯一的 regionid, 并进行 region split 的流程

(3) 分片的调度，让负载在系统中更平均（分片副本的迁移，补全，Leader 切换等等）。

- 需要一个 pd , 全局元数据兼顾调度中心
- region 和 store 定时的上报 regionHeartbeat 和 storeHeartbeat
- regionHeartbeat 包括 region 的基本信息, 比如 大小, 成员等等
- storeHeartbeat 包括 store 中 region 的数量, 总存储的大小
- pd 中需要具备 region_balancer 和 leader_balancer
  - region_balancer 负责平衡每个 store 上region 的数量, 保证存储尽量的均衡
  - leader_balancer 负责平衡每个 store 上 leader 的数量, 保证读写负载尽量均衡
  - 具体的做法很简单: 因为  pd 蕴含了全局的元数据, 比如 leader_balancer , 那就对每个 store 按照 leader nums 进行从大到小的排序, 然后在 leader 数量多的 store 上挑选处一个 leader, 将其 leader transfer 给其某个 follower(当然, 这个 follower 必须在别的 store )上
  - 而对于 region_balancer , 也可以依靠成员变更来实现
- 总的来说, 调度的核心依赖于 raft 的 leader transfer 和 conf node change

## tinykv的 multi raft 架构

认识了 multi-raft 后, 那么 tinykv 中是怎么体现 multi-raft 的呢? 如下图所示:

<img src="https://gitee.com/zisuu/mypicture/raw/master/image-20211202194641910.png" alt="image-20211202194641910" style="zoom:67%;" />



- Raft Store: 主要是创建和启动了一系列的 worker, 包括:
  - raftLogGCWorker : 负责 region log 的 compact (压缩)
  - splitCheckerWorker: 负责检测是否需要 split region
  - schedulerWorker: 负责和 tinyScheduler 交互的 worker
  - regionWorker: 负责快照相关的任务, 包括 create/apply snapshot 等
- TickDriver: 定时器的作用, 定时发送 tick 指令, 通过 router 发送给 peer_msg_handle, 做一些定时任务:
  - check split: 检测是否需要 split region
  - region heartbeat: 定时发送 region heartbeat
  - raft tick: 触发 raft.tick()
  - raft log gc: 定时 compact raft log.

- Router: 包含 peerSender 和 storeSender (这两都是 chan类型)
  - peerSender 被 raftWorker 所持有
  - storeSender 被 storeWorker 所持有
  - 很显然, 上层可以将数据发送给 route, route 再转发给 peerSender / storeSender, Route 故名思意就是路由的作用
- RaftWorker : 接收来自 peerSender 的消息, 将消息转发到特定 region 的 PeerMsgHandler 中处理, 也即根据消息中的 regionId 进行转发, 从而实现初版的 Multi-raft 架构
- PeerMsgHandler: Peer 的消息处理器, 主要有两个重要函数, 起到承上启下的作用:
  - HandleMsg() : 接收上层的消息, 处理特定的消息, 如 propose raft cmd, split region 等
  - HandleRaftReady(): 处理 下层 raft 模块准备就绪的消息(如持久化存储日志, snapshot, 发送消息给其他的 store, 应用日志到状态机等)
  - 这两个函数都是在 raftWorker 中被调用 (go 经典的 for 循环, 通过 chan 接收消息, 并调度 peerMsgHandler)
  - 因此 raftWorker 可以算是底层模块的调度器
- Peer: 封装了 raftModule, 包括 raft rawNode (也即 raft 的包装) 和 peerStorage (raft 的持久化存储)
- RawNode: 这是我们在 project2A 中实现的 raft 模块
- PeerStorage: 能为 Peer 提供持久化存储的存储层, 基于 badgerdb

最后, 我们可以以一个客户端发送 propose 请求为例, 串联这个架构图:

- 客户端通过 rpc 发送 RawGet/RawPut 等请求
- 客户端代理将该请求发送到特定的 raftStore 上
- raftStore 将请求发送到 router 的 peerSender
- raftWorker 通过监听 peerSender 获取消息, 根据 regionId, 转发到对应的 peerMsgHandler, 调用 handleMsg()  处理该消息
- peerMsgHandler 通过调用底层的 rawNode 来 propose() 该请求, 并保存 callBack()
- raft module 转发并 commit 该日志
- raftWorker 通过调用 peerMsgHandler  的 handleRaftReady() 来处理 准备就绪的消息, 也即 apply 已经committed 的日志到 statemachine, 并通过 callBack() 返回给客户端

以上的架构图需要好好研究, 后续都会用到.



# 三 代码

在本次实验中, 我们不需要实现 multi-raft , 只需要实现简单的 single-raft-kv 即可, 如图:

重点的代码就是 PeerStorage(持久化存储) 和 PeerMsgHandler

![image-20211201150150943](https://gitee.com/zisuu/mypicture/raw/master/image-20211201150150943.png)

## Implement peer storage

在这个 lab 中, 我们只需要完成 peerStorage 中的 SaveReadyState() 和 append() 即可

### 持久化存储的状态

在 project2A 的题解中, 我们知道 raft 是需要持久化存储一些状态的, 包括 term, voteFor, log, commit, lastApplied 等

在 PeerStorage 里就包含了这些状态:

- RaftLocalState: 包括 Term, vote, commit, lastIndex, lastTerm 等, 是 raft 相关的状态
- RaftApplyState: 包括 appliedIndex 和 truncateState(truncateIndex, truncateTerm) 等

此外, 还有 regionState:

- RegionLocalState: 存储了一个 region 的相关信息和 peerState

需要注意的是, 这三个 state 和 log 日志需要存在不同的 badger instance 里面, 有不同 的 key prefix

(这很好理解, log 和 localState 是和 raft 有关, applyState 和 regionState 和 kv region有关)

| Key              | KeyFormat                        | Value            | DB   |
| :--------------- | :------------------------------- | :--------------- | :--- |
| raft_log_key     | 0x01 0x02 region_id 0x01 log_idx | Entry            | raft |
| raft_state_key   | 0x01 0x02 region_id 0x02         | RaftLocalState   | raft |
| apply_state_key  | 0x01 0x02 region_id 0x03         | RaftApplyState   | kv   |
| region_state_key | 0x01 0x03 region_id 0x01         | RegionLocalState | kv   |

### saveReadyState

总的来说,  saveReadyState() 需要将 ready 结构体中的准备就绪的状态应用到 badger 中, 包括:

- apply snapshot , 这个在 project2C 中会做到
- append logs

- save raftLocalState(hardState, lastIndex, lastEntry)

```
func (ps *PeerStorage) SaveReadyState(ready *raft.Ready) (*ApplySnapResult, error) {
	// Hint: you may call `Append()` and `ApplySnapshot()` in this function
	// Your Code Here (2B/2C).
	kvWB := new(engine_util.WriteBatch)
	raftWB := new(engine_util.WriteBatch)

	// Apply snapshot
	var applySnapResult *ApplySnapResult
	if !raft.IsEmptySnap(&ready.Snapshot) {
		// 在 project2C 中会做到
		result, err := ps.ApplySnapshot(&ready.Snapshot, kvWB, raftWB)
		if err != nil {
			return nil, errors.Trace(err)
		}
		applySnapResult = result
	}

	// Append log to raftWB
	if len(ready.Entries) > 0 {
		err := ps.Append(ready.Entries, raftWB)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	// Save raftLocalState to raftWB
	if !raft.IsEmptyHardState(ready.HardState) {
		ps.raftState.HardState = &ready.HardState
	}
	err := raftWB.SetMeta(meta.RaftStateKey(ps.region.Id), ps.raftState)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Write data to each db
	err = raftWB.WriteToDB(ps.Engines.Raft)
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = kvWB.WriteToDB(ps.Engines.Kv)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return applySnapResult, nil
}

```



## Implement Raft ready process

在 第二小节 中, 我们了解到:

- raftWorker 是底层 raftModule 的调度器, 驱动底层的运行
- peerMsgHandler() 启动承上启下的作用, 用于处理特定消息

实际上, tinyKv 已经把上层的包括 raftWorker 在内的细节都处理好了,这一节, 我们只需要

完成 peerMsgHandler 的两个重要方法, 也即 handleMsg() 和 handleRaftReady()

- handleMsg 只需要完成其中的 proposeRaftCommand 即可, 也即开启 propose 的流程
- handleRaftReady 需要调用 PeerStorage.saveReady() 来持久化存储, 同时 apply logs 到状态机

### proposeRaftCommand ()

raft commad 分为两种类型：

- data request : 也即 put / get / delete / scan
- admin request : 也即 compact log / change peers / split region

这一节我们主要关注 dataRequest, adminRequest 主要是成员变更之类的, 后面的 lab 才需要处理

先搭建一个具体的框架：

```
func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {

   // Propose message
   if len(msg.Requests) > 0 {
      d.proposeDataRequest(msg, cb)
   }

   // Propose admin request
   if msg.AdminRequest != nil {
      d.proposeAdminRequest(msg, cb)
   }
   
}
```

而 proposeDataRequest() 只需要调用底层的 raftModule  来 propose 该 request 即可

需要注意的是, callback 需要保存到 peer.proposals 中, 后续 apply 该日志时, 才可以取出来

```
func (d *peerMsgHandler) proposeDataRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	data, err := proto.Marshal(msg)
	if err != nil {
		log.Errorf("Error when encode msg: %s", err.Error())
		return
	}

	// Append callback
	d.appendProposal(cb)

	err = d.RaftGroup.Propose(data)
	if err != nil {
		log.Errorf("Error when propose a msg: %s", err.Error())
	}
}


func (d *peerMsgHandler) appendProposal(cb *message.Callback) {
	d.proposals = append(d.proposals, &proposal{
		term:  d.Term(),
		index: d.nextProposalIndex(),
		cb:    cb,
	})
}
```



### handleRaftReady()

文档写的很清楚了 , 就是主要做以下这些事情:

- 通过 peerStorage 持久化存储 ready 中的数据 （包括 snapshot, entries 等）
- send message to other stores
- apply committed logs to state machine
- advance()

```
// Receive ready from rawNode, and apply same change s
func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	raftGroup := d.RaftGroup
	if !raftGroup.HasReady() {
		return
	}
	ready := raftGroup.Ready()

	// Save ready state
	snapshotApplyResult, err := d.peerStorage.SaveReadyState(&ready)
	if err != nil {
		log.Errorf("Error happens when save ready state: %s", err.Error())
	}

	// Update region info in storeMeta
	if snapshotApplyResult != nil {
		preRegion, curRegion := snapshotApplyResult.PrevRegion, snapshotApplyResult.Region
		if !reflect.DeepEqual(preRegion, curRegion) {
			d.peerStorage.region = curRegion
			d.ctx.storeMeta.replaceRegion(preRegion, curRegion)
		}
	}

	// Send message
	d.Send(d.ctx.trans, ready.Messages)

	// Apply log to state machine
	if len(ready.CommittedEntries) > 0 {
		kvWB := new(engine_util.WriteBatch)
		for _, entry := range ready.CommittedEntries {
			kvWB = d.applyCommittedEntry(entry, kvWB)
			if d.stopped {
				return
			}
		}
		d.persistApplyState(kvWB, ready.CommittedEntries[len(ready.CommittedEntries)-1].Index)
		d.writeChangesToKvDB(kvWB)
	}

	// Advance
	d.RaftGroup.Advance(ready)
}
```

需要重点关注的是 applyCommittedEntry

### applyCommittedEntry()

和 proposeRaftCommand 类似， 也需要分两类型来分别处理，

下面先搭一个框架， 后续的 lab 都会用到：

```
func (d *peerMsgHandler) applyCommittedEntry(entry eraftpb.Entry, kvWB *engine_util.WriteBatch) *engine_util.WriteBatch {
	if entry.Data == nil {
		return kvWB
	}
	// decode msg
	msg, err := d.unMarshalRequest(entry)
	if err != nil {
		return kvWB
	}

	// Apply data request
	if len(msg.Requests) > 0 {
		return d.applyDataRequest(msg, entry, kvWB)
	}

	// Apply admin request
	if msg.AdminRequest != nil {
		if err := d.checkRegionEpoch(msg, entry); err != nil {
			return kvWB
		}
		return d.applyAdminRequest(msg.AdminRequest, entry, kvWB)
	}
	return kvWB
}

```

同样， 目前我们只关注 applyDataRequest：

```
func (d *peerMsgHandler) applyDataRequest(msg raft_cmdpb.RaftCmdRequest, entry eraftpb.Entry, kvWB *engine_util.WriteBatch) *engine_util.WriteBatch {
   request := msg.Requests[0]

   // Apply modified changes
   switch request.GetCmdType() {
   case raft_cmdpb.CmdType_Put:
      putRequest := request.GetPut()
      kvWB.SetCF(putRequest.GetCf(), putRequest.GetKey(), putRequest.GetValue())
   case raft_cmdpb.CmdType_Delete:
      deleteRequest := request.GetDelete()
      kvWB.DeleteCF(deleteRequest.GetCf(), deleteRequest.GetKey())
   }

   // Write chagnes to badgerdb
   d.persistApplyState(kvWB, entry.Index)
   d.writeChangesToKvDB(kvWB)
   kvWB = new(engine_util.WriteBatch)

   // Get propose
   pr, _ := d.getProposal(entry.Index, entry.Term)
   if pr == nil {
      return kvWB
   }

   // Send response
   resp := &raft_cmdpb.RaftCmdResponse{
      Header:    &raft_cmdpb.RaftResponseHeader{},
      Responses: []*raft_cmdpb.Response{},
   }
   switch request.CmdType {
   // Apply put
   case raft_cmdpb.CmdType_Put:
      {
         resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
            CmdType: raft_cmdpb.CmdType_Put,
            Put:     &raft_cmdpb.PutResponse{},
         })
      }
   // Apply delete
   case raft_cmdpb.CmdType_Delete:
     ....
   // Apply get
   case raft_cmdpb.CmdType_Get:
     ....
   // Apply scan
   case raft_cmdpb.CmdType_Snap:
     ....
   }

   // Call back
   pr.cb.Done(resp)
   return kvWB
}
```



# 四 总结

在第三节中, 我们以及搭建了一个清晰的框架:

对于 propose cmd, 需要调用 peer_msg_handler.proposeRaftCommand(), 并分为 dataRequest 和 adminRequest 两种

```
func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {

   // Propose message
   if len(msg.Requests) > 0 {
      d.proposeDataRequest(msg, cb)
   }

   // Propose admin request
   if msg.AdminRequest != nil {
      d.proposeAdminRequest(msg, cb)
   }
   
}
```

而对于 apply cmd, 也是分为两种:

```
func (d *peerMsgHandler) applyCommittedEntry(entry eraftpb.Entry, kvWB *engine_util.WriteBatch) *engine_util.WriteBatch {
	,,,,,,,,,,,,,,,,,,,,,,,,

	// Apply data request
	if len(msg.Requests) > 0 {
		return d.applyDataRequest(msg, entry, kvWB)
	}

	// Apply admin request
	if msg.AdminRequest != nil {
		return d.applyAdminRequest(msg.AdminRequest, entry, kvWB)
	}
	return kvWB
}

```

因此, 后面的实验其实就是在这个基础上进行补充, 比如

- 2C 中的 snapshot, 就是一个 adminRequest
- 3B 中的 conf change 和 leader transfer, split region , 也是 adminRequest 类型

相信做完 project2B, 后续的实验就没什么难度啦~