# 一 任务概览

随着系统的运行时间增长， raft log 会无限增大， 这会增大磁盘的压力

raft 论文中提到， 可以通过 compact raft log 和 snapshot state machine 这两个机制来优化

在 Raft 中，Snapshot 指的是整个 State Machine 数据的一份快照，大体上有以下这几种情况需要用到 Snapshot：

1. 正常情况下 leader 与 follower/learner 之间是通过 append log 的方式进行同步的，出于空间和效率的考虑，leader 会定期清理过老的 log。假如 follower/learner 出现宕机或者网络隔离，恢复以后可能所缺的 log 已经在 leader 节点被清理掉了，此时只能通过 Snapshot 的方式进行同步。
2. Raft 加入新的节点的，由于新节点没同步过任何日志，只能通过接收 Snapshot 的方式来同步。实际上这也可以认为是 1 的一种特殊情形。
3. 出于备份/恢复等需求，应用层需要 dump 一份 State Machine 的完整数据。

因此， 本节， 需要我们做完 snapshot 模块



# 二 整体预览

所以， 还是先从整体上预览一下吧, 整个 snapshot 的流程如下:

![image-20211202153108015](https://gitee.com/zisuu/mypicture/raw/master/image-20211202153108015.png)

- 上层的 ticker 定期发送 tick 指令给 peer_msg_handler, 如果到达了 compact log 的时间点, 遂开始截断日志, 具体的做法是发送一个 RaftLogGCTask 给 raftLog_gc_worker (在 Project2b 中有提到)
- raftLog_gc_worker 会开始清楚磁盘上的日志
- 当出现以下两种情况时:
    - follower 大幅度落后 leader, nextIndex 处的日志已经被截断了
    - 新加入了一个节点
- leader 会尝试将 snapshot 发送给 follower, 具体的做法是通过 storage.snapshot() 来获取 snapshot
- leader 将 snapshot 发送给 follower, follower 调用 handleSnapshot() 来处理, 同时将 snapshot 保存到 raftLog 的 pendingSnapshot 中
- 上层 peerMsgHandle 的 handleRaftReady 会处理 该 pendingSnapshot , 并开始 applySnapshot

以上的步骤涉及到 project2b 中讲解的内容, 会比较杂乱, 我在代码模块会详细的讲解过程.

总的来说, 分为四步:

- 1.truncate log
- 2.create snapshot
- 3.handle snapshot
- 4.apply snapshot

# 三 代码

## 1.truncate log

### truncate persisted store log

tickDriver 定时发送一条 tick 指令， 通过 router 发送给对应的 peer_msg_handler

peer_msg_handler 接受到后， 将其封装成 CompactLogRequest(admin 类型)， 并propose 该 request

一旦该 request 被 commit 后， 在状态机模块， 也即 handleRaftReady() 中， 就要 apply 该 gc request

具体的处理：

- 修改 applyState 中的 truncateState, 并持久化存储到 kvdb 中
- 构建一个 RaftLogGCTask ， 通过 raftLogGCTaskSender 发送给 raftLogGCWorker， 其会主动的删除

​	   【startIndex, truncateIndex】 之间的 log 日志

也即， 这个 CompactLogRequest 就是负责对持久化存储的日志进行 compact , 但是还没有生成 snapshot.

```
func (d *peerMsgHandler) applyAdminRequest(request *raft_cmdpb.AdminRequest, entry eraftpb.Entry) {
   kvWB := new(engine_util.WriteBatch)
   switch request.CmdType {
   // Compact log
   case raft_cmdpb.AdminCmdType_CompactLog:
      {
         d.handleCompactLogRequest(request.CompactLog, kvWB)
      }
   }
   .......
}

func (d *peerMsgHandler) handleCompactLogRequest(req *raft_cmdpb.CompactLogRequest, kvWB *engine_util.WriteBatch) {
	lastIndex, lastTerm := req.CompactIndex, req.CompactTerm
	if lastIndex >= d.peerStorage.applyState.TruncatedState.Index {
		d.persistTruncateState(kvWB, lastIndex, lastTerm)
		// Send gc task
		gcTask := &runner.RaftLogGCTask{
			RaftEngine: d.peerStorage.Engines.Raft,
			RegionID:   d.regionId,
			StartIdx:   d.LastCompactedIdx,
			EndIdx:     lastIndex + 1,
		}
		d.LastCompactedIdx = gcTask.EndIdx
		d.ctx.raftLogGCTaskSender <- gcTask
	}
}
```



## 2.create snapshot

现在 Leader 磁盘中的一部分日志已经被 truncate 了， 一旦 存在以下情况：

- 有新的 follower 加入 leader
- follower 大幅度的 落后 leader
- ....

此时， leader 就会找不到该 follower 的 nextIndex 对应的日志， 于是， 就知道已经被 truncate 了， 需要构建一个 snapshot 发送给 follower:

```
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	nextIndex := r.Prs[to].Next
	preLogTerm, err := r.RaftLog.Term(preLogIndex)
	if err != nil {
		// 发送 snapshot 
		r.sendSnapshot(to)
		return false
	}
}

// sendSnapshot send snapshot to the given peer
func (r *Raft) sendSnapshot(to uint64) bool {
	// Your Code Here (2A).
	// 获取 snapshot
	snapshot, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		return false
	}
	snapshotRequest := pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		From:     r.id,
		To:       to,
		Term:     r.Term,
		Snapshot: &snapshot,
	}
	r.sendMessage(snapshotRequest)
	r.Prs[to].Next = snapshot.Metadata.Index + 1
	return true
}

```

获取一个 snapshot 的方式很简单， 只需要通过 storage . snapshot() 即可， 我们可以来看一下 peerStorage.snapshot() 的逻辑：

可以发现， 其发送了一个 RegionTaskGen 任务给 regionSched, 根据文档的解释，

这个 regionSched 是被 regionTaskWorker 所持有的 chan

也即， 创建 snapshot 的逻辑是在 regionTaskWorker 中

```
func (ps *PeerStorage) Snapshot() (eraftpb.Snapshot, error) {
   var snapshot eraftpb.Snapshot
   。。。。。。。。。。。
   log.Infof("%s requesting snapshot", ps.Tag)
   ps.snapTriedCnt++
   ch := make(chan *eraftpb.Snapshot, 1)
   
   
   ps.snapState = snap.SnapState{
      StateType: snap.SnapState_Generating,
      Receiver:  ch,
   }
   
   
   // schedule snapshot generate task
   ps.regionSched <- &runner.RegionTaskGen{
      RegionId: ps.region.GetId(),
      Notifier: ch,
   }
   return snapshot, raft.ErrSnapshotTemporarilyUnavailable
}
```



## 3.handle snapshot

一旦 leader 创建了一个 snapshot , 并发送给了 follower ， follower 便可以 Handle该 snpashot 了：
但是， 比较特殊的是， follower 并不会在 handleSnapshot () 中直接 apply 该 snapshot, 其只会先修改

- raftLog 的 几个 index(包括 commit, apply, stable 等)
- 把 snapshot 放在 raftlog.pedingSnapshot 中， 等待上层的 raftReady() 取走处理
- 修改 peers

```
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	lastIndex := m.Snapshot.Metadata.Index
	if lastIndex <= r.RaftLog.committed {
		r.sendAppendResponse(m.From, r.Term, r.RaftLog.LastIndex(), true)
		return
	}
	
	r.becomeFollower(m.Term, m.From)
	// Reset log status
	r.RaftLog.resetAllIndex(lastIndex)
	r.RaftLog.pendingSnapshot = m.Snapshot
	r.RaftLog.entries = []pb.Entry{}
	// Peers
	r.Prs = make(map[uint64]*Progress)
	for _, peer := range m.Snapshot.Metadata.ConfState.Nodes {
		r.Prs[peer] = &Progress{}
	}

	r.sendAppendResponse(m.From, r.Term, r.RaftLog.LastIndex(), false)
}

```

## 4.apply snapshot

在  peerMsgHandler.handleRaftReady 中， 我们获取到了 raft ready, 其中就包含了 snapshot

我们可以通过 peerStorage 提供的 ApplySnapshot 来 apply 该snapshot:
根据文档的说明， 主要要做以下这几点：

- 修改 raftState 中的 lastIndex, lastTerm
- 修改 applyState 中的 appliedIndex, truncateState
- 发送 RegionTaskApply 给 regionTaskWorker, 来 apply 该 snapshot 的数据到状态机（也即 badger) 中
- 修改 region state
- 返回 applySnapResult:

```
func (ps *PeerStorage) ApplySnapshot(snapshot *eraftpb.Snapshot, kvWB *engine_util.WriteBatch, raftWB *engine_util.WriteBatch) (*ApplySnapResult, error) {
	log.Infof("%v begin to apply snapshot", ps.Tag)
	snapData := new(rspb.RaftSnapshotData)
	if err := snapData.Unmarshal(snapshot.Data); err != nil {
		return nil, err
	}

	// Hint: things need to do here including: update peer storage state like raftState and applyState, etc,
	// and send RegionTaskApply task to region worker through ps.regionSched, also remember call ps.clearMeta
	// and ps.clearExtraData to delete stale data
	// Your Code Here (2C).
	
	
	// Clear meta, clear data
	if ps.isInitialized() {
		err := ps.clearMeta(kvWB, raftWB)
		if err != nil {
			return nil, err
		}
		ps.clearExtraData(snapData.Region)
	}

	regionId := snapData.Region.Id
	lastIndex, lastTerm := snapshot.Metadata.Index, snapshot.Metadata.Term

	// 1.update raft state, raftState will be appended to raftWB in SaveReadyState()
	raftState := ps.raftState
	raftState.LastIndex = lastIndex
	raftState.LastTerm = lastTerm

	// 2.update apply state
	applyState := ps.applyState
	applyState.AppliedIndex = lastIndex
	applyState.TruncatedState.Index = lastIndex
	applyState.TruncatedState.Term = lastTerm
	_ = kvWB.SetMeta(meta.ApplyStateKey(ps.region.Id), applyState)

	// 3.send regionTaskApply to region worker by regionSched
	notifier := make(chan bool, 1)
	applyTask := &runner.RegionTaskApply{
		RegionId: regionId,
		SnapMeta: snapshot.Metadata,
		StartKey: snapData.Region.StartKey,
		EndKey:   snapData.Region.EndKey,
		Notifier: notifier,
	}
	ps.regionSched <- applyTask

	// wait task done
	<-notifier
	ps.snapState.StateType = snap.SnapState_Applying

	// 4.write region state
	regionState := &rspb.RegionLocalState{
		State:  rspb.PeerState_Normal,
		Region: snapData.Region,
	}
	_ = kvWB.SetMeta(meta.RegionStateKey(regionId), regionState)

	// 5.return snapshot result
	res := &ApplySnapResult{
		Region:     snapData.Region,
		PrevRegion: ps.region,
	}
	return res, nil
}
```