# 一 任务概述

project3A , 引导我们如何一步一步实现一个完整的 multi-raft , 包括:

- raft 模块的 leader 切换和 成员变更
- raftstore 模块的  leader 切换和 成员变更, 以及 region split
- tinyScheduler 中的 balance-region 的逻辑

![image-20211201144741591](https://gitee.com/zisuu/mypicture/raw/master/image-20211201144741591.png)

在 multi-raft 框架中, 每个 region group 和 store 定期的向 scheduler(PD) 发送心跳, scheduler 中也有调度器(如 balance-region-scheduler 和 balance-leader-scheduler), 会自动的检测每个 store 上的负载, 如果发现倾斜的情况, 比如 region 的分布不均匀以及 leader 的分布不均匀, 就会发送指令给对应的 region group, 指令包括:

- 删除某个 store 上的 region replicate
- 在某个 store 上添加 region replicate
- 切换 leader
- ...................

这就是 raft 的成员变更和 leader 切换在 multi-raft 中的体现, 并具备了 multi-raft 的初步条件.

此外, 每个 store 自身也会定期的检测是否需要 split 一个 region, 或者 merge 相邻的两个 region.

成员变更 + region split, 就是一个完整的 multi-raft 框架了

OK, 就让我们好好享受 coding 吧~



# 二 3A 总结

## 概述

3A 比较简单, 只需要在 raft module 实现 leader transfer 和 conf change 即可

## leader transfer

raft phd 论文里给出了 leader transfer 的步骤：

- Leader 记录 leadTransferee， 同时阻止接受新的提案
- Leader 等待 newLeader 追赶日志， 直到其 match >= lastLogIndex
- Leader 给 newLeader 发送一个 timeoutNow Request
- newLeader 接受到该 request 后， 直接触发 electionTimeout, 开始新一轮的选举

按照这个步骤， 就很容易做出来了：

**leader 收到 transfer 请求**

```
func (r *Raft) handleTransferLeader(m pb.Message) {
  。。。。。。。。。

   // 记录leadTransferee
   r.leadTransferee = targetId
   // If up to date, send TimeoutNow request
   if r.Prs[targetId].Match >= r.RaftLog.LastIndex() {
      r.sendTimeoutNowRequest(targetId, r.Term)
   } else {
      // Append log to follower, wait catch up
      r.sendAppend(targetId)
   }
}
```

在 handleAppendResponse 中, 如果 newLeader 追赶上了 leader, 则发送 timeoutNow :

```
// 4.if leadTransferee == from, and log is matched
if r.leadTransferee == m.From {
   if r.Prs[m.From].Match >= r.RaftLog.LastIndex() {
      r.sendTimeoutNowRequest(m.From, r.Term)
      r.leadTransferee = None
   }
}
```

**newLeader 处理 timeoutNow:**

```
// handleTimeoutNowRequest transfer leadership to self, start new election immediately
func (r *Raft) handleTimeoutNowRequest(m pb.Message) {
   _, existed := r.Prs[r.id]
   if !existed {
      return
   }

   // Start new election
   r.handleCampaign()
}
```





## conf change

在 raft module 中， 和 conf change 相关的只有两个地方：

- addNode() / removeNode()
- pendingConfIndex

其中， addNode() / removeNode() 是上层的 peerMsgHandler 在 apply conf change 时会调用的， 我们只需要实现在 prs 中添加或删除 peer 即可：

```
// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	r.PendingConfIndex = None
	_, existed := r.Prs[id]
	if !existed {
		r.Prs[id] = &Progress{
			Match: 0,
			Next:  1,
		}
	}
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	r.PendingConfIndex = None
	_, existed := r.Prs[id]
	if existed {
		delete(r.Prs, id)
		r.boostCommitIndexAndBroadCast()
	}
}
```

**pendingConfIndex**

通过标注可以理解这个变量的作用， 简单来说就是 在同一时刻只能有一个还没有 apply 的 conf log, 其 Index 我们用 raft 中的 pendingConfIndex 来暂存

```
// Only one conf change may be pending (in the log, but not yet
// applied) at a time. This is enforced via PendingConfIndex, which
// is set to a value >= the log index of the latest pending
// configuration change (if any). Config changes are only allowed to
// be proposed if the leader's applied index is greater than this
// value.
// (Used in 3A conf change)
PendingConfIndex uint64
```

因此， 在 raft 中， 当我们 append log 时， 需要保证当新的 conf log 出现时， 没有旧的 conf log 正在 pending:

```
// appendEntriesWithTerm with target term
func (l *RaftLog) appendEntriesWithTerm(entries []*pb.Entry, term uint64, pendingConfIndex *uint64) {
	for _, entry := range entries {
		if entry.EntryType == pb.EntryType_EntryConfChange {
			if *pendingConfIndex == None {
				*pendingConfIndex = entry.Index
			} else {
				continue
			}
		}
		l.entries = .......
	}
}
```

# 三 3B 总结

## 概述

lab3b 的任务不好做， 每个小点都要做好几天才能通过测试。。。

总的来说， 我们已经在 lab3a 中， 在 raft module 层面做了 leader transfer 和 conf change

那现在 peerMsgHandler 中就需要做以下三件事情：

- propose and apply leader transfer
- propose and apply conf change
- propose and apply split region

这三个 lab 都有相同点： 都是 raftCommandRequest 中的 adminRequest 类型

那么， 显然, 根据我们在 2b 中搭建的 peer代码, 修改点都会涉及到 peerMsgHandler 中的 proposeAdminReqeust 和 applyAdminRequest...

```
func (d *peerMsgHandler) proposeAdminRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
   adminRequest := msg.AdminRequest
   switch adminRequest.CmdType {
   case raft_cmdpb.AdminCmdType_TransferLeader:
   case raft_cmdpb.AdminCmdType_ChangePeer:
   case raft_cmdpb.AdminCmdType_Split:
   }
}
```

```
func (d *peerMsgHandler) applyAdminRequest(request *raft_cmdpb.AdminRequest, entry eraftpb.Entry, kvWB *engine_util.WriteBatch) *engine_util.WriteBatch {
	switch request.CmdType {
	// Change peer
	case raft_cmdpb.AdminCmdType_ChangePeer:
		{
			d.handleChangePeerRequest(entry, request.ChangePeer, kvWB)
		}
	// Split region
	case raft_cmdpb.AdminCmdType_Split:
		{
			d.handleSplitRegion(entry, request.Split, kvWB)
		}
	}
	return kvWB
}
```

当我们做完了这三个 lab, 那么就具备了完整的 成员变更和 region split 的 能力， 也即初步具备了 Multi-raft 的条件。



## handle leader transfer

### propose

文档中说明了， leader transfer 无需走 raft 的提案流程， 只需要直接发给 raft leader 即可

因此， 我们只需要在 proposeAdminRequest 中， 直接调用底层的 raft module 即可：

	case raft_cmdpb.AdminCmdType_TransferLeader:
		{
			// It is no need to propose transfer leader,
			// just call rawNode to Handle it directly and send response
			targetId := adminRequest.TransferLeader.Peer.Id
			d.RaftGroup.TransferLeader(targetId)
			resp := newCmdResp()
			resp.AdminResponse = &raft_cmdpb.AdminResponse{
				CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
				TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
			}
			cb.Done(resp)
		}



## handle conf change

### propose

文档中提到， 对于 conf change 类型的 request, 需要包装成 ConfChange 这个类

并通过 ProposeConfChange 进行提案：

```
case raft_cmdpb.AdminCmdType_ChangePeer:
   {
      changePeerRequest := adminRequest.ChangePeer
      data, err := msg.Marshal()
      if err != nil {
         log.Errorf("Error when encode msg: %s", err.Error())
         return
      }
      d.appendProposal(cb)

      // Propose conf change
      err = d.RaftGroup.ProposeConfChange(eraftpb.ConfChange{
         ChangeType: changePeerRequest.ChangeType,
         NodeId:     changePeerRequest.Peer.Id,
         Context:    data,
      })
      if err != nil {
         log.Errorf("Error when propose conf change request : %s", err.Error())
         return
      }
   }
```

### apply

handle change peer 需要做以下几件事情：

- 修改 confVersion, 修改 storeMeta 中的全局数据， 修改 peer cache

- 调用底层的 raft module 的 ApplyConfChange， 也即调用 lab3a 中的 addNode / removeNode

```
peer := req.Peer
peerId := peer.Id
region := d.Region()

// Find peer
existed := false
for _, p := range d.Region().Peers {
   if p.Id == peerId {
      existed = true
      break
   }
}

// Add/Remove node, update region(regionEpoch, peers)
if req.GetChangeType() == eraftpb.ConfChangeType_AddNode {
   if existed {
      return
   }
   d.ctx.storeMeta.changeRegionPeer(d.Region(), peer, true)
   meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
   d.insertPeerCache(peer)
} else {
   if !existed {
      return
   }
   if peerId == d.Meta.Id {
      d.destroyPeer()
      return
   }
   d.ctx.storeMeta.changeRegionPeer(d.Region(), peer, false)
   meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
   d.removePeerCache(peerId)
}

// Apply conf change to raft module
d.RaftGroup.ApplyConfChange(eraftpb.ConfChange{
   ChangeType: req.ChangeType,
   NodeId:     peerId,
   Context:    nil,
})
```



## handle split region

### propose

和普通的提案差不多。。

### apply

handle split region 主要做以下几件事情：

- 构建新的 region
- 修改 storeMeta 的全局元数据信息
- 保存 regionState
- 创建新的 peer ， 并将其注册到 router 中



```
// 0. Check key was in region
err := util.CheckKeyInRegion(req.SplitKey, d.Region())
if err != nil {
   pr, _ := d.getProposal(entry.Index, entry.Term)
   if pr != nil {
      pr.cb.Done(ErrResp(err))
   }
   return
}

// 1. Update region epoch
originRegion := proto.Clone(d.Region()).(*metapb.Region)
originRegion.RegionEpoch.Version++

// 2. Create new region
newRegion := copyRegionInfo(originRegion, req)
originRegion.EndKey = req.SplitKey

// 3. Update global meta
d.ctx.storeMeta.setRegions(originRegion, newRegion)
d.peerStorage.SetRegion(originRegion)

// 4. Write region state
meta.WriteRegionState(kvWB, originRegion, rspb.PeerState_Normal)
meta.WriteRegionState(kvWB, newRegion, rspb.PeerState_Normal)

// 5. Create new peer
d.createNewPeerAndStart(newRegion)
```



# 四 3C 总结

## 概述

这个 lab 相比前面两个实在是!!! 幸福, 如果对 pd 有所了解的话, 做这个 lab 就比较容易了

实验知道书告诉我们, 在前面的 lab 的中, 我们已经完成了成员变更等 multi-raft 的基础, 现在的问题是, 谁给我们发出 change-peer / transfer-leader 的指令? 答案就在 scheduler 包下

scheduler 包下的代码实际上就是缩减版的 pd

所有的 region 每隔一段时间就会发送一个 regionHeartBeat , 其中记录了像 regionSize 这样的信息, 发送给 pd, pd 会收集每个 store 下的所有 region 的信息, 并统计 storeRegionCount, storeRegionSize 等信息

同时, pd 中有一些 scheduler, 在不间断的对整个集群做出调度, 在这个缩减版 pd 中主要有两个:

- balance_region: 均衡每个 store上含有的 region 个数
- balance_leader: 均衡每个 store 上含有的 leader 个数, 因为 leader 是 raftGroup 读写的入口, 也需要负载均衡

而这个 Lab 主要要求我们做两件事情:

- 补全 cluster/processRegionHeartbeat(), 当 pd 收到 regionHeartBeat 时, 应更改本地的缓存信息
- 补全 balance_region / schedule(), 也即均衡region 在 store 上的分布

## processRegionHeartbeat()

指导书中提到, 当收到 regionHeartbeat 时, 不能马上更新 Local cache:

- 需要和本地的 oldRegionInfo 进行对比, 判断该 Heartbeat 是否是 stale 的, 也即 regionEpoch 是否比旧的小

- 其次, 还要判断是否存在更新

- 如果本地不存在该 region, 要判断是否有别的 region 和 该 region 的范围重叠了, 有的话也要判断 epoch

**判断 stale:**

```
func (c *RaftCluster) checkRegionEpochStale(oldEpoch *metapb.RegionEpoch, newEpoch *metapb.RegionEpoch) bool {
   if oldEpoch.Version > newEpoch.Version {
      return true
   }
   if oldEpoch.ConfVer > newEpoch.ConfVer {
      return true
   }
   return false
}
```

**判断是否存在更新:**

```
func (c *RaftCluster) checkRegionInfoShouldUpdate(originRegionInfo, region *core.RegionInfo) bool {
   if region.GetRegionEpoch().Version > originRegionInfo.GetRegionEpoch().Version ||
      region.GetRegionEpoch().ConfVer > originRegionInfo.GetRegionEpoch().GetConfVer() {
      return true
   }
   if originRegionInfo.GetLeader() != region.GetLeader() {
      return true
   }
   if len(originRegionInfo.GetPendingPeers()) > 0 || len(region.GetPendingPeers()) > 0 {
      return true
   }
   if originRegionInfo.GetApproximateSize() != region.GetApproximateSize() {
      return true
   }
   return false
}
```

**整体代码:**

```
func (c *RaftCluster) processRegionHeartbeat(region *core.RegionInfo) error {
   // Your Code Here (3C).
   if region.GetRegionEpoch() == nil {
      return errors.New("The region epoch is nil")
   }
   
   originRegionInfo := c.core.GetRegion(region.GetID())
   if originRegionInfo != nil {
      // Check stale
      if c.checkRegionEpochStale(originRegionInfo.GetRegionEpoch(), region.GetRegionEpoch()) {
         return errors.New("Region info is stale")
      }
      if !c.checkRegionInfoShouldUpdate(originRegionInfo, region) {
         return nil
      }
   } else {
      overlapRegions := c.core.ScanRange(region.GetStartKey(), region.GetEndKey(), 0)
      for _, lapRegion := range overlapRegions {
         if c.checkRegionEpochStale(lapRegion.GetRegionEpoch(), region.GetRegionEpoch()) {
            return errors.New("The new regionInfo is stale")
         }
      }
   }
   // Come here, we can update local cache info now
   c.core.PutRegion(region)
   for storeId := range region.GetStoreIds() {
      c.updateStoreStatusLocked(storeId)
   }
   return nil
}
```



## balance_region/schedule()

这个 lab 要求我们对整个集群的 stores 上的 region 进行负载均衡

schedule 函数要求返回一个 operator, 代表把一个 region 从一个 store 移动到另一个 store 上

思路:

- 对 stores 按照 regionSize 从大到小排序, 那很显然就是从 regionSize 大的 store 上找出一个 region, 移动到 regionSize 小的 store 上

  ```
  	stores := s.filterSuitableStores(cluster)
  	sort.Slice(stores, func(i, j int) bool {
  		return stores[i].GetRegionSize() > stores[j].GetRegionSize()
  	})
  ```



- 寻找合适的region 进行调度: 从 stores 的第一个 store 开始找, 找出一个 合适的 region :

    - 先找pendingRegion
    - 没有pendingRegion, 则找 follower
    - 最后尝试找 Leader

  ```
  func (s *balanceRegionScheduler) filterSuitableRegion(sourceStore *core.StoreInfo, cluster opt.Cluster) *core.RegionInfo {
     var region *core.RegionInfo
  
     // Try select a pending region
     cluster.GetPendingRegionsWithLock(sourceStore.GetID(), func(container core.RegionsContainer) {
        randRegion := container.RandomRegion(nil, nil)
        if randRegion != nil {
           region = randRegion
        }
     })
     if region != nil {
        return region
     }
  
     // Try select a follower
     cluster.GetFollowersWithLock(sourceStore.GetID(), func(container core.RegionsContainer) {
        randRegion := container.RandomRegion(nil, nil)
        if randRegion != nil {
           region = randRegion
        }
     })
     if region != nil {
        return region
     }
  
     // Try select a leader
     cluster.GetLeadersWithLock(sourceStore.GetID(), func(container core.RegionsContainer) {
        randRegion := container.RandomRegion(nil, nil)
        if randRegion != nil {
           region = randRegion
        }
     })
     if region != nil {
        return region
     }
  
     return nil
  }
  ```

- 从 stores 的最后一个 store 开始, 找出一个合适的 targetStore 来装载该 region

    - 要求该 region 原先就不存在该 store 上

    - 其次 store 的regionSize的差距要大:

      ```
      for j := len(stores) - 1; j > i; j-- {
              if _, existed := storeIds[stores[j].GetID()]; !existed {
                  if sourceStore.GetRegionSize()-stores[j].GetRegionSize() >= 2*region.GetApproximateSize() {
                      targetStore = stores[j]
                      break
                  }
              }
          }
      ```



有了以上思路, 代码就出来了:

```
func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
   // Your Code Here (3C).
   stores := s.filterSuitableStores(cluster)
   sort.Slice(stores, func(i, j int) bool {
      return stores[i].GetRegionSize() > stores[j].GetRegionSize()
   })

   var i int
   var region *core.RegionInfo
   var sourceStore, targetStore *core.StoreInfo

   // Find suitable region
   for i = 0; i < len(stores); i++ {
      region = s.filterSuitableRegion(stores[i], cluster)
      if region != nil {
         sourceStore = stores[i]
         break
      }
   }
   if region == nil {
      return nil
   }

   // Find suitable target store
   storeIds := region.GetStoreIds()
   if len(storeIds) < cluster.GetMaxReplicas() {
      return nil
   }
   for j := len(stores) - 1; j > i; j-- {
      if _, existed := storeIds[stores[j].GetID()]; !existed {
         if sourceStore.GetRegionSize()-stores[j].GetRegionSize() >= 2*region.GetApproximateSize() {
            targetStore = stores[j]
            break
         }
      }
   }
   if targetStore == nil {
      return nil
   }

   // Create operator
   newPeer, err := cluster.AllocPeer(targetStore.GetID())
   if err != nil {
      return nil
   }
   fmtDesc := fmt.Sprintf("Move region peer  from %d to %d", sourceStore.GetID(), targetStore.GetID())
   op, err := operator.CreateMovePeerOperator(fmtDesc, cluster, region, operator.OpBalance, sourceStore.GetID(), targetStore.GetID(), newPeer.GetId())
   if err != nil {
      return nil
   }
   return op
}
```