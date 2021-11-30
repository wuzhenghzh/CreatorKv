
# 一 实验概述

因为 project2 每一个 part 的内容都比较多, 因此我分为三篇文章来讲述 project2 的解题思路

project2A 引导我们实现一个 tick-based-raft, 包括 leader 的选举, 日志的复制

最后实现 rawNode 中的 api, 来完成对 raft 的封装, rawNode  可以理解为 raft 的 '驱动器'

本文的接下来的内容:

- 第二小节, 我会先过一遍 raft 算法的基础模块
- 第三小节, 我会讲解 raft.go 的整体架构的布局
- 第四小节, 我会实现具体的任务, 并通过伪代码讲解思路

# 二 raft 算法

学习 basic - raft 算法, 我觉得最好的途径是学习 raft 的论文, 其中需要重点关注的就是论文中的 figure 图片, 而本次实验涉及到了 figure2, figure3..

这些 figure 不仅告诉我们 raft rpc 中包括了哪些数据, 同时也告诉我们当接受到 这些 rpc 时应该做出哪些反应, 可以说, 理解这些 figure 是完成 project2A 的关键

## state

state 这个 figure 告诉我们在 raft 里面哪些数据需要持久化存储, 哪些不需要

在 raft 中, 需要持久化存储的有:

- currentTerm : 当前 server 最后见到的 term
- votedFor: 因为在一个 term 只能给一个 server 投票, 因此也需要持久化存储, 防止多投
- log: raft 中的日志, 肯定也需要持久化存储, 这样 apply 时才能被状态机模块回放
- commitIndex : 当前的日志 commit 到了哪里
- lastApplied: 最后一个 apply 的日志索引位置, 保证重启时不会重复回放日志

以上这些状态在 project2B 中会进行持久化存储, project2A 暂时不用担心

不需要持久化的, 主要是 leader 对每个 follower 需要维护的两个指针:

- nextIndex: 下一条要发送的日志索引点
- matchIndex: 已经完全匹配的日志索引点

这两个指针不要持久化存储, 只需要分别初始化成 0 和 1, 通过 raft 的日志传输和一致性检查, 就可以自动恢复了

<img src="https://gitee.com/zisuu/mypicture/raw/master/image-20211130144603832.png" alt="image-20211130144603832" style="zoom: 67%;" />

## Leader 选举

raft 论文中描述到, leader 会和 follower 之前通过心跳来维持 lease 租约, 如果 follower 在一段时间内没有收到 heartbeat(这段时间成为 electionTimeout), 那么 follower 就会转而进入 candidate 状态

如图:

candidate 会做以下这些事情:

- 将其 term + 1
- 为自己投票
- 轮播 requestVote rpc 给其他的 server, requestVote rpc 包括:
    - term + lastLogIndex + lastLogTerm
- 如果收到了半数以上的投票, 则 candidate 转变为 新的 leader

<img src="https://gitee.com/zisuu/mypicture/raw/master/image-20211130152325678.png" alt="image-20211130152325678" style="zoom:67%;" />

而其他的 server 收到 requestVote rpc 时, 则会:

- 检查 term 和 本地的 term 的大小

- 查看该 candidate 发送过来的 lastLogIndex 和 lastLogTerm 是否是 up - to -date 的, 因为 raft 要求新选举出来的 leader 必须包含所有已经提交的日志. 对比的方法就是:

  lastLogTerm > local.lastLogTerm || (lastLogTerm == local.lastLogTerm && lastLogIndex >= local.lastLogIndex)

- 如果以上检查都通过了, 则为其投票, 并记录到 voteFor 中, 因为 server在每个 term 只能投票一次

<img src="https://gitee.com/zisuu/mypicture/raw/master/image-20211130150819904.png" alt="image-20211130150819904" style="zoom:67%;" />



## 日志复制

根据 raft log replication 模块的论述, 在 append entries 时, 需要带上几个重要标志和数据:

- term: 当前 leader 的 term
- prevLogIndex : 这一批日志之前的那一条日志的 index
- prevLogTerm: prevLogIndex 的 term
- entries: 要传给 follower 的日志
- leaderCommit: leader 当前的提交位置

prevLogIndex 和 prevLogTerm 主要用于日志的一致性检查, leaderCommit 用于推进 follower 的 commit进度

同时, 在接收到 appendEntry rpc 时,  follower 需要做的事情如下:

- 检查 term 是否比本地的 term 小, 是的话就拒绝
- 检查本地日的 prevLogIndex 的 term 是否和 prevlogTerm 相同, 不同也拒绝
- 检查是否有日志冲突, 也即拥有相同的 logIndex, 但是 term 不同,有的话需要截断日志
- append 日志到本地
- 更新 commitIndex

而 leader 收到 appendEntryResponse 时, 需要做出以下的事情:

- 如果 success = true, 则继续发送日志, 同时更新 follower.matchIndex
- 如果 success = false, 则说明日志一致性检查失败, 则将 follower.nextIndex --, 然后重试

以上就是 raft appendLog 的整体流程, 可以说, 通过一张 figure 图片就阐述了整个流程

<img src="https://gitee.com/zisuu/mypicture/raw/master/image-20211130150107982.png" alt="image-20211130150107982" style="zoom:67%;" />

<img src="https://gitee.com/zisuu/mypicture/raw/master/image-20211130150757005.png" alt="image-20211130150757005" style="zoom:67%;" />

# 三 raft.go 整体布局

复习完 raft 的基本模块后, 接下来, 我们来看看我们要实现的 raft.go

初看 raft.go 肯定会一脸懵:

**1.tick() 是什么东西?**

实际上, 我们要实现的是 tick-based raft, 也即上层的模块每 tick 一次, 就会调用一次 raft.go 中的 tick() 函数, 从而驱动 raft 模块. tick 中主要根据不同的身份调用不同的 tickFunction, 如果是 leader 则是 tickHeartbeat(), 如果 follower 和 candidate 则是 tickElection()

**2.step() 是什么?**

step 方法接受一个 message, 也即, 我们要根据不同的身份和不同的消息类型, 调用不同的方法来处理

比如, leader 会通过 调用 handlePropose 处理 propose 类型的消息

follower 会通过 handleAppendEntries 来处理 appendEntries Rpc

具体的消息类型在 raft.doc 中有介绍.

**3.以下是整体的布局:**

- sending util: 主要负责发送特定的 message

```

/***********************************  1.sending util function  *****************************************/

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
}

// broadcast send append rpc to all followers
func (r *Raft) broadcast() {
}

// sendAppendResponse send append response to the given peer
func (r *Raft) sendAppendResponse(to uint64, term uint64, lastIndex uint64, reject bool) {
}

// sendSnapshot send snapshot to the given peer
func (r *Raft) sendSnapshot(to uint64) bool {

}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {

}

// sendHeartbeatResponse sends a heartbeat response to the given peer.
func (r *Raft) sendHeartbeatResponse(to uint64) {

}

// sendHeartbeatToFollowers sends heartbeat RPC to all followers
func (r *Raft) sendHeartbeatToFollowers() {

}

// sendRequestVote sends a requestVote RPC to the given peer
func (r *Raft) sendRequestVote(to uint64) {

}

// sendRequestVoteResponse sends a requestVote response to the given peer
func (r *Raft) sendRequestVoteResponse(to uint64, term uint64, reject bool) {
}
```



- tick function: 和时钟相关的函数, 不同的身份对应不同的函数(leader 对应 tickHeartbeat, follower 对应 tickElection)

```
/***********************************  2.tick function  *****************************************/

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateLeader:
		r.tickHeartbeat()
	case StateFollower, StateCandidate:
		r.tickElection()
	}
}

func (r *Raft) tickElection() {
}

func (r *Raft) tickHeartbeat() {
}
```



- becomexxx function: 身份变换时调用的函数 , 如 becomeLeader

```
/***********************************  3.becomexxx function  *****************************************/

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
}

```



- stepxxx function: step() 函数是这个类里面最重要的函数, 其主要作用是根据不同类型的 message以及不同的身份, 调用特定的 stepxxx function, 如当前是 leader, 则调用 stepLeader(m) 处理这条消息

```
/***********************************  4.step and stepxxx function *********************************/

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// Leader change
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}

	// Handle advancing message by specific node state
	switch r.State {
	case StateLeader:
		r.stepLeader(m)
	case StateFollower:
		r.stepFollower(m)
	case StateCandidate:
		r.stepCandidate(m)
	}
	return nil
}

// stepFollower Handle message if current node is a follower
func (r *Raft) stepFollower(m pb.Message) {
	switch m.MsgType {
	}
}

// stepCandidate Handle message if current node is a candidate
func (r *Raft) stepCandidate(m pb.Message) {
	switch m.MsgType {
	}
}

// stepLeader Handle message if current node is a leader
func (r *Raft) stepLeader(m pb.Message) {
	switch m.MsgType {
	}
}
```



- handleRequest/response  function: 被 stepxxx() 所调用

```


/***********************************  5.handle request and response  *****************************************/

// handleCampaign a node handleCampaign itself to become a leader
func (r *Raft) handleCampaign() {
	
}

// handleRequestVote handle RequestVote RPC request
func (r *Raft) handleRequestVote(m pb.Message) {
	
}

// handleRequestVoteResponse handle RequestVote response
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	
}

// handlePropose handle propose RPC request
func (r *Raft) handlePropose(m pb.Message) {
	
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
}

// handleAppendResponse handle AppendEntry response
func (r *Raft) handleAppendResponse(m pb.Message) {
	
}

func (r *Raft) boostCommitIndexAndBroadCast() {
	
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

```

# 四 代码

ok, 目前为止, 我们复习了 raft 的基础算法, 同时了解了 raft.go 的整体布局和相关函数

接下来, 让我们开始愉快的 coding 吧~~~~~

## 2AA -- Leader Election

Leader 通过心跳机制和 follower 维护着租约, 如果 Follower 在 rand election timeout 时间内没有收到 来自 leader 的心跳

也即如果 tick() 函数调用次数超过了 randomElectionTimeout , 则会发送一条 mgs_hub 消息到 step(), 代表马上发起一轮新的选举, follower 收到后立即进入 candidate 状态, 并调用 handleCompagin() 函数进行选举

```
func (r *Raft) tickElection() {
   r.electionElapsed++
   if r.electionElapsed >= r.randomElectionTimeout && r.State != StateLeader {
      r.electionElapsed = 0
      _ = r.Step(pb.Message{
         MsgType: pb.MessageType_MsgHup,
         From:    r.id,
         Term:    r.Term,
      })

   }
}
```

### **handle compaign**

follower 在 electiontimeout 后, 进入 candidate 状态:

伪代码:

```
1.become candidate()

2.vote for self

3.if only self, become leader()

4.requestVote = {
	id: id
	term : term
	lastLogIndex: lastLogIndex
	lastLogTerm: lastLogTerm
}
send requestVote to all servers
```

对应 handleCompagin:

```
func (r *Raft) handleCampaign() {
   // Become candidate
   r.becomeCandidate()
   // Vote for self
   r.votes[r.id] = true
   r.Vote = r.id
   // Only self
   if len(r.Prs) == 1 {
      r.becomeLeader()
      return
   }
   // Send requestVote request to all nodes
   for to, _ := range r.Prs {
      if r.id == to {
         continue
      }
      r.sendRequestVote(to)
   }
}
```

### **handle requestVote**

其他的 server 收到 requestVote 请求后, grant or  reject:

伪代码;

```
1.if m.term < term ? reject

2.
// raft 论文: 一个 server 在一个给定的 term 内只能给一个节点投票, 因为一个节点只能投一票, 才能保证只有一个 candidate 收到了半数的投票
if  (vote == none || vote == m.from) and 
m.lastLogTerm > lastLogTerm || (m.lastLogTerm == lastLogTerm && m.lastLogIndex >= lastLogIndex) {
	voteFor = m.id
    accept
}
```

对应 handleRequestVote():

```
func (r *Raft) handleRequestVote(m pb.Message) {
	// 1.Reject lower term
	if m.Term < r.Term {
		r.sendRequestVoteResponse(m.From, r.Term, true)
		return
	}
	// 2.Only vote for one node in one term
	if r.Vote != None && r.Vote != m.From {
		r.sendRequestVoteResponse(m.From, r.Term, true)
		return
	}
	// 3.check completeness
	reject := true
	lastLogIndex, lastLogTerm := m.Index, m.LogTerm
	lastLocalIndex := r.RaftLog.LastIndex()
	lastLocalTerm, _ := r.RaftLog.Term(lastLocalIndex)
	if lastLogTerm > lastLocalTerm || (lastLogTerm == lastLocalTerm && lastLogIndex >= lastLocalIndex) {
		reject = false
		r.Vote = m.From
	}
	r.sendRequestVoteResponse(m.From, r.Term, reject)
}
```

### **handle requestVoteResponse**

candidate 收到 requestVoteResponse 后, becomeLeader or becomeFollower:

```
votes[m.id] = true
统计 granted, rejected 的数量
if granted > majority {
   becomeLeader()
} 
if rejected > majority {
	becomeFollower()
}

becomeLeader() {
	// 初始化 progress (raft 论文, nextIndex 初始化为 lastLogIndex + 1, matchIndex 设置为0, 通过 日志的一致性检查来恢复这两个变量)
	initial progresses = {
		nextIndex = localLastLogIndex + 1
		match = 0
	}
	// 发送一条 no-op log 到其他节点, 来确认自己 leader 的地位
	no-op log = {
		term : term,
		index: lastIndex + 1,
	}
	append no-op log in local
	send log to all servers to establish its authority and precent new elections
	
	update self progress
}
```

对应 handleRequestVoteResponse:

```
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	if m.Term < r.Term || m.Term > r.Term {
		return
	}

	// 1.vote
	r.votes[m.From] = !m.Reject

	// 2.check grant or reject
	granted, rejected := 0, 0
	for i, _ := range r.Prs {
		vote, voted := r.votes[i]
		if !voted {
			continue
		}
		if vote {
			granted++
		} else {
			rejected++
		}
	}

	majority := len(r.Prs) / 2
	if granted > majority {
		r.becomeLeader()
	}
	if rejected > majority {
		r.becomeFollower(m.Term, None)
	}
}
```

### Paper 注意点

1.每个结点初始化的状态为 follower

2.如果一个 结点收到了比本地更大的 term , 其马上退回到 follower 状态, 并等待 leader 的心跳消息

3.每个结点在一个给定的 term 内只会投一票, 并按照先到先来的原则投票

4.如果一个 candidate 收到了一个 appendEntries 消息, 并且其 term >= 本地的 term, 则该 candidate 马上转变为 follower, 并认定这个发送结点为 leader

5.leader election timeout 需要设为一个随机值: rand(et) + et, 从而避免选票瓜分的情况

## 2AB-- Log replication

### handle Propose

当上层的模块要 propose 一些请求时, 会调用 step(), 传入一个 propose 类型的消息, 需要 leader propose 该请求到其他 follower

```
1.append logs in local

2.appendEntriesRequest = {
	id : id,
	preLogIndex: lastLogIndex,
	preLogTerm: lastLogTerm,
	logs: logs[],
	// 需要带上 commitIndex
	committed: lastCommitIndex
}
send to all followers

3.update local progress
match = lastIndex
next = lastIndex + 1
if (len(peers) == 1) {
	commit = lastIndex
}
```

对应handlePropose():

```
func (r *Raft) handlePropose(m pb.Message) {
	entries := m.Entries
	// 1.append to locals
	r.RaftLog.appendEntriesWithTerm(entries, r.Term, &r.PendingConfIndex)

	// 2.broadcast to followers
	r.broadcast()

	// 3.update self progress
	lastIndex := r.RaftLog.LastIndex()
	r.Prs[r.id].Match = lastIndex
	r.Prs[r.id].Next = lastIndex + 1
	if len(r.Prs) == 1 {
		r.RaftLog.committed = lastIndex
	}
}
```



### handle AppendEntry

follower 收到 appendEntry rpc 后:

```

1.if m.term < term, then return false

2.if localStorage[m.preLogIndex].term != m.preLogTerm , then return false

3.handle conflict logs, delete same logIndex but not same term log

4.append log to localStorage

5.update commitIndex = min(leaderCommit, last log entry index)

6.return {
	term: term,
	Index: lastLogIndex
	success: true
}
```

对应 handleAppendEntries:

```
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	r.electionElapsed = 0

	// 1. reject lower term
	if m.Term < r.Term {
		r.sendAppendResponse(m.From, r.Term, r.RaftLog.LastIndex(), true)
		return
	}

	// 1. revert back to follower
	r.Lead = m.GetFrom()

	// 2. check log consistency
	preLogIndex, preLogTerm := m.Index, m.LogTerm
	localTerm, _ := r.RaftLog.Term(preLogIndex)
	if preLogTerm != localTerm {
		r.sendAppendResponse(m.From, r.Term, r.RaftLog.LastIndex(), true)
		return
	}

	// 3.handle log conflicts
	for i, entry := range m.Entries {
		index := entry.Index
		term := entry.Term
		if index < r.RaftLog.firstLogIndex {
			continue
		} else if r.RaftLog.firstLogIndex <= index && index <= r.RaftLog.LastIndex() {
			localLogTerm, _ := r.RaftLog.Term(index)
			if term != localLogTerm {
				// delete from here
				r.RaftLog.deleteEntriesFromIndex(index)
				r.RaftLog.entries = append(r.RaftLog.entries, *entry)
				// update stableIndex
				r.RaftLog.stabled = min(r.RaftLog.stabled, index-1)
			}
		} else {
			r.RaftLog.appendEntries(m.Entries[i:])
			break
		}
	}

	// 4.update commitIndex
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
	}

	// 5.send resp
	r.sendAppendResponse(m.From, r.Term, r.RaftLog.LastIndex(), false)
}

```



### handle AppendEntryResponse

leader 收到 AppendEntryResponse 后, 需要:

```
1. if term < curTerm , return

// 如果日志一致性检查失败, 则直接nextIndex --, 重试
2. if reject
   nextIndex --
   retry from nextIndex
3. if success
   update nextIndex = m.index + 1 	
   match = m.index
   // 推进 commitIndex
   updateCommitIndexAndBroadcast()


updateCommitIndexAndBroadcast():
	for i = commitIndex + 1; i <= lastLogIndex; i ++ {
		1. progress.match >= i ----- > cnt
		
		// 这是raft 论文提到的, 当前的 leader 只能提交当前的term的日志, 才能顺带提交之前的日志
		2. if logTerm == r.Term && cnt > majority, then update commitIndex
	}
	3.if commitIndex change, broadCast to follower with empty appendEntries()	
```

对应 handleAppendResponse:

```
func (r *Raft) handleAppendResponse(m pb.Message) {
	// 1. reject, retry
	reject := m.Reject
	if reject {
		if r.Prs[m.From].Next > 0 {
			r.Prs[m.From].Next--
		}
		r.sendAppend(m.From)
		return
	}

	// 2. accept, update index
	if m.Index > r.Prs[m.From].Match {
		id := m.From
		r.Prs[id].Next = m.Index + 1
		r.Prs[id].Match = m.Index
	}

	// 3.boost commitIndex
	r.boostCommitIndexAndBroadCast()
}


func (r *Raft) boostCommitIndexAndBroadCast() {
	if r.State != StateLeader {
		return
	}
	maxCommitIndex := r.RaftLog.committed
	for i := r.RaftLog.committed + 1; i <= r.RaftLog.LastIndex(); i++ {
		cnt := 0
		for _, pr := range r.Prs {
			if pr.Match >= i {
				cnt++
			}
		}
		term, _ := r.RaftLog.Term(i)
		if r.Term == term && cnt > len(r.Prs)/2 {
			maxCommitIndex = i
		}
	}
	if maxCommitIndex > r.RaftLog.committed {
		r.RaftLog.committed = maxCommitIndex
		// Broadcast to followers to boost commitIndex
		r.broadcast()
	}
}
```





## 2AC -- Implement the raw node interface

任务描述:

> `raft.RawNode` in `raft/rawnode.go` is the interface we interact with the upper application, `raft.RawNode` contains `raft.Raft` and provide some wrapper functions like `RawNode.Tick()`and `RawNode.Step()`. It also provides `RawNode.Propose()` to let the upper application propose new raft logs.
>
> Another important struct `Ready` is also defined here.
>
> When handling messages or advancing the logical clock, the `raft.Raft` may need to interact with the upper application, like:
>
> - send messages to other peers
> - save log entries to stable storage
> - save hard state like the term, commit index, and vote to stable storage
> - apply committed log entries to the state machine
> - etc
>
> But these interactions do not happen immediately, instead, they are encapsulated in `Ready` and returned by `RawNode.Ready()` to the upper application. It is up to the upper application when to call `RawNode.Ready()` and handle it.  After handling the returned `Ready`, the upper application also needs to call some functions like `RawNode.Advance()` to update `raft.Raft`'s internal state like the applied index, stabled log index, etc.
>
> You can run `make project2ac` to test the implementation and run `make project2a` to test the whole part A.

更详细的描述可以看 raft/raft.doc

总结起来就是:

rawNode 是 raft 和 上层应用通信的中介者

在 rawNode 中有一个 ready 结构体, 其中:

- Entries 用于存放需要持久化存储的日志
- CommittedEntries 用于存放需要提交的日志
- Messages 用于存放需要传输给其他结点的日志
- Snapshot 用于存放新创建的快照
- HardState 也即需要持久化存储的状态, 包括 term, vote, commitIndex

```
type Ready struct {
   // The current volatile state of a Node.
   // SoftState will be nil if there is no update.
   // It is not required to consume or store SoftState.
   *SoftState

   // The current state of a Node to be saved to stable storage BEFORE
   // Messages are sent.
   // HardState will be equal to empty state if there is no update.
   pb.HardState

   // Entries specifies entries to be saved to stable storage BEFORE
   // Messages are sent.
   Entries []pb.Entry

   // Snapshot specifies the snapshot to be saved to stable storage.
   Snapshot pb.Snapshot

   // CommittedEntries specifies entries to be committed to a
   // store/state-machine. These have previously been committed to stable
   // store.
   CommittedEntries []pb.Entry

   // Messages specifies outbound messages to be sent AFTER Entries are
   // committed to stable storage.
   // If it contains a MessageType_MsgSnapshot message, the application MUST report back to raft
   // when the snapshot has been received or has failed by calling ReportSnapshot.
   Messages []pb.Message
}
```

所以, 而 rawNode 中要求实现几个函数:

- ready() 返回一个准备就绪的 ready 结构体

```
// Ready returns the current point-in-time state of this RawNode.
func (rn *RawNode) Ready() Ready {
	// Your Code Here (2A).
	raft := rn.Raft
	ready := Ready{
		Entries:          raft.RaftLog.unstableEntries(),
		CommittedEntries: raft.RaftLog.nextEnts(),
		Messages:         rn.Raft.msgs,
	}

	// 如果 hardState 改变了, 则加入 ready
	if !isHardStateEqual(rn.latestHardState, rn.buildHardState()) {
		ready.HardState = rn.buildHardState()
	}

	// 如果 sofaState 改变了, 则加入 ready
	if !isSoftStateEqual(rn.latestSoftState, rn.buildSoftState()) {
		softState := rn.buildSoftState()
		ready.SoftState = &softState
	}

	// 如果快照不为空, 也即加入 ready
	if !IsEmptySnap(raft.RaftLog.pendingSnapshot) {
		ready.Snapshot = *raft.RaftLog.pendingSnapshot
	}

	rn.Raft.msgs = make([]pb.Message, 0)
	rn.Raft.RaftLog.pendingSnapshot = nil
	return ready
}
```



- hasReady() : 判断是否有状态改变了, 一般在 调用 ready() 前进行判断

```


// HasReady called when RawNode user need to check if any Ready pending.
func (rn *RawNode) HasReady() bool {
	// Your Code Here (2A).
	raft := rn.Raft
	if len(raft.RaftLog.unstableEntries()) > 0 {
		return true
	}

	if len(raft.RaftLog.nextEnts()) > 0 {
		return true
	}

	if len(rn.Raft.msgs) > 0 {
		return true
	}

	if !isHardStateEqual(rn.latestHardState, rn.buildHardState()) ||
		!isSoftStateEqual(rn.latestSoftState, rn.buildSoftState()) {
		return true
	}

	if !IsEmptySnap(raft.RaftLog.pendingSnapshot) {
		return true
	}

	return false
}

```



- advance: 用于表示上一次的 ready 已经被成功处理了, 可以改变相应的状态了

```
// Advance notifies the RawNode that the application has applied and saved progress in the
// last Ready results.
func (rn *RawNode) Advance(rd Ready) {
	// Your Code Here (2A).
	if !IsEmptyHardState(rd.HardState) {
		rn.latestHardState = rd.HardState
	}

	if rd.SoftState != nil && !IsEmptySoftState(*rd.SoftState) {
		rn.latestSoftState = *rd.SoftState
	}

	// Update applied index
	if len(rd.CommittedEntries) > 0 {
		rn.Raft.RaftLog.applied = rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
	}

	// Update stable index
	if len(rd.Entries) > 0 {
		rn.Raft.RaftLog.stabled = rd.Entries[len(rd.Entries)-1].Index
	}

	// Compact log when the snapshot is Persistently stored
	rn.Raft.RaftLog.maybeCompact()
}

```

