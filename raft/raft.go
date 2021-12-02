// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// rand et timeout, in the interval of [electionTimeout, electionTimeout + rand(electionTimeout)]
	randomElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// number of ticks since it reached last leaderTransfer request.
	// todo: hwo to set the limit timeout?
	leaderTransferElapsed int

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}

	// Your Code Here (2A).
	hardState, confState, _ := c.Storage.InitialState()
	raft := &Raft{
		id:               c.ID,
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		votes:            map[uint64]bool{},
		Prs:              map[uint64]*Progress{},
		RaftLog:          newLog(c.Storage),
	}

	if raft.Term == 0 {
		raft.Term = raft.RaftLog.LastTerm()
	}
	raft.resetRandElectionTimeout()

	// Init prs
	if c.peers == nil {
		c.peers = confState.Nodes
	}
	for _, peer := range c.peers {
		if peer == raft.id {
			raft.Prs[raft.id] = &Progress{
				Match: raft.RaftLog.LastIndex(),
				Next:  raft.RaftLog.LastIndex() + 1,
			}
		} else {
			raft.Prs[peer] = &Progress{
				Match: 0,
				Next:  raft.RaftLog.LastIndex() + 1,
			}
		}
	}
	// Init raft log
	raft.RaftLog.committed = hardState.Commit
	raft.RaftLog.applied = c.Applied
	return raft
}

/***********************************  1.sending util function  *****************************************/

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	nextIndex := r.Prs[to].Next
	var preLogIndex uint64
	if nextIndex > 0 {
		preLogIndex = nextIndex - 1
	} else {
		preLogIndex = 0
	}
	preLogTerm, err := r.RaftLog.Term(preLogIndex)
	if err != nil {
		r.sendSnapshot(to)
		return false
	}
	entries := r.RaftLog.getEntriesFromIndex(nextIndex)
	r.sendMessage(pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		LogTerm: preLogTerm,
		Index:   preLogIndex,
		Entries: entries,
		Commit:  r.RaftLog.committed,
	})
	return true
}

// broadcast send append rpc to all followers
func (r *Raft) broadcast() {
	for to, _ := range r.Prs {
		if to != r.id {
			r.sendAppend(to)
		}
	}
}

// sendAppendResponse send append response to the given peer
func (r *Raft) sendAppendResponse(to uint64, term uint64, lastIndex uint64, reject bool) {
	resp := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      to,
		Term:    term,
		Index:   lastIndex,
		Reject:  reject,
	}
	r.sendMessage(resp)
}

// sendSnapshot send snapshot to the given peer
func (r *Raft) sendSnapshot(to uint64) bool {
	// Your Code Here (2A).
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

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	lastCommittedIndex := min(r.RaftLog.committed, r.Prs[to].Match)
	m := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  lastCommittedIndex,
	}
	r.sendMessage(m)
}

// sendHeartbeatResponse sends a heartbeat response to the given peer.
func (r *Raft) sendHeartbeatResponse(to uint64) {
	// Your Code Here (2A).
	m := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}
	r.sendMessage(m)
}

// sendHeartbeatToFollowers sends heartbeat RPC to all followers
func (r *Raft) sendHeartbeatToFollowers() {
	for to, _ := range r.Prs {
		if to != r.id {
			r.sendHeartbeat(to)
		}
	}
}

// sendRequestVote sends a requestVote RPC to the given peer
func (r *Raft) sendRequestVote(to uint64) {
	m := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		LogTerm: r.RaftLog.LastTerm(),
		Index:   r.RaftLog.LastIndex(),
	}
	r.sendMessage(m)
}

// sendRequestVoteResponse sends a requestVote response to the given peer
func (r *Raft) sendRequestVoteResponse(to uint64, term uint64, reject bool) {
	resp := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      to,
		Term:    term,
		Reject:  reject,
	}
	r.sendMessage(resp)
}

func (r *Raft) sendTimeoutNowRequest(to uint64, term uint64) {
	m := pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
		From:    r.id,
		To:      to,
		Term:    term,
	}
	r.sendMessage(m)
}

/***********************************  2.tick function  *****************************************/

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateLeader:
		r.tickHeartbeat()
		if r.leadTransferee != None {
			r.leaderTransferElapsed++
			// If timeout, stop transfer
		}
	case StateFollower, StateCandidate:
		r.tickElection()
	}
}

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

func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout && r.State == StateLeader {
		r.heartbeatElapsed = 0
		_ = r.Step(pb.Message{
			MsgType: pb.MessageType_MsgBeat,
			From:    r.id,
			Term:    r.Term,
		})
	}
}

/***********************************  3.becomexxx function  *****************************************/
func (r *Raft) resetRandElectionTimeout() {
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	if term >= r.Term {
		r.resetNode()

		r.State = StateFollower
		r.Term = term
		r.Lead = lead
		r.resetRandElectionTimeout()
	}
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	// Become candidate means let term = term + 1 and vote for self
	// And after election timeout, try to elect self become leader
	r.resetNode()

	r.State = StateCandidate
	r.Term++
	r.resetRandElectionTimeout()
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.resetNode()

	r.State = StateLeader
	r.Lead = r.id

	// Update all progresses
	for _, progress := range r.Prs {
		progress.Match = 0
		progress.Next = r.RaftLog.LastIndex() + 1
	}

	// Send a noop log to followers to establish its authority and prevent new elections
	noopLog := &pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Term:      r.Term,
		Index:     r.RaftLog.LastIndex() + 1,
	}
	r.RaftLog.appendEntries([]*pb.Entry{noopLog})
	r.broadcast()

	// Update self progress
	lastIndex := r.RaftLog.LastIndex()
	r.Prs[r.id].Next = lastIndex + 1
	r.Prs[r.id].Match = lastIndex
	if len(r.Prs) == 1 {
		r.RaftLog.committed = lastIndex
	}

}

/***********************************  4.step and stepxxx function  *****************************************/

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
	// election timeout, try a new election
	case pb.MessageType_MsgHup:
		{
			r.handleCampaign()
		}
	// Msg append request
	case pb.MessageType_MsgAppend:
		{
			r.handleAppendEntries(m)
		}
	// Heartbeat request
	case pb.MessageType_MsgHeartbeat:
		{
			r.handleHeartbeat(m)
		}
	// Snapshot request
	case pb.MessageType_MsgSnapshot:
		{
			r.handleSnapshot(m)
		}
	// Request vote request
	case pb.MessageType_MsgRequestVote:
		{
			r.handleRequestVote(m)
		}
	// Timeout now request
	case pb.MessageType_MsgTimeoutNow:
		{
			r.handleTimeoutNowRequest(m)
		}
	// Leader transfer, send to leader
	case pb.MessageType_MsgTransferLeader:
		{
			if r.Lead != None {
				m.To = r.Lead
				r.sendMessage(m)
			}
		}
	}
}

// stepCandidate Handle message if current node is a candidate
func (r *Raft) stepCandidate(m pb.Message) {
	switch m.MsgType {
	// election timeout, try a new election, call method handleCampaign()
	case pb.MessageType_MsgHup:
		{
			r.handleCampaign()
		}
	// Message append, Revert back to follower
	case pb.MessageType_MsgAppend:
		{
			if m.Term >= r.Term {
				r.becomeFollower(m.Term, m.From)
			}
			r.handleAppendEntries(m)
		}
	// Request vote request
	case pb.MessageType_MsgRequestVote:
		{
			r.handleRequestVote(m)
		}
	// Request vote response, Check whether receive majority votes
	case pb.MessageType_MsgRequestVoteResponse:
		{
			r.handleRequestVoteResponse(m)
		}
	// Heartbeat message
	case pb.MessageType_MsgHeartbeat:
		{
			r.handleHeartbeat(m)
		}
	// Snapshot request
	case pb.MessageType_MsgSnapshot:
		{
			r.handleSnapshot(m)
		}
	// Timeout now request
	case pb.MessageType_MsgTimeoutNow:
		{
			r.handleTimeoutNowRequest(m)
		}
	}
}

// stepLeader Handle message if current node is a leader
func (r *Raft) stepLeader(m pb.Message) {
	switch m.MsgType {
	// Heartbeat timeout, send heartbeat message to followers
	case pb.MessageType_MsgBeat:
		{
			r.sendHeartbeatToFollowers()
		}
	// Handle heart beat response, if leader receive, it indicate that
	// the follower doesn't have update-to-date log
	case pb.MessageType_MsgHeartbeatResponse:
		{
			r.sendAppend(m.From)
		}
	// no-op log
	case pb.MessageType_MsgPropose:
		{
			r.handlePropose(m)
		}
	// Message append, Revert back to follower
	case pb.MessageType_MsgAppend:
		{
			r.handleAppendEntries(m)
		}
	// Append entries response
	case pb.MessageType_MsgAppendResponse:
		{
			r.handleAppendResponse(m)
		}
	// Request vote request
	case pb.MessageType_MsgRequestVote:
		{
			r.handleRequestVote(m)
		}
	// Snapshot request
	case pb.MessageType_MsgSnapshot:
		{
			r.handleSnapshot(m)
		}
	// Transfer leader
	case pb.MessageType_MsgTransferLeader:
		{
			r.handleTransferLeader(m)
		}
	}
}

/***********************************  5.handle request and response  *****************************************/

// handleCampaign a node handleCampaign itself to become a leader
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

// handleRequestVote handle RequestVote RPC request
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

// handleRequestVoteResponse handle RequestVote response
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

// handlePropose handle propose RPC request
func (r *Raft) handlePropose(m pb.Message) {
	// If exist leadTransferee, reject propose
	if r.leadTransferee != None {
		return
	}

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

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	r.electionElapsed = 0

	// 1. reject lower term
	if m.Term < r.Term {
		r.sendAppendResponse(m.From, r.Term, r.RaftLog.LastIndex(), true)
		return
	}

	// 2. revert back to follower
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

// handleAppendResponse handle AppendEntry response
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

	// 4.if leadTransferee == from, and log is matched
	if r.leadTransferee == m.From {
		if r.Prs[m.From].Match >= r.RaftLog.LastIndex() {
			r.sendTimeoutNowRequest(m.From, r.Term)
			r.leadTransferee = None
		}
	}
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

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		r.sendHeartbeatResponse(m.From)
		return
	}

	// If term is upper then local, become follower
	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
	}

	// Update
	r.electionElapsed = 0
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
	}

	r.sendHeartbeatResponse(m.From)
}

// handleSnapshot handle Snapshot RPC request
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

// handleTransferLeader transfer leadership to target server
func (r *Raft) handleTransferLeader(m pb.Message) {
	targetId := m.From
	if _, existed := r.Prs[targetId]; !existed {
		return
	}
	if m.From == r.id {
		return
	}

	r.leadTransferee = targetId
	// If up to date, send TimeoutNow request
	if r.Prs[targetId].Match >= r.RaftLog.LastIndex() {
		r.sendTimeoutNowRequest(targetId, r.Term)
	} else {
		// Append log to follower, wait catch up
		r.leaderTransferElapsed = 0
		r.sendAppend(targetId)
	}
}

// handleTimeoutNowRequest transfer leadership to self, start new election immediately
func (r *Raft) handleTimeoutNowRequest(m pb.Message) {
	_, existed := r.Prs[r.id]
	if !existed {
		return
	}

	// Start new election
	r.handleCampaign()
}

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

func (r *Raft) resetNode() {
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.leaderTransferElapsed = 0
	r.leadTransferee = None
	r.votes = make(map[uint64]bool, 0)
	r.Vote = None
	r.Lead = None
}

func (r *Raft) sendMessage(m pb.Message) {
	r.msgs = append(r.msgs, m)
}
