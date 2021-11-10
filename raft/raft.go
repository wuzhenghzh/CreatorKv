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
	"github.com/pingcap-incubator/tinykv/log"
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

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	// Tick func(electionTick, heartbeatTick)
	tickFunc func()

	// Step func(stepLeader, stepFollower, stepCandidate)
	stepFunc func(m pb.Message)
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
		Vote:             hardState.GetVote(),
		votes:            map[uint64]bool{},
		Prs:              map[uint64]*Progress{},
		RaftLog:          newLog(c.Storage),
	}

	if raft.Term == 0 {
		raft.Term = raft.RaftLog.LastTerm()
	}
	raft.electionTimeout += rand.Intn(raft.electionTimeout)

	// Init prs
	if c.peers == nil {
		c.peers = confState.Nodes
	}
	for _, peer := range c.peers {
		if peer == raft.id {
			raft.Prs[peer] = &Progress{
				Match: raft.RaftLog.LastIndex(),
				Next:  raft.RaftLog.LastIndex() + 1,
			}
		} else {
			raft.Prs[peer] = &Progress{
				Match: 0,
				Next:  1,
			}
		}
	}
	// Init raft log
	raft.RaftLog.committed = hardState.Commit
	raft.RaftLog.applied = c.Applied
	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).

	return false
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

// sendHeartbeatToFollowers sends heartbeat RPC to all followers
func (r *Raft) sendHeartbeatToFollowers() {
	for i, _ := range r.Prs {
		if i != r.id {
			r.sendHeartbeat(i)
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

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.tickFunc()
}

func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.electionElapsed >= r.electionTimeout && r.State != StateLeader {
		r.electionElapsed = 0
		err := r.Step(pb.Message{
			MsgType: pb.MessageType_MsgHup,
			From:    r.id,
			Term:    r.Term,
		})
		if err != nil {
			log.Errorf("Error happens when start a election, node : %d, term : %d, err: %s", r.id, r.Term, err.Error())
		}
	}
}

func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout && r.State == StateLeader {
		r.heartbeatElapsed = 0
		err := r.Step(pb.Message{
			MsgType: pb.MessageType_MsgBeat,
			From:    r.id,
			Term:    r.Term,
		})
		if err != nil {
			log.Errorf("Error happens when send heartBeat, node : %d, term : %d, err: %s", r.id, r.Term, err.Error())
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	if term >= r.Term {
		r.resetNode()

		r.State = StateFollower
		r.stepFunc = r.stepFollower
		r.tickFunc = r.tickElection
		r.Term = term
		r.Lead = lead
		r.electionTimeout = rand.Intn(r.electionTimeout) + r.electionTimeout
	}
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	// Become candidate means let term = term + 1 and vote for self
	// And after election timeout, try to elect self become leader
	r.resetNode()

	r.State = StateCandidate
	r.stepFunc = r.stepCandidate
	r.tickFunc = r.tickElection
	r.Term++
	r.electionTimeout = rand.Intn(r.electionTimeout) + r.electionTimeout
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.resetNode()

	r.State = StateLeader
	r.stepFunc = r.stepLeader
	r.tickFunc = r.tickHeartbeat
	r.Lead = r.id

	// update all progresses
	for _, progress := range r.Prs {
		progress.Match = r.RaftLog.LastIndex()
		progress.Next = r.RaftLog.LastIndex() + 1
	}
	// send a noop log(see raft.doc about MsgPropose)
	err := r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    r.id,
		Term:    r.Term,
	})
	if err != nil {
		log.Errorf("Error happens when send noop log, node : %d, term : %d, err: %s", r.id, r.Term, err.Error())
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// Leader change
	if m.Term > r.Term {
		log.Infof("Node %d received a message with higher term, become follower, current term : %d, message term : %d", r.Term, m.Term)
		r.becomeFollower(m.Term, None)
	}

	// Prevent stale log entries, just ignore
	if m.Term < r.Term {
		resp := pb.Message{
			From: r.id,
			To:   m.From,
			Term: r.Term,
		}
		if m.MsgType == pb.MessageType_MsgRequestVote {
			resp.MsgType = pb.MessageType_MsgRequestVoteResponse
			resp.Reject = true
		} else if m.MsgType == pb.MessageType_MsgHeartbeat {
			resp.MsgType = pb.MessageType_MsgHeartbeatResponse
		}
		r.sendMessage(resp)
		return nil
	}

	// Handle advancing message by specific node state
	r.stepFunc(m)
	return nil
}

// campaign a node campaign itself to become a leader
func (r *Raft) campaign() {
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
	for i, _ := range r.Prs {
		if r.id == i {
			continue
		}
		r.sendRequestVote(i)
	}
}

// stepFollower Handle message if current node is a follower
func (r *Raft) stepFollower(m pb.Message) {
	switch m.MsgType {
	// election timeout, try a new election
	case pb.MessageType_MsgHup:
		{
			r.campaign()
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
			r.handleRequestVoteRequest(m)
		}
	}
}

// stepCandidate Handle message if current node is a candidate
func (r *Raft) stepCandidate(m pb.Message) {
	switch m.MsgType {
	// election timeout, try a new election, call method campaign()
	case pb.MessageType_MsgHup:
		{
			r.campaign()
		}
	// Message append, Reverts back to follower
	case pb.MessageType_MsgAppend:
		{
			r.handleAppendEntries(m)
		}
	// Request vote request
	case pb.MessageType_MsgRequestVote:
		{
			r.handleRequestVoteRequest(m)
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
	}
}

// stepLeader Handle message if current node is a leader
func (r *Raft) stepLeader(m pb.Message) {
	switch m.MsgType {
	// heartbeat timeout, send heartbeat message to followers
	case pb.MessageType_MsgBeat:
		{
			r.sendHeartbeatToFollowers()
		}
	case pb.MessageType_MsgHeartbeatResponse:
		{

		}
	// no-op log
	case pb.MessageType_MsgPropose:
		{
			r.handlePropose(m)
		}
	// Append entries response
	case pb.MessageType_MsgAppendResponse:
		{
			r.handleAppendResponse(m)
		}
	// Request vote request
	case pb.MessageType_MsgRequestVote:
		{
			r.handleRequestVoteRequest(m)
		}
	// Install snapshot
	case pb.MessageType_MsgSnapshot:
		{

		}
	}
}

// handleRequestVoteRequest handle RequestVote RPC request
func (r *Raft) handleRequestVoteRequest(m pb.Message) {
	reject := true
	if m.Term > r.Term || (m.LogTerm == r.Term && m.Index > r.RaftLog.LastIndex()) {
		reject = false
		r.Vote = m.From
	}
	resp := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  reject,
	}
	r.sendMessage(resp)
}

// handleRequestVoteResponse handle RequestVote response
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	if m.Term < r.Term {
		return
	}
	r.votes[m.From] = !m.Reject
	quorum := (len(r.Prs) + 1) / 2
	granted, rejected := 0, 0
	for i := 1; i <= len(r.Prs); i++ {
		accept, voted := r.votes[uint64(i)]
		if !voted {
			continue
		}
		if accept {
			granted++
		} else {
			rejected++
		}
	}
	if granted >= quorum {
		r.becomeLeader()
	} else if rejected >= quorum {
		r.becomeFollower(m.Term, None)
	}
}

// handlePropose handle propose RPC request
func (r *Raft) handlePropose(m pb.Message) {

}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleAppendResponse handle AppendEntry response
func (r *Raft) handleAppendResponse(m pb.Message) {

}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
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

func (r *Raft) resetNode() {
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.votes = make(map[uint64]bool, 0)
	r.Vote = None
	r.Lead = None
}

func (r *Raft) sendMessage(m pb.Message) {
	r.msgs = append(r.msgs, m)
}
