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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	firstLogIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		log.Errorf("Error happens when get firstLogIndex from storage, %s", err.Error())
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		log.Errorf("Error happens when get lastLogIndex from storage, %s", err.Error())
	}
	entries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		log.Errorf("Error happens when get log entries[%d, %d] from storage, %s", firstIndex, lastIndex+1, err.Error())
	}
	return &RaftLog{
		storage:       storage,
		committed:     firstIndex - 1,
		applied:       firstIndex - 1,
		firstLogIndex: firstIndex,
		stabled:       lastIndex,
		entries:       entries,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	firstUnStabledIndex, _ := l.storage.FirstIndex()
	for len(l.entries) > 0 {
		if l.firstLogIndex >= firstUnStabledIndex {
			break
		}
		// Delete first log
		l.entries = l.entries[1:]
		if len(l.entries) == 0 {
			l.firstLogIndex = 0
		} else {
			l.firstLogIndex = l.entries[0].Index
		}
	}
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	beginIndex := l.stabled - l.firstLogIndex + 1
	if beginIndex <= uint64(len(l.entries)) {
		return l.entries[beginIndex:]
	}
	return []pb.Entry{}
}

func (l *RaftLog) getEntriesFromIndex(index uint64) []*pb.Entry {
	var entries []*pb.Entry
	for _, entry := range l.entries {
		if entry.Index >= index {
			entries = append(entries, &pb.Entry{
				EntryType: entry.EntryType,
				Term:      entry.Term,
				Index:     entry.Index,
				Data:      entry.Data,
			})
		}
	}
	return entries
}

func (l *RaftLog) deleteEntriesFromIndex(index uint64) {
	for i, v := range l.entries {
		if v.Index == index {
			l.entries = l.entries[:i]
		}
	}
	lastLogIndex := l.LastIndex()
	l.committed = min(l.committed, lastLogIndex)
	l.applied = min(l.applied, lastLogIndex)
	l.stabled = min(l.stabled, lastLogIndex)
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	var result []pb.Entry
	for _, entry := range l.entries {
		if entry.Index > l.applied && entry.Index <= l.committed {
			result = append(result, entry)
		}
	}
	return result
}

func (l *RaftLog) FirstIndex() uint64 {
	return l.firstLogIndex
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if !IsEmptySnap(l.pendingSnapshot) {
		return l.pendingSnapshot.Metadata.Index
	}
	logLen := len(l.entries)
	if logLen >= 1 {
		return l.entries[logLen-1].Index
	}
	lastIndex, _ := l.storage.LastIndex()
	return lastIndex
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if !IsEmptySnap(l.pendingSnapshot) {
		if i == l.pendingSnapshot.Metadata.GetIndex() {
			return l.pendingSnapshot.Metadata.GetTerm(), nil
		}
	}
	for _, entry := range l.entries {
		if entry.Index == i {
			return entry.Term, nil
		}
	}
	return l.storage.Term(i)
}

// LastTerm return the last term of the log entries
func (l *RaftLog) LastTerm() uint64 {
	term, _ := l.Term(l.LastIndex())
	return term
}

// appendEntries with their own term and index
func (l *RaftLog) appendEntries(entries []*pb.Entry) {
	for _, entry := range entries {
		l.entries = append(l.entries, pb.Entry{
			EntryType: entry.EntryType,
			Term:      entry.Term,
			Index:     entry.Index,
			Data:      entry.Data,
		})
	}
}

// appendEntriesWithTerm with target term
func (l *RaftLog) appendEntriesWithTerm(entries []*pb.Entry, term uint64, pendingConfIndex *uint64) {
	for _, entry := range entries {
		if *pendingConfIndex == None {
			*pendingConfIndex = entry.Index
		} else {
			continue
		}
		l.entries = append(l.entries, pb.Entry{
			EntryType: entry.EntryType,
			Term:      term,
			Index:     l.LastIndex() + 1,
			Data:      entry.Data,
		})
	}
}

func (l *RaftLog) resetAllIndex(index uint64) {
	l.committed = index
	l.applied = index
	l.stabled = index
	l.firstLogIndex = index + 1
}
