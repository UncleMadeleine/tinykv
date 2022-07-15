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
	"os"

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
	//TODO:2AB
	lg *log.Logger
	// lastIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	//TODO:2AB
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}

	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}

	ent, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		panic(err)
	}
	model := &RaftLog{
		storage: storage,
		entries: ent,
		stabled: lastIndex,
		lg:      NewStoragelg(LOG_BOOL), //改动此处决定是否输出日志
	}
	return model
}

func NewStoragelg(st int) *log.Logger {
	writer2 := os.Stdout
	if st == 0 {
		writer2 = nil
	}
	w := log.NewLogger(writer2, "Storage:")

	w.SetLevel(log.LOG_LEVEL_DEBUG)
	w.SetHighlighting(true)
	return w
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	//TODO:2AB
	l.lg.Debugf("check unstableEntries")
	l.lg.Debugf("stabled:%d,lastIndex:%d", l.stabled, l.LastIndex())
	if l.stabled > l.LastIndex() {
		l.lg.Debugf("Err!!! The stabled is more than lastIndex")
		return make([]pb.Entry, 0)
	}
	res := l.entries[l.stabled:]
	return res
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	//TODO:2AB
	l.lg.Debugf("check nextEntries")
	l.lg.Debugf("the applied is %d, and the commited is %d", l.applied, l.committed)
	if l.committed == l.applied {
		return make([]pb.Entry, 0)
	}
	return l.entries[l.applied:l.committed]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	//TODO:2AB
	// l.lg.Debugf("check lastIndex:%d", len(l.entries))
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].GetIndex()
}

func (l *RaftLog) LastLogTerm() uint64 {
	if l.LastIndex() > 0 {
		return l.entries[l.LastIndex()-1].GetTerm()
	}
	return 0
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	//TODO:2AB
	l.lg.Debugf("return Term %d", i)
	if i <= 0 {
		term, err := l.storage.Term(i)
		return term, err
	}
	if i <= l.LastIndex() {
		return l.entries[i-1].Term, nil
	}
	return 0, errors.New("The index is higher than the length of entries")
}

func (l *RaftLog) maybeCommit(maxIndex, term uint64) bool {
	l.lg.Debugf("Max Index:%d, Commited Index:%d", maxIndex, l.committed)
	if maxIndex > l.committed {
		l.committed = maxIndex
		return true
	}
	return false
}

func (l *RaftLog) getEntries(left, right uint64) []pb.Entry {
	if left < 0 {
		l.lg.Debugf("get entries failed,the left number is %d", left)
		return make([]pb.Entry, 0)
	}
	if right > l.LastIndex() {
		l.lg.Debugf("get entries failed,the right number is %d", right)
		return make([]pb.Entry, 0)
	}
	if left >= right {
		return make([]pb.Entry, 0)
	}
	return l.entries[left:right]
}
