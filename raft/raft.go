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
	"math/rand"
	"os"
	"sync"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

const LOG_BOOL int = 0 //决定是否输出日志

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

	//2AA
	peerArray   []uint64
	mtx         *sync.RWMutex
	lg          *log.Logger
	vts         int //得票数
	voteAnswers int //得到投票答复数
	eletimeout  int //常数，不用随机化

	//2AB
	matchBuf uint64Slice
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).

	hardState, _, _ := c.Storage.InitialState()

	model := &Raft{
		id:               c.ID,
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
		Prs:              make(map[uint64]*Progress),
		Lead:             None,
		State:            StateFollower,
		peerArray:        c.peers,
		mtx:              &sync.RWMutex{},
		votes:            make(map[uint64]bool),
		Vote:             hardState.Vote,
		lg:               Newlg(LOG_BOOL), //改动此处决定是否输出日志
		eletimeout:       c.ElectionTick,
		Term:             hardState.Term,
		// leadTransferee:   0,

		RaftLog: newLog(c.Storage),
	}
	lastIndex := model.RaftLog.LastIndex()
	for _, i := range c.peers {
		model.Prs[i] = &Progress{Match: 0, Next: 1}
		if i == model.id {
			model.Prs[i] = &Progress{Match: lastIndex, Next: lastIndex + 1}
		}
	}

	return model
}

func Newlg(st int) *log.Logger {
	writer2 := os.Stdout
	if st == 0 {
		writer2 = nil
	}
	w := log.NewLogger(writer2, "Project2A:")

	w.SetLevel(log.LOG_LEVEL_DEBUG)
	w.SetHighlighting(true)
	return w
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	r.lg.Debugf("Send Append!")

	preIndex := r.Prs[to].Next - 1

	//TODO:需判断m.term是否在任期内

	var ents []*pb.Entry

	r.lg.Debugf("The first entry index:%d,The last entry index:%d", preIndex, r.RaftLog.LastIndex())

	for _, v := range r.RaftLog.getEntries(preIndex, r.RaftLog.LastIndex()) {
		// r.lg.Debugf("/// v:[%v] ///", v)
		// r.lg.Debugf("/// ents:[%v] ///", ents)
		tmp := v //go的for循环底层需要这个临时变量避免bug
		ents = append(ents, &tmp)
		// r.lg.Debugf("/// ents:[%v] ///", ents)
	}

	var logTerm uint64
	if len(ents) > 0 {
		logTerm = ents[len(ents)-1].GetTerm()
	} else {
		logTerm = 0
	}

	r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgAppend, Term: r.Term,
		LogTerm: logTerm, From: r.id, To: to, Entries: ents, Index: preIndex,
		Commit: r.RaftLog.committed})

	r.lg.Debugf("Send Append [%v],Successfully,index:%d", ents, preIndex)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).)
	r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgHeartbeat, Term: r.Term,
		From: r.id, To: to,
		Commit: r.RaftLog.committed})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.electionElapsed++
	switch r.State {
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			for _, peer := range r.peerArray {
				if peer == r.id {
					continue
				}
				r.sendHeartbeat(peer)
			}
		}
	case StateCandidate:
		// r.lg.Debug("time:", r.electionElapsed, " timeout: ", r.electionTimeout)
		if r.electionElapsed >= r.electionTimeout {
			r.becomeCandidate()
			for _, peer := range r.peerArray {
				if peer == r.id {
					continue
				}
				r.lg.Debugf("%d peer candidate log term %d", r.id, r.RaftLog.LastLogTerm())
				r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgRequestVote, From: r.id, To: peer, Term: r.Term, Index: r.RaftLog.LastIndex(),
					LogTerm: r.RaftLog.LastLogTerm()})
			}
		}
	case StateFollower:
		// r.lg.Debug("time:", r.electionElapsed, " timeout: ", r.electionTimeout)
		if r.electionElapsed >= r.electionTimeout {
			r.becomeCandidate()
			for _, peer := range r.peerArray {
				if peer == r.id {
					continue
				}
				r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgRequestVote, From: r.id, To: peer, Term: r.Term,
					Index: r.RaftLog.LastIndex(), LogTerm: r.RaftLog.LastLogTerm()})
			}
			// r.lg.Debug("become Candidate: ", r.State.String())
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.msgs = make([]pb.Message, 0)
	r.State = StateFollower
	r.electionElapsed = 0
	r.electionTimeout = int((1 + rand.Float64()) * float64(r.eletimeout))
	r.heartbeatElapsed = 0
	r.Term = term
	r.Lead = lead
	r.Vote = 0
	r.vts = 0
	r.votes = make(map[uint64]bool)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.lg.Debugf("%d has become a candidate in Term %d", r.id, r.Term)
	r.msgs = make([]pb.Message, 0)
	r.votes = make(map[uint64]bool)
	r.State = StateCandidate
	r.Term++
	r.votes[r.id] = true
	r.Vote = r.id
	r.electionElapsed = 0
	r.electionTimeout = int((1 + rand.Float64()) * float64(r.eletimeout))
	r.vts = 1
	r.voteAnswers = 1
	r.heartbeatElapsed = 0
	if len(r.peerArray) == 1 {
		r.becomeLeader()
		return
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.msgs = make([]pb.Message, 0)
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.State = StateLeader
	r.Lead = r.id
	r.Vote = 0
	r.vts = 0
	r.votes = make(map[uint64]bool)

	lastIndex := r.RaftLog.LastIndex()
	for peer := range r.Prs {
		if peer == r.id {
			r.Prs[peer].Next = lastIndex + 2
			r.Prs[peer].Match = lastIndex + 1
		} else {
			// r.Prs[peer].Next = lastIndex + 1
			// r.lg.Debugf("%d peer nextIndex:%d", peer, r.Prs[peer].Next)
		}
	}

	ent := pb.Entry{
		Term:      r.Term,
		Data:      nil,
		EntryType: pb.EntryType_EntryNormal,
		Index:     r.RaftLog.LastIndex() + 1,
	}

	r.RaftLog.entries = append(r.RaftLog.entries, ent)
	if r.quorum() == 0 {
		r.singleLeaderCommit(r.RaftLog.LastIndex()-1, []pb.Entry{ent})
		return
	}

	for _, peer := range r.peerArray {
		if peer == r.id {
			continue
		}
		r.sendAppend(peer)
	}

}

var allowedMsgMap = map[pb.MessageType]interface{}{
	pb.MessageType_MsgHup:  nil,
	pb.MessageType_MsgBeat: nil,
	// pb.MessageType_MsgAppendResponse: nil,
	pb.MessageType_MsgPropose: nil,
	// pb.MessageType_MsgRequestVote: nil,
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).

	if r.Term > m.Term {
		//特判有无可能是local message
		if _, ok := allowedMsgMap[m.MsgType]; ok != true {
			r.lg.Debugf("ignore:r.term > m.term, msgType: %s", m.MsgType)
			return nil
		}
	}

	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			for _, peer := range r.peerArray {
				if peer == r.id {
					continue
				}
				r.lg.Debugf("%d peer candidate log term %d", r.id, r.RaftLog.LastLogTerm())
				r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgRequestVote, From: r.id, To: peer, Term: r.Term, Index: r.RaftLog.LastIndex(),
					LogTerm: r.RaftLog.LastLogTerm()})
			}
		case pb.MessageType_MsgRequestVote:
			r.handleRequstVote(m)
		case pb.MessageType_MsgPropose:
			r.handleMsgPropose(m)
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleRequestVoteResponse(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequstVote(m)
		case pb.MessageType_MsgHup:
			r.becomeCandidate()
			for _, peer := range r.peerArray {
				if peer == r.id {
					continue
				}
				r.lg.Debugf("%d peer candidate log term %d", r.id, r.RaftLog.LastLogTerm())
				r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgRequestVote, From: r.id, To: peer, Term: r.Term, Index: r.RaftLog.LastIndex(),
					LogTerm: r.RaftLog.LastLogTerm()})
			}
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVote:
			r.handleRequstVote(m)
		case pb.MessageType_MsgHeartbeatResponse:
			r.handleHeartbeatResponse(m)
		case pb.MessageType_MsgBeat:
			r.handleMsgBeat(m)
		case pb.MessageType_MsgPropose:
			r.handleMsgPropose(m)
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendResp(m)
		}
	}
	return nil
}

func (r *Raft) quorum() int {
	return len(r.peerArray) / 2
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
