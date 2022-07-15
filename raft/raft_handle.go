package raft

import (
	"math/rand"
	"sort"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

func (r *Raft) handleMsgBeat(m pb.Message) {
	r.heartbeatElapsed = 0
	for _, peer := range r.peerArray {
		if peer == r.id {
			continue
		}
		r.sendHeartbeat(peer)
	}
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	r.lg.Debugf("handle requestVoteResp")
	// r.voteAnswers++
	if m.Reject == false {
		r.lg.Debugf("Term:%d,%d receive vote from %d", m.Term, r.id, m.GetFrom())
		if r.votes[m.GetFrom()] == false {
			r.vts++
			r.votes[m.GetFrom()] = true
		}
	} else {
		//此处请求目前是不幂等的
		r.votes[m.GetFrom()] = false
	}
	// r.lg.Debugf("Term:%d,%d votes number: %d, peers number: %d", m.Term, r.id, r.vts, len(r.peerArray))
	// r.lg.Debugf("ans num:%d,vts num:%d,peer num:%d", len(r.votes), r.vts, len(r.peerArray))
	if r.vts > len(r.peerArray)/2 {
		r.becomeLeader()
	}
	if len(r.votes)-r.vts > len(r.peerArray)/2 {
		r.becomeFollower(m.Term, None)
	}
}

func (r *Raft) handleRequstVote(m pb.Message) {
	r.lg.Debugf("handle request vote")
	if r.Term > m.Term {
		//TODO:
		// r.msgs = append(r.msgs,pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, To: m.GetFrom(), From: r.id, Term: m.Term, Reject: true})
	} else if r.Term < m.Term && r.Term != 0 {
		r.becomeFollower(m.Term, None)
		voterLogTerm := r.RaftLog.LastLogTerm()
		if voterLogTerm > m.GetLogTerm() {
			r.lg.Debugf("reject vote")
			r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, To: m.GetFrom(), From: r.id, Term: m.Term, Reject: true})
			return
		} else if voterLogTerm == m.GetLogTerm() && r.RaftLog.LastIndex() > m.GetIndex() {
			// r.lg.Debugf("reject vote")
			r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, To: m.GetFrom(), From: r.id, Term: m.Term, Reject: true})
			return
		}

		r.lg.Debugf("Term:%d,%d vote for %d", m.Term, m.GetTo(), m.GetFrom())
		r.Vote = m.GetFrom()
		r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, To: m.GetFrom(), From: r.id, Term: m.Term, Reject: false})
		return
	} else if r.State != StateFollower {
		r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, To: m.GetFrom(), From: r.id, Term: m.Term, Reject: true})
		return
	}
	r.electionElapsed = 0
	r.electionTimeout = int((1 + rand.Float64()) * float64(r.eletimeout))
	if r.Vote == None || r.Vote == m.GetFrom() {

		//比对logterm
		voterLogTerm := r.RaftLog.LastLogTerm()
		// r.lg.Debugf("voter log Term:%d,candidate log term :%d", voterLogTerm, m.GetLogTerm())
		if voterLogTerm > m.GetLogTerm() {
			r.lg.Debugf("reject vote")
			r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, To: m.GetFrom(), From: r.id, Term: m.Term, Reject: true})
			return
		} else if voterLogTerm == m.GetLogTerm() && r.RaftLog.LastIndex() > m.GetIndex() {
			// r.lg.Debugf("reject vote")
			r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, To: m.GetFrom(), From: r.id, Term: m.Term, Reject: true})
			return
		}

		// r.lg.Debugf("Term:%d,%d vote for %d", m.Term, m.GetTo(), m.GetFrom())
		r.Vote = m.GetFrom()
		r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, To: m.GetFrom(), From: r.id, Term: m.Term, Reject: false})
	} else {
		r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, To: m.GetFrom(), From: r.id, Term: m.Term, Reject: true})
	}
}

func (r *Raft) handleAppendResp(m pb.Message) {

	r.lg.Debugf("handleMsgAppendResp,the logIndex:%d", m.GetIndex())

	if m.Reject {
		//TODO:
		if m.Index == None {
			return
		}
		// if m.LogTerm != None {
		// 	logTerm := m.LogTerm
		// 	l := r.RaftLog
		// 	sliceIndex := sort.Search(len(l.entries),
		// 		func(i int) bool { return l.entries[i].Term > logTerm })
		// 	if sliceIndex > 0 && l.entries[sliceIndex-1].Term == logTerm {
		// 		index = l.toEntryIndex(sliceIndex)
		// 	}
		// }
		r.Prs[m.From].Next = m.GetIndex()
		r.sendAppend(m.From)
		return
	}

	if r.Prs[m.GetFrom()].Match < m.GetIndex() {
		r.Prs[m.GetFrom()].Match = m.GetIndex()
		// r.Prs[m.GetFrom()].Next = m.GetIndex() + 1
		// r.lg.Debugf("Pr[%d] next is %d", m.GetFrom(), r.Prs[m.GetFrom()].Next)
	}

	r.Prs[m.GetFrom()].Next = r.Prs[m.GetFrom()].Match + 1

	preIndex := m.GetIndex() - 1
	if preIndex >= 0 && r.RaftLog.entries[preIndex].GetTerm() != r.Term {
		return
	}

	if cap(r.matchBuf) < len(r.peerArray) {
		r.matchBuf = make(uint64Slice, len(r.peerArray))
	}
	r.matchBuf = r.matchBuf[:len(r.peerArray)]
	idx := 0
	for _, p := range r.peerArray {
		r.lg.Debugf("%d peer matches index %d", p, r.Prs[p].Match)
		r.matchBuf[idx] = r.Prs[p].Match
		idx++
	}
	sort.Sort(&r.matchBuf)
	mci := r.matchBuf[len(r.matchBuf)-r.quorum()-1]
	r.lg.Debugf("matchBuf:%d! quorum:%d! mci:%d! idx:%d!", r.matchBuf[len(r.matchBuf)-r.quorum()-1], r.quorum(), mci, idx)
	r.lg.Debugf("finished,Pr[%d] next is %d", m.GetFrom(), r.Prs[m.GetFrom()].Next)
	if r.RaftLog.maybeCommit(mci, r.Term) {
		for _, i := range r.peerArray {
			if i == r.id {
				continue
			}
			r.sendAppend(i)
		}
	}

}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).

	// if m.Term != None && m.Term < r.Term {
	// 	r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgAppendResponse, Reject: true,
	// 		From: r.id, To: m.GetFrom(), Term: r.Term})
	// }

	r.lg.Debugf("%d update term %d to %d", r.id, r.Term, m.Term)
	r.becomeFollower(m.Term, m.GetFrom())

	//TODO: maybe rejected
	if m.GetIndex() > r.RaftLog.LastIndex() {
		r.lg.Debugf("rejected,the Msg index :%d,the Log Index:%d", m.GetIndex(), r.RaftLog.LastIndex())
		r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgAppendResponse, Reject: true,
			From: r.id, To: m.GetFrom(), Term: r.Term, Index: r.RaftLog.LastIndex() + 1})
		return
	}

	//TODO:已持久化,reject
	// if {

	// }

	preIndex := m.GetIndex() - 1
	r.lg.Debugf("Msg Index:%d, LogIndex:%d", m.GetIndex(), r.RaftLog.LastIndex())
	if m.GetIndex() <= r.RaftLog.LastIndex() && m.GetIndex() >= 0 {
		if m.GetIndex() == 0 {
			preIndex = 0
		} else if r.RaftLog.entries[preIndex].GetTerm() > m.GetLogTerm() {
			r.lg.Debugf("Reject! Msg Term:%d, LogTerm:%d", m.GetLogTerm(), r.RaftLog.entries[preIndex].GetTerm())
			//TODO:考虑logterm和index
			r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgAppendResponse, Reject: true,
				From: r.id, To: m.GetFrom(), Term: r.Term, Index: r.RaftLog.LastIndex()})
			return
		}

		r.lg.Debugf("check lastindex:%d", r.RaftLog.LastIndex())
		if r.RaftLog.LastIndex() > 0 {
			r.lg.Debugf("Msg Term:%d, LogTerm:%d,preIndex:%d", m.GetLogTerm(), r.RaftLog.entries[preIndex].GetTerm(), preIndex) //这条log在lastindex=0时会panic
		}

		if r.RaftLog.LastIndex() > 0 && (r.RaftLog.LastIndex() > preIndex+uint64(len(m.GetEntries())) && r.RaftLog.entries[preIndex+uint64(len(m.GetEntries()))].GetTerm() < m.GetLogTerm()) {
			r.lg.Debugf("reject 3")
			r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgAppendResponse, Reject: true,
				From: r.id, To: m.GetFrom(), Term: r.Term, Index: preIndex, LogTerm: r.RaftLog.entries[preIndex].GetTerm()})
			return
		}
	}

	for i, entry := range m.Entries {
		r.lg.Debugf("entry:[%v]", entry)
		if entry.Index > r.RaftLog.LastIndex() {
			n := len(m.Entries)
			for j := i; j < n; j++ {
				r.lg.Debugf("j:%d,append entry index:%d", j, entry.GetIndex())
				r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[j])
			}
			break
		} else {
			logTerm := r.RaftLog.entries[entry.GetIndex()-1].GetTerm()

			if logTerm != entry.Term {
				r.lg.Debugf("do something,the index :%d,the log term:%d,the entry term:%d", entry.GetIndex(), logTerm, entry.Term)
				r.RaftLog.entries[entry.GetIndex()-1] = *entry
				r.RaftLog.entries = r.RaftLog.entries[:entry.GetIndex()]
				// r.lg.Infof("logStabled:%d,entry stabled:%d", r.RaftLog.stabled, entry.Index-1)
				r.RaftLog.stabled = min(r.RaftLog.stabled, entry.Index-1)
			}
		}
	}

	// r.lg.Debugf("Msg commit:%d,Log commit:%d", m.GetIndex(), r.RaftLog.committed)

	if m.GetCommit() > r.RaftLog.committed {
		// r.lg.Debugf("follower raise commit to %d", m.GetCommit())
		r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
	}

	// if m.GetCommit() > r.RaftLog.committed {
	// 	r.lg.Debugf("follower raise commit to %d", m.GetCommit())
	// 	r.RaftLog.committed = m.GetCommit()
	// }

	// if m.GetIndex() > r.RaftLog.stabled {
	// 	r.RaftLog.stabled = m.GetIndex()
	// }
	// r.lg.Debugf("Msg commit:%d,Log commit:%d,finished", m.GetIndex(), r.RaftLog.committed)

	r.lg.Debugf("the %d peer Raftlog is [%v] now", r.id, r.RaftLog.entries)

	//reply

	r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgAppendResponse, Reject: false,
		From: r.id, To: m.GetFrom(), Term: r.Term, Index: r.RaftLog.LastIndex()})
	r.lg.Debugf("handleAppend finished")
}

func (r *Raft) handleMsgPropose(m pb.Message) {
	r.lg.Debugf("handleMsgPropose")
	if r.State != StateLeader {
		r.lg.Debugf("Msg refer to leader %d", r.Lead)
		msg := pb.Message{MsgType: m.GetMsgType(), To: r.Lead, From: r.id, Entries: m.GetEntries()}
		r.msgs = append(r.msgs, msg)
		r.lg.Debugf("Msg [%v] refer to leader", msg)
		return
	}
	ents := make([]pb.Entry, 0)
	//这里的设计是，对于数组的末尾下标LastIndex，不存放数据，数组下标i处的entry的index为i+1
	for k, v := range m.Entries {
		v.Index = r.RaftLog.LastIndex() + uint64(k) + 1
		v.Term = r.Term
		r.lg.Debugf("The %dth log entry data is [%s]", v.Index, string(v.Data))
		if v.EntryType == pb.EntryType_EntryConfChange {
			//TODO:
		}
		ents = append(ents, *v)
	}
	r.RaftLog.entries = append(r.RaftLog.entries, ents...)
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1
	if r.quorum() == 0 {
		r.singleLeaderCommit(m.GetIndex(), ents)
		return
	}
	for _, i := range r.peerArray {
		if i == r.id {
			continue
		}
		r.sendAppend(i)
	}
	r.lg.Debugf("handleMsgPropose successfully,the last index is %d", r.RaftLog.LastIndex())
}

func (r *Raft) singleLeaderCommit(index uint64, ents []pb.Entry) {
	r.lg.Debugf("handleMsgPropose successfully,the last index is %d", r.RaftLog.LastIndex())
	r.RaftLog.maybeCommit(r.RaftLog.committed+uint64(len(ents)), r.Term)
	return
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.electionElapsed = int((1 + rand.Float64()) * float64(r.eletimeout))
	r.electionElapsed = 0
	r.RaftLog.committed = m.GetCommit()
	r.lg.Debugf("%d update term %d to %d", r.id, r.Term, m.Term)
	r.becomeFollower(m.Term, m.GetFrom())
	r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgHeartbeatResponse, Term: r.Term, To: r.Lead, From: r.id})
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {

}
