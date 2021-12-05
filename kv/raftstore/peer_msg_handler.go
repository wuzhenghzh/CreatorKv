package raftstore

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"reflect"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) getProposal(index uint64, expectedTerm uint64) (*proposal, error) {
	pr := d.getAndRemoveProposal(index)
	if pr == nil {
		return nil, errors.New("Failed to get pr")
	}
	if pr.term != expectedTerm {
		NotifyStaleReq(expectedTerm, pr.cb)
		return nil, errors.New("Stale request")
	}
	return pr, nil
}

func (d *peerMsgHandler) appendProposal(cb *message.Callback) {
	d.proposals = append(d.proposals, &proposal{
		term:  d.Term(),
		index: d.nextProposalIndex(),
		cb:    cb,
	})
}

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
		d.applyChangesToBadger(kvWB, ready.CommittedEntries[len(ready.CommittedEntries)-1].Index)
	}

	// Advance
	d.RaftGroup.Advance(ready)
}

// applyCommittedEntry apply committed entry to state machine, include data request and admin request
func (d *peerMsgHandler) applyCommittedEntry(entry eraftpb.Entry, kvWB *engine_util.WriteBatch) *engine_util.WriteBatch {
	if entry.Data == nil {
		return kvWB
	}
	msg, err := d.unMarshalRequest(entry)
	if err != nil {
		return kvWB
	}

	// Apply data request
	if len(msg.Requests) > 0 {
		return d.applyDataRequest(&msg, &entry, kvWB)
	}

	// Apply admin request
	if msg.AdminRequest != nil {
		return d.applyAdminRequest(&msg, &entry, kvWB)
	}
	return kvWB
}

func (d *peerMsgHandler) applyDataRequest(msg *raft_cmdpb.RaftCmdRequest, entry *eraftpb.Entry, kvWB *engine_util.WriteBatch) *engine_util.WriteBatch {
	request := msg.Requests[0]

	// Check key in region's range
	key := parseRequestKey(request)
	if key != nil {
		if err := util.CheckKeyInRegion(key, d.Region()); err != nil {
			pr, _ := d.getProposal(entry.Index, entry.Term)
			if pr != nil {
				pr.cb.Done(ErrResp(err))
			}
			return kvWB
		}
	}

	// Apply modified changes
	switch request.GetCmdType() {
	case raft_cmdpb.CmdType_Put:
		putRequest := request.GetPut()
		kvWB.SetCF(putRequest.GetCf(), putRequest.GetKey(), putRequest.GetValue())
	case raft_cmdpb.CmdType_Delete:
		deleteRequest := request.GetDelete()
		kvWB.DeleteCF(deleteRequest.GetCf(), deleteRequest.GetKey())
	}

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
		{
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Delete,
				Delete:  &raft_cmdpb.DeleteResponse{},
			})
		}
	// Apply get
	case raft_cmdpb.CmdType_Get:
		{
			kvWB = d.applyChangesToBadger(kvWB, entry.Index)
			getRequest := request.Get
			data, _ := engine_util.GetCF(d.peerStorage.Engines.Kv, getRequest.Cf, getRequest.Key)
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Get,
				Get: &raft_cmdpb.GetResponse{
					Value: data,
				},
			})
		}
	// Apply scan
	case raft_cmdpb.CmdType_Snap:
		{
			// If the region was split or merge, the region epoch change
			if d.Region().RegionEpoch.Version != msg.Header.RegionEpoch.Version {
				pr.cb.Done(ErrResp(&util.ErrEpochNotMatch{}))
				return kvWB
			}

			kvWB = d.applyChangesToBadger(kvWB, entry.Index)
			pr.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Snap,
				Snap: &raft_cmdpb.SnapResponse{
					Region: d.Region(),
				},
			})
		}
	}

	// Call back
	pr.cb.Done(resp)
	return kvWB
}

func (d *peerMsgHandler) applyAdminRequest(msg *raft_cmdpb.RaftCmdRequest, entry *eraftpb.Entry, kvWB *engine_util.WriteBatch) *engine_util.WriteBatch {
	request := msg.AdminRequest
	switch request.CmdType {
	// Compact log
	case raft_cmdpb.AdminCmdType_CompactLog:
		{
			d.handleCompactLogRequest(request.CompactLog, kvWB)
		}
	// Change peer
	case raft_cmdpb.AdminCmdType_ChangePeer:
		{
			d.handleChangePeerRequest(entry, msg, request.ChangePeer, kvWB)
		}
	// Split region
	case raft_cmdpb.AdminCmdType_Split:
		{
			d.handleSplitRegion(entry, msg, request.Split, kvWB)
		}
	}
	return kvWB
}

// handleSplitRegion Split the region into two regions
func (d *peerMsgHandler) handleSplitRegion(entry *eraftpb.Entry, msg *raft_cmdpb.RaftCmdRequest, req *raft_cmdpb.SplitRequest, kvWB *engine_util.WriteBatch) {
	// 0.Check region epoch and whether key was in region
	err := util.CheckRegionEpoch(msg, d.Region(), true)
	if err == nil {
		err = util.CheckKeyInRegion(req.SplitKey, d.Region())
	}
	if err != nil {
		pr, _ := d.getProposal(entry.Index, entry.Term)
		if pr != nil {
			pr.cb.Done(ErrResp(err))
		}
		return
	}

	// 1. Split region
	originRegion := proto.Clone(d.Region()).(*metapb.Region)
	originRegion.RegionEpoch.Version++
	newRegion := copyRegionInfo(originRegion, req)
	originRegion.EndKey = req.SplitKey

	// 2 Create new peer
	d.createNewPeerAndStart(newRegion)

	// 3. Update global meta
	m := d.ctx.storeMeta
	m.Lock()
	m.regionRanges.Delete(&regionItem{region: d.Region()})
	m.regions[originRegion.Id] = originRegion
	m.regions[newRegion.Id] = newRegion
	m.regionRanges.ReplaceOrInsert(&regionItem{region: originRegion})
	m.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})
	d.peerStorage.SetRegion(originRegion)
	m.Unlock()

	// 4. Write region state
	meta.WriteRegionState(kvWB, originRegion, rspb.PeerState_Normal)
	meta.WriteRegionState(kvWB, newRegion, rspb.PeerState_Normal)
	d.SizeDiffHint = 0
	d.ApproximateSize = new(uint64)

	// 5. Send response
	pr, _ := d.getProposal(entry.Index, entry.Term)
	if pr != nil {
		resp := newCmdResp()
		resp.AdminResponse = &raft_cmdpb.AdminResponse{
			CmdType: raft_cmdpb.AdminCmdType_Split,
			Split: &raft_cmdpb.SplitResponse{
				Regions: []*metapb.Region{originRegion, newRegion},
			},
		}
		pr.cb.Done(resp)
	}

	d.sendRegionHeartBeat()
}

// handleCompactLogRequest update truncated state, schedule gc task to raftLog-gc worker
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

// handleChangePeerRequest change region's peer(add/remove)
func (d *peerMsgHandler) handleChangePeerRequest(entry *eraftpb.Entry, msg *raft_cmdpb.RaftCmdRequest, req *raft_cmdpb.ChangePeerRequest, kvWB *engine_util.WriteBatch) {
	// Check region epoch
	err := util.CheckRegionEpoch(msg, d.Region(), true)
	if err != nil {
		pr, _ := d.getProposal(entry.Index, entry.Term)
		if pr != nil {
			pr.cb.Done(ErrResp(err))
		}
		return
	}

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

	// Send response
	pr, _ := d.getProposal(entry.Index, entry.Term)
	if pr != nil {
		resp := newCmdResp()
		resp.AdminResponse = &raft_cmdpb.AdminResponse{
			CmdType: raft_cmdpb.AdminCmdType_ChangePeer,
			ChangePeer: &raft_cmdpb.ChangePeerResponse{
				Region: d.peerStorage.region,
			},
		}
		pr.cb.Done(resp)
	}

	if d.IsLeader() {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
}

func (d *peerMsgHandler) createNewPeerAndStart(newRegion *metapb.Region) {
	peer, _ := createPeer(d.ctx.store.Id, d.ctx.cfg, d.ctx.schedulerTaskSender, d.ctx.engine, newRegion)
	d.ctx.router.register(peer)
	d.ctx.router.send(newRegion.Id, message.Msg{
		Type: message.MsgTypeStart,
	})
}

func (d *peerMsgHandler) sendRegionHeartBeat() {
	if d.IsLeader() {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
}

func (d *peerMsgHandler) unMarshalRequest(entry eraftpb.Entry) (raft_cmdpb.RaftCmdRequest, error) {
	msg := raft_cmdpb.RaftCmdRequest{}
	if entry.EntryType == eraftpb.EntryType_EntryConfChange {
		cc := eraftpb.ConfChange{}
		err := cc.Unmarshal(entry.Data)
		if err != nil {
			log.Errorf("Error when decode conf change msg: %s", err.Error())
			return msg, errors.Trace(err)
		}
		err = msg.Unmarshal(cc.Context)
		if err != nil {
			log.Errorf("Error when decode raft cmd msg: %s", err.Error())
			return msg, errors.Trace(err)
		}
	} else {
		err := msg.Unmarshal(entry.Data)
		if err != nil {
			log.Errorf("Error when decode raft cmd msg: %s", err.Error())
			return msg, errors.Trace(err)
		}
	}
	return msg, nil
}

func (d *peerMsgHandler) applyChangesToBadger(kvWB *engine_util.WriteBatch, index uint64) *engine_util.WriteBatch {
	d.persistApplyState(kvWB, index)
	d.writeChangesToKvDB(kvWB)
	return new(engine_util.WriteBatch)
}

func (d *peerMsgHandler) persistApplyState(kvWB *engine_util.WriteBatch, index uint64) {
	d.peerStorage.applyState.AppliedIndex = index
	err := kvWB.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
	if err != nil {
		log.Errorf("Error when set applyState meta: %s", err.Error())
	}
}

func (d *peerMsgHandler) persistTruncateState(kvWB *engine_util.WriteBatch, index, term uint64) {
	applyState := d.peerStorage.applyState
	applyState.TruncatedState.Index = index
	applyState.TruncatedState.Term = term
	err := kvWB.SetMeta(meta.ApplyStateKey(d.regionId), applyState)
	if err != nil {
		log.Errorf("Error when set applyState meta: %s", err.Error())
	}
}

func (d *peerMsgHandler) writeChangesToKvDB(kvWB *engine_util.WriteBatch) {
	err := kvWB.WriteToDB(d.peerStorage.Engines.Kv)
	if err != nil {
		log.Errorf("Error happens when write changes to kv badger, regionId: %d, %s", d.regionId, err.Error())
	}
}

// HandleMsg handle upper application msgs
func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	// The message transported between raft peers
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	// The message received from client, need to be proposed
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	// Boost raft
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

// proposeRaftCommand propose raft cmd request to raft module, include data request and admin request
func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	// Propose message
	if len(msg.Requests) > 0 {
		d.proposeDataRequest(msg, cb)
	}

	// Propose admin request
	if msg.AdminRequest != nil {
		d.proposeAdminRequest(msg, cb)
	}
}

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

func (d *peerMsgHandler) proposeAdminRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	adminRequest := msg.AdminRequest
	switch adminRequest.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		{
			// Propose request directly
			d.proposeDataRequest(msg, cb)
		}
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
	case raft_cmdpb.AdminCmdType_Split:
		{
			key := msg.AdminRequest.Split.SplitKey
			if err := util.CheckKeyInRegion(key, d.Region()); err != nil {
				cb.Done(ErrResp(err))
				return
			}
			d.proposeDataRequest(msg, cb)
		}
	}
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	// Tick raft module
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	// Compact raft log
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	// Send region heartbeat to scheduler
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	// Check split region
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

/// Checks if the message is sent to the correct peer.
///
/// Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func copyRegionInfo(originRegion *metapb.Region, req *raft_cmdpb.SplitRequest) *metapb.Region {
	var newPeers []*metapb.Peer
	newPeerIds := req.NewPeerIds
	for i, p := range originRegion.Peers {
		newPeers = append(newPeers, &metapb.Peer{
			Id:      newPeerIds[i],
			StoreId: p.StoreId,
		})
	}
	newRegion := &metapb.Region{
		Id:       req.NewRegionId,
		StartKey: req.SplitKey,
		EndKey:   originRegion.EndKey,
		Peers:    newPeers,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
	}
	return newRegion
}

func parseRequestKey(req *raft_cmdpb.Request) []byte {
	var key []byte
	switch req.CmdType {
	case raft_cmdpb.CmdType_Get:
		key = req.Get.Key
	case raft_cmdpb.CmdType_Put:
		key = req.Put.Key
	case raft_cmdpb.CmdType_Delete:
		key = req.Delete.Key
	}
	return key
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
