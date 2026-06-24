package bully

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"time"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

const noLeader byte = 0xFF

type Config struct {
	SelfID              byte
	Peers               []Peer
	LeaderCheckInterval time.Duration // how often to check the heartbeat map
	LeaderTimeout       time.Duration // how long without a heartbeat before triggering election
	OKTimeout           time.Duration
	CoordTimeout        time.Duration
	BaseJitter          time.Duration
}

type Control interface {
	Send(peerAddr string, msg SentinelMessage) error
}

type BullyCoordinator struct {
	cfg           Config
	control       Control
	peersByID     map[byte]Peer
	mu            sync.RWMutex
	state         State
	leaderID      byte
	electMu       sync.Mutex
	okReceived    chan struct{}
	coordReceived chan struct{}
	jitterFn      func() time.Duration

	peerMu       sync.RWMutex
	peerLastSeen map[byte]time.Time
}

func NewBullyCoordinator(cfg Config, control Control) *BullyCoordinator {
	byID := make(map[byte]Peer, len(cfg.Peers))
	for _, p := range cfg.Peers {
		byID[p.ID] = p
	}
	bc := &BullyCoordinator{
		cfg:           cfg,
		control:       control,
		peersByID:     byID,
		state:         Follower,
		leaderID:      noLeader,
		okReceived:    make(chan struct{}, 1),
		coordReceived: make(chan struct{}, 1),
		peerLastSeen:  make(map[byte]time.Time),
	}
	bc.jitterFn = func() time.Duration {
		if cfg.BaseJitter <= 0 {
			return 0
		}
		return time.Duration(int64(cfg.BaseJitter) * int64(cfg.SelfID+1) / int64(len(cfg.Peers)+2))
	}
	return bc
}

func (bc *BullyCoordinator) IsLeader() bool {
	bc.mu.RLock()
	defer bc.mu.RUnlock()
	return bc.state == Leader
}

func (bc *BullyCoordinator) LeaderID() byte {
	bc.mu.RLock()
	defer bc.mu.RUnlock()
	return bc.leaderID
}

func (bc *BullyCoordinator) RecordPeerHeartbeat(peerID byte) {
	bc.peerMu.Lock()
	bc.peerLastSeen[peerID] = time.Now()
	bc.peerMu.Unlock()
}

func (bc *BullyCoordinator) PeersLastSeen() map[byte]time.Time {
	bc.peerMu.RLock()
	defer bc.peerMu.RUnlock()
	out := make(map[byte]time.Time, len(bc.peerLastSeen))
	for k, v := range bc.peerLastSeen {
		out[k] = v
	}
	return out
}

func (bc *BullyCoordinator) EmitPeerHeartbeats(ctx context.Context, sender HeartbeatSender, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, peer := range bc.cfg.Peers {
				if err := sender.SendHeartbeat(peer.UDPAddr, bc.cfg.SelfID); err != nil {
					slog.Debug("bully: failed to send heartbeat to peer", "peer", peer.ID, "err", err)
				}
			}
		}
	}
}

func (bc *BullyCoordinator) Run(ctx context.Context) {
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(bc.jitterFn()):
			bc.triggerElection()
		}
	}()
	bc.monitorLeader(ctx)
}

func (bc *BullyCoordinator) monitorLeader(ctx context.Context) {
	ticker := time.NewTicker(bc.cfg.LeaderCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			bc.mu.RLock()
			state, leader := bc.state, bc.leaderID
			bc.mu.RUnlock()
			if state == Leader || leader == noLeader {
				continue
			}
			bc.peerMu.RLock()
			lastSeen, seen := bc.peerLastSeen[leader]
			bc.peerMu.RUnlock()
			if seen && time.Since(lastSeen) <= bc.cfg.LeaderTimeout {
				continue
			}
			slog.Info("bully: leader heartbeat timeout, starting election", "leader", leader)
			bc.triggerElection()
		}
	}
}

func (bc *BullyCoordinator) Handle(msg SentinelMessage) {
	switch msg.Type {
	case MsgElection:
		if msg.SenderID < bc.cfg.SelfID {
			if peer, ok := bc.peersByID[msg.SenderID]; ok {
				_ = bc.control.Send(peer.TCPAddr, SentinelMessage{MsgOK, bc.cfg.SelfID})
			}
			go bc.triggerElection()
		}
	case MsgOK:
		signal(bc.okReceived)
	case MsgCoord:
		bc.onCoord(msg.SenderID)
	}
}

func (bc *BullyCoordinator) onCoord(sender byte) {
	if sender < bc.cfg.SelfID {
		slog.Info("bully: Coord from a lower ID, re-contesting leadership", "from", sender)
		go bc.triggerElection()
		return
	}
	bc.RecordPeerHeartbeat(sender)
	bc.mu.Lock()
	bc.leaderID = sender
	bc.state = Follower
	bc.mu.Unlock()
	slog.Info("bully: new leader registered", "leader", sender)
	signal(bc.coordReceived)
}

func (bc *BullyCoordinator) triggerElection() {
	if !bc.electMu.TryLock() {
		return // an election is already running
	}
	defer bc.electMu.Unlock()
	bc.runElection()
}

func (bc *BullyCoordinator) runElection() {
	for {
		bc.setState(Candidate)
		higher := bc.peersWithHigherID()
		if len(higher) == 0 {
			bc.becomeLeader()
			return
		}
		drain(bc.okReceived)
		drain(bc.coordReceived)
		for _, p := range higher {
			_ = bc.control.Send(p.TCPAddr, SentinelMessage{MsgElection, bc.cfg.SelfID})
		}
		select {
		case <-bc.okReceived:
		case <-time.After(bc.cfg.OKTimeout):
			bc.becomeLeader()
			return
		}
		select {
		case <-bc.coordReceived:
			return
		case <-time.After(bc.cfg.CoordTimeout):
			slog.Info("bully: no Coord arrived after OK, retrying election")
		}
	}
}

func (bc *BullyCoordinator) becomeLeader() {
	bc.mu.Lock()
	bc.state = Leader
	bc.leaderID = bc.cfg.SelfID
	bc.mu.Unlock()
	for _, p := range bc.cfg.Peers {
		_ = bc.control.Send(p.TCPAddr, SentinelMessage{MsgCoord, bc.cfg.SelfID})
	}
	slog.Info("bully: proclaiming myself leader")
}

func (bc *BullyCoordinator) setState(s State) {
	bc.mu.Lock()
	bc.state = s
	bc.mu.Unlock()
}

func (bc *BullyCoordinator) peersWithHigherID() []Peer {
	var higher []Peer
	for _, p := range bc.cfg.Peers {
		if p.ID > bc.cfg.SelfID {
			higher = append(higher, p)
		}
	}
	sort.Slice(higher, func(i, j int) bool { return higher[i].ID < higher[j].ID })
	return higher
}

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func drain(ch chan struct{}) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}
