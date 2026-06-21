package checkpoint

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/dedup"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
)

const checkpointFileMode = 0o644
const checkpointFileFlags = os.O_CREATE | os.O_TRUNC | os.O_WRONLY

func atomicWriteFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, checkpointFileFlags, checkpointFileMode)
	if err != nil {
		return fmt.Errorf("open tmp %s: %w", tmp, err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("write %s: %w", tmp, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("fsync content %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename %s: %w", tmp, err)
	}
	dirFD, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open dir for fsync: %w", err)
	}
	defer dirFD.Close()
	return dirFD.Sync()
}

func GlobalStatePath(dir string) string {
	return filepath.Join(dir, "global_state.json")
}

func WriteGlobalState(dir string, data []byte) error {
	return atomicWriteFile(GlobalStatePath(dir), data)
}

func ReadGlobalState(dir string) ([]byte, error) {
	data, err := os.ReadFile(GlobalStatePath(dir))
	if os.IsNotExist(err) {
		return nil, nil
	}
	return data, err
}

type RingEntry struct {
	IsInitiator     bool   `json:"i"`
	UpstreamTotal   uint32 `json:"t"`
	Phase           int    `json:"p"` // 0=collecting, 1=closing
	LocalMatched    uint64 `json:"lm"`
	LocalNotMatched uint64 `json:"lnm"`
	CachedEOF       []byte `json:"e,omitempty"`
}

type JACEntry struct {
	ReceivedFrom map[string]uint32 `json:"r"` // "stageType:replicaID" → Total received
	Expected     int               `json:"e"`
}

type ClientCheckpoint struct {
	Version        int                           `json:"v"`
	ClientID       string                        `json:"cid"`
	StrategyState  []byte                        `json:"ss,omitempty"`
	RingStates     map[string]RingEntry          `json:"rs,omitempty"`
	JACStates      map[string]JACEntry           `json:"js,omitempty"`
	LastRecvSeqID  map[string]uint64             `json:"lr,omitempty"`
	DedupSeen      map[string]*dedup.IntervalSet `json:"dd,omitempty"`
	ProcessedItems uint64                        `json:"pi,omitempty"`
	WALGen             uint64            `json:"wg,omitempty"`
	OutSeqID           uint64            `json:"os"`
	OutCounts          map[string]uint64 `json:"oc,omitempty"` // "outputIndex|routingKey" → messages published
	PendingEOFBody     []byte            `json:"peof,omitempty"`
	PendingEOFInputIdx int               `json:"peofidx,omitempty"`
}

type MetaCheckpoint struct {
	Version    int              `json:"v"`
	Tombstones map[string]int64 `json:"tb"` // clientID → timestamp of abort/expiry
}

const checkpointVersion = 2
const metaFileName = "meta.json"

func ClientCheckpointPath(dir string, clientID inner.ClientID) string {
	return filepath.Join(dir, "client_"+string(clientID)+".json")
}

func WriteClientCheckpoint(dir string, cp *ClientCheckpoint) error {
	cp.Version = checkpointVersion
	data, err := json.Marshal(cp)
	if err != nil {
		return fmt.Errorf("marshal checkpoint for %s: %w", cp.ClientID, err)
	}
	return atomicWriteFile(ClientCheckpointPath(dir, inner.ClientID(cp.ClientID)), data)
}

func ReadClientCheckpoint(path string) (*ClientCheckpoint, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read checkpoint %s: %w", path, err)
	}
	var clientCheckpoint ClientCheckpoint
	if err := json.Unmarshal(data, &clientCheckpoint); err != nil {
		return nil, fmt.Errorf("unmarshal checkpoint %s: %w", path, err)
	}
	return &clientCheckpoint, nil
}

func ReadMetaCheckpoint(dir string) (*MetaCheckpoint, error) {
	path := filepath.Join(dir, metaFileName)
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return &MetaCheckpoint{Tombstones: map[string]int64{}}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read meta: %w", err)
	}
	var meta MetaCheckpoint
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("unmarshal meta: %w", err)
	}
	if meta.Tombstones == nil {
		meta.Tombstones = map[string]int64{}
	}
	return &meta, nil
}

func WriteMetaCheckpoint(dir string, meta *MetaCheckpoint) error {
	meta.Version = checkpointVersion
	data, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshal meta: %w", err)
	}
	return atomicWriteFile(filepath.Join(dir, metaFileName), data)
}
