package checkpoint

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
)

const checkpointFileMode = 0o644
const checkpointFileFlags = os.O_CREATE | os.O_TRUNC | os.O_WRONLY

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
	Version       int                  `json:"v"`
	ClientID      string               `json:"cid"`
	StrategyState []byte               `json:"ss,omitempty"`
	RingStates    map[string]RingEntry `json:"rs,omitempty"`
	JACStates     map[string]JACEntry  `json:"js,omitempty"`
	LastRecvSeqID map[string]uint64    `json:"lr,omitempty"` // "stageType:replicaID" → last seen SeqID
	OutSeqID      uint64               `json:"os"`
	OutCounts     map[string]uint64    `json:"oc,omitempty"` // "outputIndex|routingKey" → messages published
}

type MetaCheckpoint struct {
	Version    int              `json:"v"`
	Tombstones map[string]int64 `json:"tb"` // clientID → timestamp of abort/expiry
}

const checkpointVersion = 1
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

	path := ClientCheckpointPath(dir, inner.ClientID(cp.ClientID))
	tmp := path + ".tmp"

	f, err := os.OpenFile(tmp, checkpointFileFlags, checkpointFileMode)
	if err != nil {
		return fmt.Errorf("open tmp checkpoint: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("write checkpoint: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("fsync checkpoint content: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close checkpoint: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename checkpoint: %w", err)
	}
	// fsync the directory to durably persist the rename.
	dirFD, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open dir for fsync: %w", err)
	}
	defer dirFD.Close()
	if err := dirFD.Sync(); err != nil {
		return fmt.Errorf("fsync dir: %w", err)
	}
	return nil
}

func ReadClientCheckpoint(path string) (*ClientCheckpoint, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read checkpoint %s: %w", path, err)
	}
	var cp ClientCheckpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, fmt.Errorf("unmarshal checkpoint %s: %w", path, err)
	}
	return &cp, nil
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
	path := filepath.Join(dir, metaFileName)
	tmp := path + ".tmp"

	f, err := os.OpenFile(tmp, checkpointFileFlags, checkpointFileMode)
	if err != nil {
		return fmt.Errorf("open tmp meta: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("write meta: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("fsync meta content: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close meta: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename meta: %w", err)
	}
	dirFD, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open dir for meta fsync: %w", err)
	}
	defer dirFD.Close()
	return dirFD.Sync()
}
