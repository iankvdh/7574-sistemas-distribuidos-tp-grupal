package clientstore

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/checkpoint"
	"github.com/iankvdh/7574-sistemas-distribuidos-tp-grupal/common/messageprotocol/inner"
)

const fileName = "clients.json"

func Path(dir string) string {
	return filepath.Join(dir, fileName)
}

func Load(dir string) (map[inner.ClientID]struct{}, error) {
	data, err := os.ReadFile(Path(dir))
	if os.IsNotExist(err) {
		return map[inner.ClientID]struct{}{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read client store: %w", err)
	}
	var ids []string
	if err := json.Unmarshal(data, &ids); err != nil {
		return nil, fmt.Errorf("unmarshal client store: %w", err)
	}
	set := make(map[inner.ClientID]struct{}, len(ids))
	for _, id := range ids {
		set[inner.ClientID(id)] = struct{}{}
	}
	return set, nil
}

func Save(dir string, set map[inner.ClientID]struct{}) error {
	ids := make([]string, 0, len(set))
	for id := range set {
		ids = append(ids, string(id))
	}
	sort.Strings(ids)
	data, err := json.Marshal(ids)
	if err != nil {
		return fmt.Errorf("marshal client store: %w", err)
	}
	return checkpoint.AtomicWriteFile(Path(dir), data)
}
