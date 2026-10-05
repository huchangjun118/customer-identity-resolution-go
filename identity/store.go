package identity

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// store 是本地 WAL 持久化：追加事件、启动重放、崩溃恢复。
// 已提交批次与未完成批次通过 batch-commit 记录区分：
// 重放时只有看到 commit 的批次才被应用，崩溃截断的尾部记录被安全跳过，
// 因此不会留下部分写入。
type store struct {
	dir  string
	path string
	mu   sync.Mutex
	f    *os.File
	enc  *json.Encoder
}

// event 是 WAL 中的一条记录。
type event struct {
	Kind    string          `json:"kind"` // batch-begin / signal / batch-commit / merge / undo-merge / split / erase
	Payload json.RawMessage `json:"payload"`
}

func openStore(dir string) (*store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "identity.wal")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &store{dir: dir, path: path, f: f, enc: json.NewEncoder(f)}, nil
}

// append 追加一条事件（写入 OS 缓冲，需配合 sync 落盘）。
func (s *store) append(e event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.enc.Encode(e)
}

// sync 将已追加事件落盘，保证重启后不丢失。
func (s *store) sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.f.Sync()
}

// replay 按序重放全部事件；仅容忍文件末尾因崩溃截断的半行，其余损坏报错以便定位。
func (s *store) replay(fn func(event) error) error {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	lines := bytes.Split(data, []byte("\n"))
	// 去掉末尾空行（最后一条记录以 \n 结尾时产生）。
	if n := len(lines); n > 0 && len(bytes.TrimSpace(lines[n-1])) == 0 {
		lines = lines[:n-1]
	}
	for i, ln := range lines {
		var e event
		if err := json.Unmarshal(ln, &e); err != nil {
			if i == len(lines)-1 {
				break // 崩溃截断的尾部半行，安全忽略
			}
			return fmt.Errorf("identity: wal corrupt at line %d: %w", i+1, err)
		}
		if err := fn(e); err != nil {
			return err
		}
	}
	return nil
}

// close 关闭存储。
func (s *store) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.f.Sync(); err != nil {
		return err
	}
	return s.f.Close()
}
