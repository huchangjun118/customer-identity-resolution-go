package identity

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// store 是本地 WAL：JSONL 事件日志。
// 一批事件先整体写入、fsync，再写入 commit 标记并 fsync；
// 恢复时只重放以 commit 标记结尾的完整批次，
// 未完成的批次（崩溃留下的部分写入）被识别并丢弃。
type store struct {
	mu   sync.Mutex
	path string
	f    *os.File
	enc  *json.Encoder
}

func openStore(dir string) (*store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("identity: create data dir: %w", err)
	}
	path := filepath.Join(dir, "events.log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("identity: open wal: %w", err)
	}
	return &store{path: path, f: f, enc: json.NewEncoder(f)}, nil
}

// appendBatch 原子地写入一批事件（events 中最后一条须为 EvCommit 标记）。
func (s *store) appendBatch(events []Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ev := range events {
		if err := s.enc.Encode(&ev); err != nil {
			return fmt.Errorf("identity: wal encode: %w", err)
		}
	}
	if err := s.f.Sync(); err != nil {
		return fmt.Errorf("identity: wal sync: %w", err)
	}
	return nil
}

// replay 按序读出所有已提交批次的事件；未提交的部分批次被跳过。
func (s *store) replay() ([]Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := os.Open(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	var committed []Event
	var pending []Event
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	line := 0
	for sc.Scan() {
		line++
		var ev Event
		if err := json.Unmarshal(sc.Bytes(), &ev); err != nil {
			// 尾部半行（崩溃时部分写入）：停止重放，之前的已提交批次不受影响。
			break
		}
		if ev.Kind == EvCommit {
			committed = append(committed, pending...)
			committed = append(committed, ev)
			pending = pending[:0]
			continue
		}
		pending = append(pending, ev)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("identity: wal replay: %w", err)
	}
	// pending 中剩余的即为未完成批次，区分并丢弃（不留部分写入）。
	return committed, nil
}

// close 关闭存储。
func (s *store) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.f.Close()
}
