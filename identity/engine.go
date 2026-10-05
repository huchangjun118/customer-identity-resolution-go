package identity

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrNotFound     = errors.New("identity: not found")
	ErrPermission   = errors.New("identity: permission denied")
	ErrLimit        = errors.New("identity: limit exceeded")
	ErrBatchTimeout = errors.New("identity: batch processing time exceeded")
)

// snapshot 是某一提交点的不可变只读视图。
// 查询只读快照（无锁），慢查询不会阻塞信号接入。
type snapshot struct {
	clusters map[string]Cluster
	profiles map[string]Profile
}

// Engine 是身份归并引擎。采集与查询并发安全。
type Engine struct {
	cfg Config
	st  *store

	mu     sync.Mutex // 写路径串行化（接入/合并/撤销/拆分/删除）
	cur    *state
	events []Event // 已提交事件（历史时点查询重放用）
	seq    uint64
	batch  uint64

	snap atomic.Pointer[snapshot]

	auditMu sync.Mutex
	audit   []AuditEntry

	quarMu sync.Mutex
	quar   []RejectedSignal
}

// Open 打开（或创建）引擎，重放 WAL 恢复已提交状态；
// 未提交的部分批次被识别并丢弃，不会重复应用。
func Open(cfg Config) (*Engine, error) {
	st, err := openStore(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	e := &Engine{cfg: cfg, st: st, cur: newState()}
	committed, err := st.replay()
	if err != nil {
		st.close()
		return nil, err
	}
	for _, ev := range committed {
		if ev.Kind == EvCommit {
			continue
		}
		e.cur.apply(ev)
		e.events = append(e.events, ev)
		if ev.Seq > e.seq {
			e.seq = ev.Seq
		}
		if ev.BatchID > e.batch {
			e.batch = ev.BatchID
		}
	}
	e.cur.finalize()
	e.publish()
	return e, nil
}

// Close 关闭引擎。
func (e *Engine) Close() error { return e.st.close() }

// allow 检查 actor 是否拥有指定权限范围。
func (e *Engine) allow(actor, scope string) bool {
	for _, s := range e.cfg.Actors[actor] {
		if s == scope || s == "*" {
			return true
		}
	}
	return false
}

func (e *Engine) addAudit(actor, action, detail string, allowed bool) {
	e.auditMu.Lock()
	defer e.auditMu.Unlock()
	e.audit = append(e.audit, AuditEntry{
		Seq:     atomic.LoadUint64(&e.seq),
		At:      time.Now(),
		Actor:   actor,
		Action:  action,
		Detail:  detail,
		Allowed: allowed,
	})
}

// commit 是写路径的唯一出口：先写 WAL（含 commit 标记并 fsync），
// 再换入新状态并发布快照。WAL 失败则状态不变，不留部分写入。
// 调用方须持有 e.mu。
func (e *Engine) commit(trial *state, events []Event) error {
	e.batch++
	batchID := e.batch
	for i := range events {
		seq := atomic.AddUint64(&e.seq, 1)
		events[i].Seq = seq
		events[i].BatchID = batchID
	}
	all := append(append([]Event(nil), events...), Event{Kind: EvCommit, BatchID: batchID})
	if err := e.st.appendBatch(all); err != nil {
		return err
	}
	e.cur = trial
	e.events = append(e.events, events...)
	e.publish()
	return nil
}

// publish 从当前状态构建不可变快照并原子发布。
// 调用方须持有 e.mu（或处于 Open 恢复阶段）。
func (e *Engine) publish() {
	s := &snapshot{
		clusters: map[string]Cluster{},
		profiles: map[string]Profile{},
	}
	for cid, members := range e.cur.clusters {
		ids := sortedIDs(members)
		s.clusters[cid] = Cluster{ID: cid, SignalIDs: ids}
		sigs := make([]Signal, 0, len(ids))
		for _, id := range ids {
			sigs = append(sigs, e.cur.signals[id])
		}
		s.profiles[cid] = buildProfile(cid, sigs, e.cur, &e.cfg)
	}
	e.snap.Store(s)
}

// validate 校验单条信号，返回空串表示通过，否则为可定位的拒绝原因。
func (e *Engine) validate(sig Signal, trial *state) string {
	if sig.ID == "" {
		return "缺少信号ID"
	}
	if sig.CustomerKey == "" {
		return "缺少客户主体"
	}
	if sig.Channel == "" {
		return "缺少来源渠道"
	}
	if sig.Confidence < 0 || sig.Confidence > 1 {
		return "可信度越界[0,1]"
	}
	if _, dup := trial.signals[sig.ID]; dup {
		return "重复信号ID"
	}
	if trial.deleted[sig.CustomerKey] {
		return "客户主体已删除"
	}
	return ""
}

// IngestBatch 批量接入信号。乱序、重复、迟到均可：
// 最终归属与画像只由信号内容与配置决定。
// 异常信号进入隔离区并附原因，不静默丢弃。
func (e *Engine) IngestBatch(signals []Signal) (BatchResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	res := BatchResult{}
	if len(signals) > e.cfg.MaxBatchSize {
		return res, fmt.Errorf("%w: 批量大小%d超过上限%d", ErrLimit, len(signals), e.cfg.MaxBatchSize)
	}
	if len(e.cur.signals)+len(signals) > e.cfg.MaxSignals {
		return res, fmt.Errorf("%w: 信号总量将超过上限%d", ErrLimit, e.cfg.MaxSignals)
	}

	deadline := time.Now().Add(e.cfg.MaxBatchDuration)
	trial := e.cur.clone()
	var events []Event
	for _, sig := range signals {
		if reason := e.validate(sig, trial); reason != "" {
			res.Rejected = append(res.Rejected, RejectedSignal{Signal: sig, Reason: reason})
			continue
		}
		ev := Event{Kind: EvSignal, Signal: &sig}
		trial.apply(ev)
		events = append(events, ev)
		res.Accepted++
		if time.Now().After(deadline) {
			// 超时：整批不提交，trial 丢弃，不留部分写入。
			e.quarantine(res.Rejected)
			return BatchResult{}, fmt.Errorf("%w: 上限%s", ErrBatchTimeout, e.cfg.MaxBatchDuration)
		}
	}
	trial.finalize()
	if len(events) > 0 {
		if err := e.commit(trial, events); err != nil {
			e.quarantine(res.Rejected)
			return BatchResult{}, err
		}
	}
	e.quarantine(res.Rejected)
	res.BatchID = e.batch
	return res, nil
}

func (e *Engine) quarantine(rej []RejectedSignal) {
	if len(rej) == 0 {
		return
	}
	e.quarMu.Lock()
	defer e.quarMu.Unlock()
	e.quar = append(e.quar, rej...)
}

// Profile 读取某簇当前画像（某个自洽的完整版本）。
func (e *Engine) Profile(actor, clusterID string) (Profile, error) {
	if !e.allow(actor, e.cfg.ReadScope) {
		e.addAudit(actor, "read_profile", clusterID, false)
		return Profile{}, ErrPermission
	}
	e.addAudit(actor, "read_profile", clusterID, true)
	s := e.snap.Load()
	p, ok := s.profiles[clusterID]
	if !ok {
		return Profile{}, fmt.Errorf("%w: 簇%s", ErrNotFound, clusterID)
	}
	return p, nil
}

// ProfileAsOf 读取指定事件序号时点之前的画像（历史时点查询）。
// 重放不持有写锁，不会阻塞信号接入。
func (e *Engine) ProfileAsOf(actor, clusterID string, seq uint64) (Profile, error) {
	if !e.allow(actor, e.cfg.ReadScope) {
		e.addAudit(actor, "read_profile_asof", clusterID, false)
		return Profile{}, ErrPermission
	}
	e.addAudit(actor, "read_profile_asof", clusterID, true)

	e.mu.Lock()
	events := append([]Event(nil), e.events...) // 拷贝切片头后释放锁
	e.mu.Unlock()

	st := newState()
	for _, ev := range events {
		if ev.Seq > seq {
			break
		}
		st.apply(ev)
	}
	st.finalize()
	members, ok := st.clusters[clusterID]
	if !ok {
		return Profile{}, fmt.Errorf("%w: 时点%d的簇%s", ErrNotFound, seq, clusterID)
	}
	sigs := make([]Signal, 0, len(members))
	for _, id := range members {
		sigs = append(sigs, st.signals[id])
	}
	return buildProfile(clusterID, sigs, st, &e.cfg), nil
}

// Clusters 列出当前所有身份簇。
func (e *Engine) Clusters(actor string) ([]Cluster, error) {
	if !e.allow(actor, e.cfg.ReadScope) {
		e.addAudit(actor, "list_clusters", "", false)
		return nil, ErrPermission
	}
	e.addAudit(actor, "list_clusters", "", true)
	s := e.snap.Load()
	out := make([]Cluster, 0, len(s.clusters))
	for _, c := range s.clusters {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// anchorOf 取簇的锚点 signalID（簇内最小 ID）。
func anchorOf(clusterID string) string { return strings.TrimPrefix(clusterID, "C-") }

// Merge 人工合并两个簇，返回 mergeID。
func (e *Engine) Merge(actor, clusterA, clusterB string) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.cur.clusters[clusterA]; !ok {
		return "", fmt.Errorf("%w: 簇%s", ErrNotFound, clusterA)
	}
	if _, ok := e.cur.clusters[clusterB]; !ok {
		return "", fmt.Errorf("%w: 簇%s", ErrNotFound, clusterB)
	}
	mergeID := fmt.Sprintf("M-%d", e.seq+1)
	ev := Event{Kind: EvMerge, MergeID: mergeID,
		ClusterA: anchorOf(clusterA), ClusterB: anchorOf(clusterB)}
	trial := e.cur.clone()
	trial.apply(ev)
	trial.finalize()
	if err := e.commit(trial, []Event{ev}); err != nil {
		return "", err
	}
	e.addAudit(actor, "merge", mergeID, true)
	return mergeID, nil
}

// UndoMerge 撤销一次合并：不影响其余身份簇，
// 被撤销的归属在后续读取与重放中不会复活（事件流保序）。
func (e *Engine) UndoMerge(actor, mergeID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.cur.merges[mergeID]; !ok {
		return fmt.Errorf("%w: 合并%s", ErrNotFound, mergeID)
	}
	ev := Event{Kind: EvUndoMerge, UndoMergeID: mergeID}
	trial := e.cur.clone()
	trial.apply(ev)
	trial.finalize()
	if err := e.commit(trial, []Event{ev}); err != nil {
		return err
	}
	e.addAudit(actor, "undo_merge", mergeID, true)
	return nil
}

// Split 按规则拆分一个簇，返回拆分后形成的簇 ID。
// 拆分依据（规则与原因）随事件持久留痕。
func (e *Engine) Split(actor, clusterID string, rule SplitRule) ([]string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	members, ok := e.cur.clusters[clusterID]
	if !ok {
		return nil, fmt.Errorf("%w: 簇%s", ErrNotFound, clusterID)
	}
	old := map[string]bool{}
	for _, m := range members {
		old[m] = true
	}
	ev := Event{Kind: EvSplit, SplitTarget: anchorOf(clusterID), Rule: &rule}
	trial := e.cur.clone()
	trial.apply(ev)
	trial.finalize()
	if err := e.commit(trial, []Event{ev}); err != nil {
		return nil, err
	}
	var out []string
	for cid, ms := range e.cur.clusters {
		for _, m := range ms {
			if old[m] {
				out = append(out, cid)
				break
			}
		}
	}
	sort.Strings(out)
	e.addAudit(actor, "split", clusterID+" -> "+strings.Join(out, ","), true)
	return out, nil
}

// WithdrawConsent 撤回某客户主体的同意范围；
// 被撤回范围的信号立即退出画像。
func (e *Engine) WithdrawConsent(actor, customerKey string, scopes []string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	ev := Event{Kind: EvWithdraw, CustomerKey: customerKey, Scopes: scopes}
	trial := e.cur.clone()
	trial.apply(ev)
	trial.finalize()
	if err := e.commit(trial, []Event{ev}); err != nil {
		return err
	}
	e.addAudit(actor, "withdraw", customerKey, true)
	return nil
}

// DeleteCustomer 删除某客户主体的全部信号与派生画像。
// 墓碑阻止后续写入让已删除内容复活。
func (e *Engine) DeleteCustomer(actor, customerKey string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	ev := Event{Kind: EvDelete, CustomerKey: customerKey}
	trial := e.cur.clone()
	trial.apply(ev)
	trial.finalize()
	if err := e.commit(trial, []Event{ev}); err != nil {
		return err
	}
	e.quarMu.Lock()
	n := e.quar[:0]
	for _, r := range e.quar {
		if r.Signal.CustomerKey != customerKey {
			n = append(n, r)
		}
	}
	e.quar = n
	e.quarMu.Unlock()
	e.addAudit(actor, "delete", customerKey, true)
	return nil
}

// AuditLog 返回审计记录（含被拒绝的越权访问）。
func (e *Engine) AuditLog() []AuditEntry {
	e.auditMu.Lock()
	defer e.auditMu.Unlock()
	return append([]AuditEntry(nil), e.audit...)
}

// Quarantined 返回被隔离的信号及原因。
func (e *Engine) Quarantined() []RejectedSignal {
	e.quarMu.Lock()
	defer e.quarMu.Unlock()
	return append([]RejectedSignal(nil), e.quar...)
}
