package identity

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Reader 是一次读取的身份与权限范围。
type Reader struct {
	Actor  string   `json:"actor"`
	Scopes []string `json:"scopes"` // 允许读取的渠道范围；"*" 表示全部
}

func (r Reader) hasScope(ch string) bool {
	for _, s := range r.Scopes {
		if s == "*" || s == ch {
			return true
		}
	}
	return false
}

// mergeRecord 是一次手工合并的记录，撤销后永不复活。
type mergeRecord struct {
	ID     string `json:"id"`
	KeyA   string `json:"keyA"` // 标识的规范化键（plain key）
	KeyB   string `json:"keyB"`
	Undone bool   `json:"undone"`
}

// splitRecord 是一次拆分的记录：信号 -> 分组，留有判定依据。
// 拆分后，被拆分信号的标识在各自分组命名空间内成点，
// 后续新信号或手工合并可通过原始标识把各组重新连通（正常归并路径）。
type splitRecord struct {
	ID          string            `json:"id"`
	Rule        string            `json:"rule"`
	SignalGroup map[string]string `json:"signalGroup"` // 信号 ID -> 组键
	Rationale   string            `json:"rationale"`
}

// eraseRecord 是一次删除的记录。
type eraseRecord struct {
	Ident    string   `json:"ident"`
	Signals  []string `json:"signals"`
	Idents   []string `json:"idents"` // 规范化标识键（plain）
	Clusters []string `json:"clusters"`
}

// Engine 是身份归并引擎：批量接入、并发查询、撤销、拆分、删除与恢复。
//
// 并发模型：写路径（接入/合并/撤销/拆分/删除）在 mu 下串行；
// 读路径持有原子发布的不可变 view，全程无锁，慢查询不阻塞接入。
type Engine struct {
	cfg   Config
	mu    sync.Mutex
	st    *store
	audit *auditLog

	viewPtr atomic.Pointer[view]

	// 以下为写路径可变状态（mu 保护），全部由 WAL 重放确定性重建。
	ver           uint64
	batchSeq      int
	mergeSeq      int
	splitSeq      int
	g             *graph
	signals       map[string]Signal            // 已提交信号
	members       map[string]map[string]Signal // 根节点键 -> 成员信号
	plainIndex    map[string]map[string]bool   // 规范化标识键 -> 图中的节点键（含各分组变体）
	merges        []mergeRecord
	splits        []splitRecord
	splitBySignal map[string]*splitRecord
	tombstones    map[string]bool // 已删除主体的标识键，防止复活
	closed        bool
}

// Open 打开（或创建）引擎并重放本地 WAL 恢复状态。
func Open(cfg Config) (*Engine, error) {
	st, err := openStore(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	e := &Engine{
		cfg:           cfg,
		st:            st,
		audit:         newAuditLog(),
		g:             newGraph(),
		signals:       map[string]Signal{},
		members:       map[string]map[string]Signal{},
		plainIndex:    map[string]map[string]bool{},
		splitBySignal: map[string]*splitRecord{},
		tombstones:    map[string]bool{},
	}
	e.viewPtr.Store(newView())
	var pending []Signal
	err = st.replay(func(ev event) error {
		switch ev.Kind {
		case "batch-begin":
			e.batchSeq++
			pending = nil
		case "signal":
			var s Signal
			if err := json.Unmarshal(ev.Payload, &s); err != nil {
				return err
			}
			pending = append(pending, s)
		case "batch-commit":
			// 只有看到 commit 的批次才被应用；崩溃留下的未提交批次被丢弃。
			e.ver++
			e.applyCommittedBatchLocked(pending, nil)
			pending = nil
		case "merge":
			var rec mergeRecord
			if err := json.Unmarshal(ev.Payload, &rec); err != nil {
				return err
			}
			e.mergeSeq++
			e.merges = append(e.merges, rec)
			e.ver++
			e.rebuildAndPublishLocked()
		case "undo-merge":
			var id string
			if err := json.Unmarshal(ev.Payload, &id); err != nil {
				return err
			}
			for i := range e.merges {
				if e.merges[i].ID == id {
					e.merges[i].Undone = true
				}
			}
			e.ver++
			e.rebuildAndPublishLocked()
		case "split":
			var rec splitRecord
			if err := json.Unmarshal(ev.Payload, &rec); err != nil {
				return err
			}
			e.splitSeq++
			e.addSplitLocked(rec)
			e.ver++
			e.rebuildAndPublishLocked()
		case "erase":
			var rec eraseRecord
			if err := json.Unmarshal(ev.Payload, &rec); err != nil {
				return err
			}
			e.applyEraseLocked(rec)
		}
		return nil
	})
	if err != nil {
		st.close()
		return nil, err
	}
	return e, nil
}

// cur 返回当前已提交的不可变视图。
func (e *Engine) cur() *view { return e.viewPtr.Load() }

// Ingest 批量接入信号，逐项返回处理结果；批次要么整体提交要么整体失败。
func (e *Engine) Ingest(signals []Signal) (BatchResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return BatchResult{}, ErrEngineClosed
	}
	e.batchSeq++
	res := BatchResult{BatchID: fmt.Sprintf("batch-%d", e.batchSeq)}
	b := e.cfg.Budgets
	if b.MaxSignalsPerBatch > 0 && len(signals) > b.MaxSignalsPerBatch {
		for _, s := range signals {
			res.Items = append(res.Items, SignalOutcome{SignalID: s.ID, Status: "rejected", Err: ErrBatchTooLarge.Error()})
		}
		return res, ErrBatchTooLarge
	}
	fresh := 0
	for _, s := range signals {
		if _, ok := e.signals[s.ID]; !ok {
			fresh++
		}
	}
	if b.MaxSignalsInMemory > 0 && len(e.signals)+fresh > b.MaxSignalsInMemory {
		for _, s := range signals {
			res.Items = append(res.Items, SignalOutcome{SignalID: s.ID, Status: "rejected", Err: ErrMemoryLimit.Error()})
		}
		return res, ErrMemoryLimit
	}

	deadline := time.Now().Add(b.MaxBatchDuration)
	seen := map[string]bool{}
	var toApply []Signal
	for i, s := range signals {
		out := SignalOutcome{SignalID: s.ID}
		switch {
		case b.MaxBatchDuration > 0 && time.Now().After(deadline):
			out.Status, out.Err = "rejected", ErrBatchTimeout.Error()
			res.Items = append(res.Items, out)
			for _, rest := range signals[i+1:] {
				res.Items = append(res.Items, SignalOutcome{SignalID: rest.ID, Status: "rejected", Err: ErrBatchTimeout.Error()})
			}
			return res, ErrBatchTimeout
		case s.ID == "" || len(s.Identifiers) == 0:
			out.Status, out.Err = "rejected", ErrSignalInvalid.Error()
		case seen[s.ID]:
			out.Status = "duplicate"
		case e.hasSignal(s.ID):
			out.Status = "duplicate"
		case e.touchesTombstone(s):
			out.Status, out.Err = "rejected", ErrErasedSubject.Error()
		default:
			out.Status = "applied"
			seen[s.ID] = true
			toApply = append(toApply, s)
		}
		res.Items = append(res.Items, out)
	}
	if len(toApply) == 0 {
		res.Committed = true
		return res, nil
	}

	// 先写 WAL 并落盘，再应用：已提交批次重启后不丢失、不重复应用。
	if err := e.st.append(event{Kind: "batch-begin", Payload: json.RawMessage(fmt.Sprintf(`{"id":%q}`, res.BatchID))}); err != nil {
		return res, err
	}
	for _, s := range toApply {
		raw, err := json.Marshal(s)
		if err != nil {
			return res, err
		}
		if err := e.st.append(event{Kind: "signal", Payload: raw}); err != nil {
			return res, err
		}
	}
	commit, _ := json.Marshal(map[string]any{"id": res.BatchID})
	if err := e.st.append(event{Kind: "batch-commit", Payload: commit}); err != nil {
		return res, err
	}
	if err := e.st.sync(); err != nil {
		return res, err
	}

	e.ver++
	e.applyCommittedBatchLocked(toApply, &res)
	res.Committed = true
	return res, nil
}

func (e *Engine) hasSignal(id string) bool {
	_, ok := e.signals[id]
	return ok
}

func (e *Engine) touchesTombstone(s Signal) bool {
	for _, id := range s.Identifiers {
		if e.tombstones[id.Key()] {
			return true
		}
	}
	return false
}

// applyCommittedBatchLocked 把一批已提交信号增量应用到身份图并发布新视图。
// 只重算受影响的簇，不做全量重算。
func (e *Engine) applyCommittedBatchLocked(batch []Signal, res *BatchResult) {
	old := e.cur()
	nv := old.cloneForWrite()
	nv.version = e.ver

	affectedOld := map[string]bool{}
	for _, s := range batch {
		for _, id := range s.Identifiers {
			if cid, ok := old.byIdent[id.Key()]; ok {
				affectedOld[cid] = true
			}
			// 拆分变体节点的簇同样受影响。
			for nk := range e.plainIndex[id.Key()] {
				affectedOld[e.g.find(nk)] = true
			}
		}
	}
	for _, s := range batch {
		e.applySignalLinksLocked(s, true)
		e.signals[s.ID] = s
	}
	newRoots := map[string]bool{}
	for _, s := range batch {
		for _, id := range s.Identifiers {
			newRoots[e.g.find(id.Key())] = true
			for nk := range e.plainIndex[id.Key()] {
				newRoots[e.g.find(nk)] = true
			}
		}
	}
	for cid := range affectedOld {
		if !newRoots[cid] {
			nv.removeCluster(cid)
		}
	}
	for root := range newRoots {
		e.materializeClusterLocked(nv, old, root)
	}
	e.viewPtr.Store(nv)

	if res != nil {
		byID := map[string]string{}
		for _, s := range batch {
			byID[s.ID] = e.g.find(e.nodeKey(s, s.Identifiers[0]))
		}
		for i := range res.Items {
			if cid, ok := byID[res.Items[i].SignalID]; ok {
				res.Items[i].ClusterID = cid
			}
		}
	}
}

// nodeKey 返回信号某个标识在身份图中的节点键：
// 未被拆分的信号使用规范化标识键本身；被拆分的信号使用 组键+标识键 的命名空间变体。
func (e *Engine) nodeKey(s Signal, id Identifier) string {
	if sp, ok := e.splitBySignal[s.ID]; ok {
		return sp.SignalGroup[s.ID] + "\x1f" + id.Key()
	}
	return id.Key()
}

// applySignalLinksLocked 把一条信号的标识并入身份图。
// live 为 true 时（新到达信号），同时把标识与图中已有的全部分组变体连通，
// 使拆分后的身份簇可以经正常归并重新合并。
func (e *Engine) applySignalLinksLocked(s Signal, live bool) {
	nodes := make([]string, 0, len(s.Identifiers))
	for _, id := range s.Identifiers {
		nk := e.nodeKey(s, id)
		nodes = append(nodes, nk)
		set := e.plainIndex[id.Key()]
		if set == nil {
			set = map[string]bool{}
			e.plainIndex[id.Key()] = set
		}
		set[nk] = true
	}
	first := nodes[0]
	for _, nk := range nodes[1:] {
		e.linkIdentsLocked(first, nk)
	}
	if live {
		// 新信号不属于任何既有分组：把它的标识与所有分组变体连通。
		for _, id := range s.Identifiers {
			for nk := range e.plainIndex[id.Key()] {
				e.linkIdentsLocked(id.Key(), nk)
			}
		}
	}
	e.addMemberLocked(e.g.find(first), s)
}

// linkIdentsLocked 确定性合并两个节点键，并合并对应的成员集。
func (e *Engine) linkIdentsLocked(a, b string) {
	ra, rb := e.g.find(a), e.g.find(b)
	e.g.union(a, b)
	r := e.g.find(a)
	if ra == rb {
		return
	}
	ma, mb := e.members[ra], e.members[rb]
	if ma == nil {
		ma = map[string]Signal{}
	}
	for k, v := range mb {
		ma[k] = v
	}
	e.members[r] = ma
	if r != ra {
		delete(e.members, ra)
	}
	if r != rb {
		delete(e.members, rb)
	}
}

// addMemberLocked 把信号登记到指定根下的成员集。
func (e *Engine) addMemberLocked(root string, s Signal) {
	m := e.members[root]
	if m == nil {
		m = map[string]Signal{}
		e.members[root] = m
	}
	m[s.ID] = s
}

// materializeClusterLocked 为一个根构建不可变簇并写入视图；未变化的簇复用旧对象。
func (e *Engine) materializeClusterLocked(nv, old *view, root string) {
	ms := e.members[root]
	members := make([]Signal, 0, len(ms))
	for _, s := range ms {
		members = append(members, s)
	}
	sort.Slice(members, func(i, j int) bool { return members[i].ID < members[j].ID })

	if oc, ok := old.clusters[root]; ok && sameMembers(oc.members, members) {
		nv.clusters[root] = oc // 未变化：复用旧对象，画像版本链不追加
		nv.registerIdents(oc)
		return
	}
	identSet := map[string]bool{}
	for _, s := range members {
		for _, id := range s.Identifiers {
			identSet[id.Key()] = true
		}
	}
	idents := make([]string, 0, len(identSet))
	for k := range identSet {
		idents = append(idents, k)
	}
	sort.Strings(idents)
	c := &cluster{
		id:      root,
		members: members,
		idents:  idents,
		profile: buildProfile(e.cfg, members, nv.version, root),
	}
	nv.putCluster(c)
}

func sameMembers(a, b []Signal) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].ID != b[i].ID {
			return false
		}
	}
	return true
}

// rebuildLocked 全量确定性重建身份图（用于合并/撤销/拆分/删除等罕见管理操作）。
// 结果只取决于已提交信号、有效合并、拆分与墓碑，与历史到达顺序无关。
func (e *Engine) rebuildLocked() *view {
	e.g = newGraph()
	e.members = map[string]map[string]Signal{}
	e.plainIndex = map[string]map[string]bool{}

	ids := make([]string, 0, len(e.signals))
	for id := range e.signals {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		e.applySignalLinksLocked(e.signals[id], false)
	}
	for _, m := range e.merges {
		if m.Undone || e.tombstones[m.KeyA] || e.tombstones[m.KeyB] {
			continue // 被撤销或已删除的合并不再生效，重放中也不复活
		}
		// 把两个标识的全部已知变体（含分组变体与 plain 节点）连通。
		for _, ka := range e.variantNodes(m.KeyA) {
			for _, kb := range e.variantNodes(m.KeyB) {
				e.linkIdentsLocked(ka, kb)
			}
		}
	}

	old := e.cur()
	nv := newView()
	nv.version = e.ver
	for k, h := range old.history {
		nv.history[k] = h
	}
	for k, h := range old.identHis {
		nv.identHis[k] = h
	}
	roots := make([]string, 0, len(e.members))
	for k := range e.members {
		roots = append(roots, k)
	}
	sort.Strings(roots) // 确定性物化顺序，byIdent 冲突裁决可复现
	for _, root := range roots {
		e.materializeClusterLocked(nv, old, root)
	}
	return nv
}

// variantNodes 返回一个规范化标识键在图中的全部节点键（含 plain 节点本身）。
func (e *Engine) variantNodes(plainKey string) []string {
	out := []string{plainKey}
	for nk := range e.plainIndex[plainKey] {
		if nk != plainKey {
			out = append(out, nk)
		}
	}
	return out
}

func (e *Engine) rebuildAndPublishLocked() {
	e.viewPtr.Store(e.rebuildLocked())
}

// allowed 判断读者是否有权读取该簇：需覆盖簇内任一成员信号的渠道。
func (e *Engine) allowed(r Reader, c *cluster) bool {
	for _, s := range c.members {
		if r.hasScope(s.Channel) {
			return true
		}
	}
	return false
}

func (e *Engine) auditDenied(r Reader, action, target, detail string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.audit.record(AuditEntry{At: time.Now(), Actor: r.Actor, Action: action, Target: target, Allowed: false, Detail: detail})
}

// Query 按标识查询当前画像，带权限校验与审计。
func (e *Engine) Query(r Reader, id Identifier) (Profile, error) {
	v := e.cur()
	cid, ok := v.byIdent[id.Key()]
	if !ok {
		return Profile{}, ErrClusterNotFound
	}
	c := v.clusters[cid]
	if !e.allowed(r, c) {
		e.auditDenied(r, "query", id.Key(), "cross-scope read denied")
		return Profile{}, ErrPermissionDenied
	}
	return c.profile, nil
}

// Inspect 按簇 ID 查看当前画像（运维与审计用途，需全量权限）。
func (e *Engine) Inspect(r Reader, clusterID string) (Profile, error) {
	v := e.cur()
	c, ok := v.clusters[clusterID]
	if !ok {
		return Profile{}, ErrClusterNotFound
	}
	if !e.allowed(r, c) {
		e.auditDenied(r, "inspect", clusterID, "cross-scope read denied")
		return Profile{}, ErrPermissionDenied
	}
	return c.profile, nil
}

// QueryAsOf 历史时点查询：返回不晚于 version 的画像版本。
// 已删除主体的历史已被清除，不会通过时点查询复活。
func (e *Engine) QueryAsOf(r Reader, id Identifier, version uint64) (Profile, error) {
	v := e.cur()
	p, ok := v.profileAsOf(id.Key(), version)
	if !ok {
		return Profile{}, ErrClusterNotFound
	}
	cid, ok := v.byIdent[id.Key()]
	if ok && !e.allowed(r, v.clusters[cid]) {
		e.auditDenied(r, "query-as-of", id.Key(), "cross-scope read denied")
		return Profile{}, ErrPermissionDenied
	}
	return p, nil
}

// Merge 手工合并两个身份簇，返回合并记录 ID。
func (e *Engine) Merge(actor string, a, b Identifier) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return "", ErrEngineClosed
	}
	v := e.cur()
	if _, ok := v.byIdent[a.Key()]; !ok {
		return "", ErrClusterNotFound
	}
	if _, ok := v.byIdent[b.Key()]; !ok {
		return "", ErrClusterNotFound
	}
	e.mergeSeq++
	rec := mergeRecord{ID: fmt.Sprintf("merge-%d", e.mergeSeq), KeyA: a.Key(), KeyB: b.Key()}
	raw, _ := json.Marshal(rec)
	if err := e.st.append(event{Kind: "merge", Payload: raw}); err != nil {
		return "", err
	}
	if err := e.st.sync(); err != nil {
		return "", err
	}
	e.merges = append(e.merges, rec)
	e.ver++
	e.rebuildAndPublishLocked()
	e.audit.record(AuditEntry{At: time.Now(), Actor: actor, Action: "merge", Target: rec.ID, Allowed: true,
		Detail: rec.KeyA + " + " + rec.KeyB})
	return rec.ID, nil
}

// UndoMerge 撤销一次误合；被撤销的归属不会在后续读取或重放中复活。
func (e *Engine) UndoMerge(actor, mergeID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrEngineClosed
	}
	idx := -1
	for i := range e.merges {
		if e.merges[i].ID == mergeID && !e.merges[i].Undone {
			idx = i
			break
		}
	}
	if idx < 0 {
		return ErrMergeNotFound
	}
	raw, _ := json.Marshal(mergeID)
	if err := e.st.append(event{Kind: "undo-merge", Payload: raw}); err != nil {
		return err
	}
	if err := e.st.sync(); err != nil {
		return err
	}
	e.merges[idx].Undone = true
	e.ver++
	e.rebuildAndPublishLocked()
	e.audit.record(AuditEntry{At: time.Now(), Actor: actor, Action: "undo-merge", Target: mergeID, Allowed: true})
	return nil
}

// Split 按配置的拆分规则拆分身份簇，返回拆分后的簇 ID 与判定依据。
func (e *Engine) Split(actor string, id Identifier, ruleID string) ([]string, string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, "", ErrEngineClosed
	}
	rule, ok := e.cfg.splitRule(ruleID)
	if !ok {
		return nil, "", ErrSplitRuleNotFound
	}
	v := e.cur()
	cid, ok := v.byIdent[id.Key()]
	if !ok {
		return nil, "", ErrClusterNotFound
	}
	c := v.clusters[cid]
	groups, rationale := computeSplitGroups(rule, c)
	if len(groups) == 0 {
		return []string{cid}, rationale + "；仅一个分组，无需拆分", nil
	}
	sigGroup := map[string]string{}
	for _, g := range groups {
		for _, sid := range g.members {
			sigGroup[sid] = g.key
		}
	}
	e.splitSeq++
	rec := splitRecord{
		ID:          fmt.Sprintf("split-%d", e.splitSeq),
		Rule:        rule.ID,
		SignalGroup: sigGroup,
		Rationale:   rationale,
	}
	raw, _ := json.Marshal(rec)
	if err := e.st.append(event{Kind: "split", Payload: raw}); err != nil {
		return nil, "", err
	}
	if err := e.st.sync(); err != nil {
		return nil, "", err
	}
	e.addSplitLocked(rec)
	e.ver++
	e.rebuildAndPublishLocked()
	e.audit.record(AuditEntry{At: time.Now(), Actor: actor, Action: "split", Target: rec.ID, Allowed: true, Detail: rationale})

	// 收集拆分后的簇 ID。
	out := map[string]bool{}
	for sid := range sigGroup {
		s := e.signals[sid]
		out[e.g.find(e.nodeKey(s, s.Identifiers[0]))] = true
	}
	clusterIDs := make([]string, 0, len(out))
	for id := range out {
		clusterIDs = append(clusterIDs, id)
	}
	sort.Strings(clusterIDs)
	return clusterIDs, rationale, nil
}

// splitGroup 是拆分计算中的一个分组。
type splitGroup struct {
	key     string
	members []string // 信号 ID
}

// computeSplitGroups 按拆分规则计算分组（by-channel：先按渠道、再按渠道内连通性）。
func computeSplitGroups(rule SplitRule, c *cluster) (map[string]*splitGroup, string) {
	byChannel := map[string][]Signal{}
	for _, s := range c.members {
		byChannel[s.Channel] = append(byChannel[s.Channel], s)
	}
	channels := make([]string, 0, len(byChannel))
	for ch := range byChannel {
		channels = append(channels, ch)
	}
	sort.Strings(channels)
	groups := map[string]*splitGroup{}
	var sb strings.Builder
	fmt.Fprintf(&sb, "rule=%s(%s) 簇 %s 按渠道拆分:", rule.ID, rule.Kind, c.id)
	for _, ch := range channels {
		sigs := byChannel[ch]
		g := newGraph()
		for _, s := range sigs {
			g.addSignal(s)
		}
		comps := map[string][]string{}
		for _, s := range sigs {
			r := g.find(s.Identifiers[0].Key())
			comps[r] = append(comps[r], s.ID)
		}
		roots := make([]string, 0, len(comps))
		for r := range comps {
			roots = append(roots, r)
		}
		sort.Strings(roots)
		for i, r := range roots {
			key := fmt.Sprintf("%s#%d", ch, i)
			members := comps[r]
			sort.Strings(members)
			groups[key] = &splitGroup{key: key, members: members}
			fmt.Fprintf(&sb, " [%s: %d 条信号]", key, len(members))
		}
	}
	if len(groups) < 2 {
		return nil, sb.String()
	}
	return groups, sb.String()
}

func (e *Engine) addSplitLocked(rec splitRecord) {
	e.splits = append(e.splits, rec)
	idx := &e.splits[len(e.splits)-1]
	for sid := range rec.SignalGroup {
		e.splitBySignal[sid] = idx
	}
}

// Erase 删除某主体的全部身份信号与派生画像，并放置墓碑防止复活。
func (e *Engine) Erase(actor string, id Identifier) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrEngineClosed
	}
	v := e.cur()
	cid, ok := v.byIdent[id.Key()]
	if !ok {
		return ErrClusterNotFound
	}
	c := v.clusters[cid]
	rec := eraseRecord{Ident: id.Key(), Clusters: []string{cid}}
	identSet := map[string]bool{}
	for _, s := range c.members {
		rec.Signals = append(rec.Signals, s.ID)
		for _, ident := range s.Identifiers {
			identSet[ident.Key()] = true
		}
	}
	for k := range identSet {
		rec.Idents = append(rec.Idents, k)
	}
	sort.Strings(rec.Signals)
	sort.Strings(rec.Idents)
	raw, _ := json.Marshal(rec)
	if err := e.st.append(event{Kind: "erase", Payload: raw}); err != nil {
		return err
	}
	if err := e.st.sync(); err != nil {
		return err
	}
	e.applyEraseLocked(rec)
	e.audit.record(AuditEntry{At: time.Now(), Actor: actor, Action: "erase", Target: rec.Ident, Allowed: true,
		Detail: fmt.Sprintf("removed %d signals, %d identifiers", len(rec.Signals), len(rec.Idents))})
	return nil
}

// applyEraseLocked 应用删除：移除信号、放置墓碑、重建视图并清除派生画像历史。
func (e *Engine) applyEraseLocked(rec eraseRecord) {
	for _, sid := range rec.Signals {
		delete(e.signals, sid)
	}
	for _, k := range rec.Idents {
		e.tombstones[k] = true
	}
	e.ver++
	nv := e.rebuildLocked()
	nv.dropHistory(rec.Clusters...)
	e.viewPtr.Store(nv)
}

// Audit 返回审计日志副本。
func (e *Engine) Audit() []AuditEntry {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.audit.list()
}

// Close 关闭引擎并落盘。
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true
	return e.st.close()
}
