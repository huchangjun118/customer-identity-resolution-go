package identity

import "sort"

// splitConstraint 是一次拆分留下的持久约束：
// 对 members 集合内的信号，忽略 exclude 中键类型形成的归并边。
// 约束随事件重放确定性地重建，撤销合并不影响它。
type splitConstraint struct {
	target  string          // 被拆分簇的锚点 signalID
	exclude map[string]bool // 忽略的键类型
	members map[string]bool // 拆分时簇内信号集合
	reason  string          // 判定依据（留痕）
}

// state 是由事件流重放得到的确定性状态。
// 同一事件序列在任意到达顺序、批次划分与并发度下，
// 只要事件内容相同，重放结果逐项一致。
type state struct {
	signals   map[string]Signal   // signalID -> 活跃信号
	keyIndex  map[string][]string // matchKey(type\x00value) -> signalIDs
	parent    map[string]string   // 并查集
	merges    map[string][2]string // mergeID -> 锚点对（人工合并边）
	splits    []splitConstraint
	deleted   map[string]bool            // 已删除客户主体（墓碑，防复活）
	withdrawn map[string]map[string]bool // customerKey -> 已撤回范围

	// finalize 派生：
	clusters      map[string][]string // clusterID -> 有序 signalID
	signalCluster map[string]string   // signalID -> clusterID
}

func newState() *state {
	return &state{
		signals:       map[string]Signal{},
		keyIndex:      map[string][]string{},
		parent:        map[string]string{},
		merges:        map[string][2]string{},
		deleted:       map[string]bool{},
		withdrawn:     map[string]map[string]bool{},
		clusters:      map[string][]string{},
		signalCluster: map[string]string{},
	}
}

func matchKey(typ, value string) string { return typ + "\x00" + value }

func (s *state) find(x string) string {
	p := s.parent[x]
	if p == "" || p == x {
		return x
	}
	root := s.find(p)
	s.parent[x] = root
	return root
}

func (s *state) union(a, b string) {
	ra, rb := s.find(a), s.find(b)
	if ra == rb {
		return
	}
	// 确定性：小 ID 为根。
	if ra < rb {
		s.parent[rb] = ra
	} else {
		s.parent[ra] = rb
	}
}

// suppressed 判断两个信号间某键类型的边是否被拆分约束抑制。
func (s *state) suppressed(typ, a, b string) bool {
	for _, sp := range s.splits {
		if sp.exclude[typ] && sp.members[a] && sp.members[b] {
			return true
		}
	}
	return false
}

// apply 将一条已提交事件应用到状态（确定性）。
func (s *state) apply(ev Event) {
	switch ev.Kind {
	case EvSignal:
		sig := *ev.Signal
		if s.deleted[sig.CustomerKey] {
			return // 已删除主体的信号不得复活
		}
		if _, dup := s.signals[sig.ID]; dup {
			return // 重复信号幂等
		}
		s.signals[sig.ID] = sig
		s.parent[sig.ID] = sig.ID
		for typ, val := range sig.MatchKeys {
			k := matchKey(typ, val)
			for _, other := range s.keyIndex[k] {
				if !s.suppressed(typ, sig.ID, other) {
					s.union(sig.ID, other)
				}
			}
			s.keyIndex[k] = append(s.keyIndex[k], sig.ID)
		}
	case EvMerge:
		s.merges[ev.MergeID] = [2]string{ev.ClusterA, ev.ClusterB}
		s.union(ev.ClusterA, ev.ClusterB)
	case EvUndoMerge:
		if _, ok := s.merges[ev.UndoMergeID]; ok {
			delete(s.merges, ev.UndoMergeID)
			s.rebuild()
		}
	case EvSplit:
		// 成员集合在重放到此事件时确定性计算，与首次应用一致。
		members := map[string]bool{}
		root := s.find(ev.SplitTarget)
		for id := range s.signals {
			if s.find(id) == root {
				members[id] = true
			}
		}
		ex := map[string]bool{}
		for _, t := range ev.Rule.ExcludeKeyTypes {
			ex[t] = true
		}
		s.splits = append(s.splits, splitConstraint{
			target:  ev.SplitTarget,
			exclude: ex,
			members: members,
			reason:  ev.Rule.Reason,
		})
		s.rebuild()
	case EvWithdraw:
		m := s.withdrawn[ev.CustomerKey]
		if m == nil {
			m = map[string]bool{}
			s.withdrawn[ev.CustomerKey] = m
		}
		for _, sc := range ev.Scopes {
			m[sc] = true
		}
	case EvDelete:
		s.deleted[ev.CustomerKey] = true
		for id, sig := range s.signals {
			if sig.CustomerKey != ev.CustomerKey {
				continue
			}
			for typ, val := range sig.MatchKeys {
				k := matchKey(typ, val)
				ids := s.keyIndex[k]
				n := ids[:0]
				for _, x := range ids {
					if x != id {
						n = append(n, x)
					}
				}
				s.keyIndex[k] = n
			}
			delete(s.signals, id)
		}
		s.rebuild()
	}
}

// rebuild 从信号、拆分约束与人工合并边全量重建并查集。
// 仅用于撤销合并、拆分、删除等罕见管理操作；常规接入走增量路径。
func (s *state) rebuild() {
	s.parent = map[string]string{}
	s.keyIndex = map[string][]string{}
	ids := make([]string, 0, len(s.signals))
	for id := range s.signals {
		ids = append(ids, id)
	}
	sort.Strings(ids) // 确定性重建顺序
	for _, id := range ids {
		s.parent[id] = id
	}
	for _, id := range ids {
		sig := s.signals[id]
		for typ, val := range sig.MatchKeys {
			k := matchKey(typ, val)
			for _, other := range s.keyIndex[k] {
				if !s.suppressed(typ, id, other) {
					s.union(id, other)
				}
			}
			s.keyIndex[k] = append(s.keyIndex[k], id)
		}
	}
	for _, e := range s.merges {
		s.union(e[0], e[1])
	}
}

// finalize 派生簇划分：clusterID = "C-" + 簇内最小 signalID（确定性）。
func (s *state) finalize() {
	s.clusters = map[string][]string{}
	s.signalCluster = map[string]string{}
	ids := make([]string, 0, len(s.signals))
	for id := range s.signals {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		root := s.find(id)
		s.clusters[root] = append(s.clusters[root], id)
	}
	for root, members := range s.clusters {
		cid := "C-" + members[0]
		delete(s.clusters, root)
		s.clusters[cid] = members
		for _, m := range members {
			s.signalCluster[m] = cid
		}
	}
}

// consentAllows 判断信号是否允许进入指定范围（未撤回且已同意）。
func (s *state) consentAllows(sig Signal, scope string) bool {
	if s.withdrawn[sig.CustomerKey][scope] {
		return false
	}
	for _, c := range sig.Consent {
		if c == scope {
			return true
		}
	}
	return false
}

// canonical 返回簇成员的确定性排序副本。
func sortedIDs(ids []string) []string {
	out := append([]string(nil), ids...)
	sort.Strings(out)
	return out
}

// clone 深拷贝状态，用于写路径的试算/回滚。
func (s *state) clone() *state {
	c := newState()
	for k, v := range s.signals {
		c.signals[k] = v
	}
	for k, v := range s.keyIndex {
		c.keyIndex[k] = append([]string(nil), v...)
	}
	for k, v := range s.parent {
		c.parent[k] = v
	}
	for k, v := range s.merges {
		c.merges[k] = v
	}
	for _, sp := range s.splits {
		nsp := splitConstraint{target: sp.target, reason: sp.reason,
			exclude: map[string]bool{}, members: map[string]bool{}}
		for k, v := range sp.exclude {
			nsp.exclude[k] = v
		}
		for k, v := range sp.members {
			nsp.members[k] = v
		}
		c.splits = append(c.splits, nsp)
	}
	for k, v := range s.deleted {
		c.deleted[k] = v
	}
	for k, v := range s.withdrawn {
		m := map[string]bool{}
		for sc, b := range v {
			m[sc] = b
		}
		c.withdrawn[k] = m
	}
	for k, v := range s.clusters {
		c.clusters[k] = append([]string(nil), v...)
	}
	for k, v := range s.signalCluster {
		c.signalCluster[k] = v
	}
	return c
}
