package identity

import "sort"

// graph 是身份图：以标识为点、信号为边维护连通分量（身份簇）。
// 普通接入走增量并查集，无需全量重算；
// 代表元恒为集合内字典序最小的标识键，保证结果与到达顺序无关。
type graph struct {
	parent map[string]string // 并查集：标识键 -> 父标识键
}

func newGraph() *graph { return &graph{parent: map[string]string{}} }

// find 返回标识键所在集合的代表元（路径压缩）。
func (g *graph) find(k string) string {
	p, ok := g.parent[k]
	if !ok {
		g.parent[k] = k
		return k
	}
	if p != k {
		g.parent[k] = g.find(p)
	}
	return g.parent[k]
}

// union 确定性合并两个标识键所在集合：代表元取字典序较小者。
func (g *graph) union(a, b string) {
	ra, rb := g.find(a), g.find(b)
	if ra == rb {
		return
	}
	if rb < ra {
		ra, rb = rb, ra
	}
	g.parent[rb] = ra
}

// addSignal 将一条信号的全部标识并入同一集合。
func (g *graph) addSignal(s Signal) {
	if len(s.Identifiers) == 0 {
		return
	}
	first := s.Identifiers[0].Key()
	for _, id := range s.Identifiers[1:] {
		g.union(first, id.Key())
	}
}

// clusterID 返回标识键当前所属簇的确定性 ID。
func (g *graph) clusterID(k string) string { return g.find(k) }

// keys 返回图中全部标识键（排序后），用于重建视图。
func (g *graph) keys() []string {
	out := make([]string, 0, len(g.parent))
	for k := range g.parent {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
