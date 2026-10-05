package identity

// cluster 是一个身份簇的不可变表示：成员信号 + 完整自洽画像。
type cluster struct {
	id      string
	members []Signal // 按信号 ID 排序
	idents  []string // 属于该簇的标识键（排序）
	profile Profile
}

// identVersion 记录标识在某个版本上所属的簇，用于历史时点查询。
type identVersion struct {
	Version   uint64
	ClusterID string
}

// view 是某一版本上已提交状态的不可变快照。
// 读路径持有 view 指针即可无锁查询，慢查询不阻塞接入；
// 写路径采用写时复制生成新 view 并原子发布。
type view struct {
	version  uint64
	clusters map[string]*cluster       // 簇 ID -> 簇
	byIdent  map[string]string         // 标识键 -> 簇 ID
	history  map[string][]Profile      // 簇 ID -> 画像版本链（升序，合并后旧簇链冻结保留）
	identHis map[string][]identVersion // 标识键 -> 所属簇版本链（升序）
}

func newView() *view {
	return &view{
		clusters: map[string]*cluster{},
		byIdent:  map[string]string{},
		history:  map[string][]Profile{},
		identHis: map[string][]identVersion{},
	}
}

// cloneForWrite 浅拷贝各映射，基于当前视图生成可修改副本。
func (v *view) cloneForWrite() *view {
	nv := &view{
		version:  v.version,
		clusters: make(map[string]*cluster, len(v.clusters)),
		byIdent:  make(map[string]string, len(v.byIdent)),
		history:  make(map[string][]Profile, len(v.history)),
		identHis: make(map[string][]identVersion, len(v.identHis)),
	}
	for k, c := range v.clusters {
		nv.clusters[k] = c
	}
	for k, id := range v.byIdent {
		nv.byIdent[k] = id
	}
	for k, h := range v.history {
		nv.history[k] = h
	}
	for k, h := range v.identHis {
		nv.identHis[k] = h
	}
	return nv
}

// putCluster 写入/替换一个簇，并维护画像版本链与标识索引。
func (v *view) putCluster(c *cluster) {
	v.clusters[c.id] = c
	v.history[c.id] = append(v.history[c.id], c.profile)
	v.registerIdents(c)
}

// registerIdents 登记簇的标识索引（不追加画像版本）。
// 拆分后同一标识可能属于多个簇：确定性归属到簇 ID 较小者。
func (v *view) registerIdents(c *cluster) {
	for _, k := range c.idents {
		if cur, ok := v.byIdent[k]; !ok || c.id < cur {
			v.byIdent[k] = c.id
		}
		chain := v.identHis[k]
		if n := len(chain); n == 0 || chain[n-1].ClusterID != c.id {
			v.identHis[k] = append(chain, identVersion{Version: c.profile.Version, ClusterID: c.id})
		}
	}
}

// removeCluster 摘除一个簇（其标识索引由调用方随后重建或覆盖）。
func (v *view) removeCluster(id string) {
	delete(v.clusters, id)
	for k, cid := range v.byIdent {
		if cid == id {
			delete(v.byIdent, k)
		}
	}
}

// dropHistory 删除簇的全部历史（用于删除权场景，派生画像必须被清除）。
func (v *view) dropHistory(clusterIDs ...string) {
	gone := map[string]bool{}
	for _, id := range clusterIDs {
		gone[id] = true
		delete(v.history, id)
		delete(v.clusters, id)
	}
	for k, chain := range v.identHis {
		kept := chain[:0]
		dirty := false
		for _, iv := range chain {
			if gone[iv.ClusterID] {
				dirty = true
				continue
			}
			kept = append(kept, iv)
		}
		if dirty {
			if len(kept) == 0 {
				delete(v.identHis, k)
				delete(v.byIdent, k)
			} else {
				v.identHis[k] = kept
				v.byIdent[k] = kept[len(kept)-1].ClusterID
			}
		}
	}
}

// profileAsOf 返回标识在 version 时点（含）之前最近一次的完整画像版本。
func (v *view) profileAsOf(identKey string, version uint64) (Profile, bool) {
	chain := v.identHis[identKey]
	clusterID := ""
	for _, iv := range chain { // 链升序，取最后一个 <= version 的簇
		if iv.Version > version {
			break
		}
		clusterID = iv.ClusterID
	}
	if clusterID == "" {
		return Profile{}, false
	}
	var out Profile
	found := false
	for _, p := range v.history[clusterID] {
		if p.Version > version {
			break
		}
		out, found = p, true
	}
	return out, found
}
