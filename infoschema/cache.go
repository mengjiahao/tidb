// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package infoschema

import (
	"sort"
	"sync"

	"github.com/pingcap/tidb/metrics"
)

var (
	getLatestCounter  = metrics.InfoCacheCounters.WithLabelValues("get", "latest")
	getTSCounter      = metrics.InfoCacheCounters.WithLabelValues("get", "ts")
	getVersionCounter = metrics.InfoCacheCounters.WithLabelValues("get", "version")

	hitLatestCounter  = metrics.InfoCacheCounters.WithLabelValues("hit", "latest")
	hitTSCounter      = metrics.InfoCacheCounters.WithLabelValues("hit", "ts")
	hitVersionCounter = metrics.InfoCacheCounters.WithLabelValues("hit", "version")
)

// InfoCache handles information schema, including getting and setting.
// The cache behavior, however, is transparent and under automatic management.
// It only promised to cache the infoschema, if it is newer than all the cached.
type InfoCache struct {
	// 读写锁 用于 DDL 更新
	mu sync.RWMutex
	// cache is sorted by SchemaVersion in descending order
	// 注意 历史的schema真的是全部保存着！！！
	cache []InfoSchema
	// record SnapshotTS of the latest schema Insert.
	maxUpdatedSnapshotTS uint64
}

// NewCache creates a new InfoCache.
func NewCache(capcity int) *InfoCache {
	return &InfoCache{cache: make([]InfoSchema, 0, capcity)}
}

// GetLatest gets the newest information schema.
// cache[0] 获取 schema cache，注意获取的是指针
func (h *InfoCache) GetLatest() InfoSchema {
	// 注意加 读写锁的读锁
	h.mu.RLock()
	defer h.mu.RUnlock()
	getLatestCounter.Inc()
	if len(h.cache) > 0 {
		hitLatestCounter.Inc()
		// 用的是 version最小的？
		return h.cache[0]
	}
	return nil
}

// 注意 InfoCache 上了读锁;
// GetByVersion gets the information schema based on schemaVersion. Returns nil if it is not loaded.
func (h *InfoCache) GetByVersion(version int64) InfoSchema {
	h.mu.RLock()
	defer h.mu.RUnlock()
	getVersionCounter.Inc()
	i := sort.Search(len(h.cache), func(i int) bool {
		return h.cache[i].SchemaMetaVersion() <= version
	})
	if i < len(h.cache) && h.cache[i].SchemaMetaVersion() == version {
		hitVersionCounter.Inc()
		return h.cache[i]
	}
	return nil
}

// GetBySnapshotTS gets the information schema based on snapshotTS.
// If the snapshotTS is new than maxUpdatedSnapshotTS, that's mean it can directly use
// the latest infoschema. otherwise, will return nil.
func (h *InfoCache) GetBySnapshotTS(snapshotTS uint64) InfoSchema {
	h.mu.RLock()
	defer h.mu.RUnlock()

	getTSCounter.Inc()
	if snapshotTS >= h.maxUpdatedSnapshotTS {
		if len(h.cache) > 0 {
			hitTSCounter.Inc()
			return h.cache[0]
		}
	}
	return nil
}

// Insert will **TRY** to insert the infoschema into the cache.
// It only promised to cache the newest infoschema.
// It returns 'true' if it is cached, 'false' otherwise.
func (h *InfoCache) Insert(is InfoSchema, snapshotTS uint64) bool {
	// 上读写锁的写锁
	h.mu.Lock()
	defer h.mu.Unlock()

	// 注意 cache[] 可能有多个 version 的 schema，根据 version 排序
	version := is.SchemaMetaVersion()
	i := sort.Search(len(h.cache), func(i int) bool {
		return h.cache[i].SchemaMetaVersion() <= version
	})

	if h.maxUpdatedSnapshotTS < snapshotTS {
		h.maxUpdatedSnapshotTS = snapshotTS
	}

	// cached entry
	if i < len(h.cache) && h.cache[i].SchemaMetaVersion() == version {
		return true
	}

	// 最新 version 的 schema 添加到 cache；
	// cache 只增加 不减少？
	if len(h.cache) < cap(h.cache) {
		// has free space, grown the slice
		h.cache = h.cache[:len(h.cache)+1]
		copy(h.cache[i+1:], h.cache[i:])
		h.cache[i] = is
		return true
	} else if i < len(h.cache) {
		// drop older schema
		copy(h.cache[i+1:], h.cache[i:])
		h.cache[i] = is
		return true
	}
	// older than all cached schemas, refuse to cache it
	return false
}
