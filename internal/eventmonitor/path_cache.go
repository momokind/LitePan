package eventmonitor

import (
	"strconv"
	"sync"
	"time"
)

// pathCache 是 (账号,目录ID) → 完整路径 的短 TTL 内存缓存。
// 用于把事件父目录 id 翻译成远端路径以匹配 STRM 任务作用域，同时限制 get_info 补漏请求量。
type pathCache struct {
	mu  sync.Mutex
	m   map[string]pathCacheEntry
	ttl time.Duration
}

type pathCacheEntry struct {
	path string
	ok   bool
	at   time.Time
}

func newPathCache(ttl time.Duration) *pathCache {
	if ttl <= 0 {
		ttl = time.Minute
	}
	return &pathCache{m: make(map[string]pathCacheEntry), ttl: ttl}
}

func pathCacheKey(accountID int64, parentID string) string {
	return strconv.FormatInt(accountID, 10) + ":" + parentID
}

func (c *pathCache) get(accountID int64, parentID string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[pathCacheKey(accountID, parentID)]
	if !ok || time.Since(e.at) > c.ttl {
		return "", false
	}
	return e.path, e.ok
}

func (c *pathCache) set(accountID int64, parentID string, path string, ok bool) {
	c.mu.Lock()
	c.m[pathCacheKey(accountID, parentID)] = pathCacheEntry{path: path, ok: ok, at: time.Now()}
	c.mu.Unlock()
}

func (c *pathCache) clearAccount(accountID int64) {
	prefix := strconv.FormatInt(accountID, 10) + ":"
	c.mu.Lock()
	for k := range c.m {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			delete(c.m, k)
		}
	}
	c.mu.Unlock()
}
