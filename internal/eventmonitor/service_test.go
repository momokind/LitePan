package eventmonitor

import (
	"testing"
	"time"

	"litepan/internal/domain"
)

func TestMatchTaskScope(t *testing.T) {
	cases := []struct {
		name   string
		path   string
		task   *domain.StrmTask
		expect bool
	}{
		{
			name:   "递归任务命中子目录",
			path:   "/媒体/电影/新电影A",
			task:   &domain.StrmTask{Path: "/媒体/电影", Recursive: true},
			expect: true,
		},
		{
			name:   "递归任务精确命中根目录",
			path:   "/媒体/电影",
			task:   &domain.StrmTask{Path: "/媒体/电影", Recursive: true},
			expect: true,
		},
		{
			name:   "递归任务不在作用域",
			path:   "/媒体/剧集/新剧",
			task:   &domain.StrmTask{Path: "/媒体/电影", Recursive: true},
			expect: false,
		},
		{
			name:   "非递归任务仅精确命中",
			path:   "/媒体/电影",
			task:   &domain.StrmTask{Path: "/媒体/电影", Recursive: false},
			expect: true,
		},
		{
			name:   "非递归任务子目录不命中",
			path:   "/媒体/电影/新电影A",
			task:   &domain.StrmTask{Path: "/媒体/电影", Recursive: false},
			expect: false,
		},
		{
			name:   "任务根为整个空间",
			path:   "/任意/目录",
			task:   &domain.StrmTask{Path: "", Recursive: true},
			expect: true,
		},
		{
			name:   "前缀陷阱：/a/b 不命中 /a/bc",
			path:   "/媒体/电影2/内容",
			task:   &domain.StrmTask{Path: "/媒体/电影", Recursive: true},
			expect: false,
		},
		{
			name:   "根目录尾斜杠归一化",
			path:   "/媒体/电影/",
			task:   &domain.StrmTask{Path: "/媒体/电影", Recursive: true},
			expect: true,
		},
		{
			name:   "前导斜杠差异：事件路径无斜杠、任务路径带斜杠",
			path:   "媒体/电影",
			task:   &domain.StrmTask{Path: "/媒体/电影", Recursive: true},
			expect: true,
		},
		{
			name:   "前导斜杠差异：无斜杠事件路径命中子目录",
			path:   "媒体/电影/新电影A",
			task:   &domain.StrmTask{Path: "/媒体/电影", Recursive: true},
			expect: true,
		},
		{
			name:   "前导斜杠差异：无斜杠事件路径不命中别的根",
			path:   "媒体/电影2/内容",
			task:   &domain.StrmTask{Path: "/媒体/电影", Recursive: true},
			expect: false,
		},
	}
	for _, c := range cases {
		if got := matchTaskScope(c.path, c.task); got != c.expect {
			t.Errorf("%s: matchTaskScope(%q) = %v, want %v", c.name, c.path, got, c.expect)
		}
	}
}

func TestUniqueParentIDs(t *testing.T) {
	events := []domain.OperationEvent{
		{ParentID: "p1"},
		{ParentID: "p1"},
		{ParentID: "  p2  "},
		{ParentID: ""},
		{ParentID: "0"},
		{ParentID: "p3"},
	}
	got := uniqueParentIDs(events)
	if len(got) != 3 {
		t.Fatalf("expected 3 unique ids, got %v", got)
	}
	seen := map[string]bool{}
	for _, id := range got {
		if seen[id] {
			t.Errorf("duplicate id %q", id)
		}
		seen[id] = true
	}
	for _, want := range []string{"p1", "p2", "p3"} {
		if !seen[want] {
			t.Errorf("missing id %q in %v", want, got)
		}
	}
}

func TestPathCacheTTL(t *testing.T) {
	c := newPathCache(50 * time.Millisecond)
	c.set(1, "p1", "/a/b", true)
	if path, ok := c.get(1, "p1"); !ok || path != "/a/b" {
		t.Fatalf("expected cached path, got %q %v", path, ok)
	}
	time.Sleep(80 * time.Millisecond)
	if _, ok := c.get(1, "p1"); ok {
		t.Fatal("expected expired cache entry")
	}
	// 失败结果也应缓存为 miss。
	c.set(2, "p9", "", false)
	if _, ok := c.get(2, "p9"); ok {
		t.Fatal("expected ok=false for failed entry")
	}
	// 清空单个账号。
	c.set(1, "p1", "/a/b", true)
	c.set(9, "p1", "/x", true)
	c.clearAccount(1)
	if _, ok := c.get(1, "p1"); ok {
		t.Fatal("expected account 1 cleared")
	}
	if path, ok := c.get(9, "p1"); !ok || path != "/x" {
		t.Fatalf("expected account 9 preserved, got %q %v", path, ok)
	}
}

func TestAcquireTriggerReadyTime(t *testing.T) {
	s := NewService(Options{})
	now := time.Now()
	if ok, ready := s.acquireTrigger(1, time.Minute, now); !ok || !ready.Equal(now) {
		t.Fatalf("first acquire should pass, got ok=%v ready=%v", ok, ready)
	}
	ok, ready := s.acquireTrigger(1, time.Minute, now.Add(30*time.Second))
	if ok {
		t.Fatal("second acquire within cooldown should be rejected")
	}
	if want := now.Add(time.Minute); !ready.Equal(want) {
		t.Fatalf("ready time = %v, want %v", ready, want)
	}
	if ok, _ := s.acquireTrigger(1, time.Minute, now.Add(time.Minute)); !ok {
		t.Fatal("acquire after cooldown should pass")
	}
}

func TestDeferTriggerMerge(t *testing.T) {
	s := NewService(Options{})
	earlier := time.Now().Add(time.Minute)
	later := time.Now().Add(5 * time.Minute)
	s.deferTrigger(7, 1, later, []string{"d1"}, false)
	s.deferTrigger(7, 1, earlier, []string{"d1", "d2"}, true)

	p := s.pending[7]
	if p == nil {
		t.Fatal("expected pending entry for task 7")
	}
	if !p.readyAt.Equal(earlier) {
		t.Fatalf("readyAt = %v, want earliest %v", p.readyAt, earlier)
	}
	if !p.invalidateAccount {
		t.Fatal("invalidateAccount should stick once any event sets it")
	}
	if len(p.dirIDs) != 2 {
		t.Fatalf("dirIDs = %v, want union of 2", p.dirIDs)
	}

	// 账号清理应移除该账号下的补触发登记。
	s.deferTrigger(8, 2, earlier, nil, false)
	s.CleanupAccount(nil, 1)
	if _, ok := s.pending[7]; ok {
		t.Fatal("expected task 7 pending cleared by CleanupAccount")
	}
	if _, ok := s.pending[8]; !ok {
		t.Fatal("expected task 8 pending preserved")
	}
}

func TestBackoffExponential(t *testing.T) {
	s := NewService(Options{})
	s.setBackoff(1)
	first := s.backoff[1].until
	s.setBackoff(1)
	second := s.backoff[1].until
	if d1, d2 := time.Until(first), time.Until(second); d2 <= d1 {
		t.Fatalf("backoff should grow: %v then %v", d1, d2)
	}
	if d := time.Until(s.backoff[1].until); d > backoffBase*4+time.Second {
		t.Fatalf("third backoff = %v, want ~%v", d, backoffBase*4)
	}
	s.clearBackoff(1)
	s.setBackoff(1)
	if d := time.Until(s.backoff[1].until); d > backoffBase+time.Second {
		t.Fatalf("backoff after clear = %v, want ~%v", d, backoffBase)
	}
}

func TestPathCacheNegativeTTL(t *testing.T) {
	c := newPathCache(time.Minute)
	c.set(1, "p9", "", false)
	if _, ok := c.get(1, "p9"); ok {
		t.Fatal("failed entry should be a miss value")
	}
	// 失败结果短缓存：把负 TTL 调短后应更快过期，让下一轮轮询尽快重试。
	c.negTTL = 30 * time.Millisecond
	c.set(1, "p8", "", false)
	time.Sleep(50 * time.Millisecond)
	if _, ok := c.get(1, "p8"); ok {
		t.Fatal("expired negative entry should be a miss")
	}
	// 正缓存 TTL 未受影响。
	c.set(1, "p1", "/a", true)
	if path, ok := c.get(1, "p1"); !ok || path != "/a" {
		t.Fatalf("positive entry should still hit, got %q %v", path, ok)
	}
}

func TestHasDirRenameEvent(t *testing.T) {
	if hasDirRenameEvent(nil) {
		t.Fatal("empty events should not match")
	}
	if hasDirRenameEvent([]domain.OperationEvent{{Type: domain.OperationEventRename}}) {
		t.Fatal("file rename should not match")
	}
	if !hasDirRenameEvent([]domain.OperationEvent{{Type: domain.OperationEventRename, IsDir: true}}) {
		t.Fatal("dir rename should match")
	}
}
