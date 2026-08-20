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
			name: "递归任务命中子目录",
			path: "/媒体/电影/新电影A",
			task: &domain.StrmTask{Path: "/媒体/电影", Recursive: true},
			expect: true,
		},
		{
			name: "递归任务精确命中根目录",
			path: "/媒体/电影",
			task: &domain.StrmTask{Path: "/媒体/电影", Recursive: true},
			expect: true,
		},
		{
			name: "递归任务不在作用域",
			path: "/媒体/剧集/新剧",
			task: &domain.StrmTask{Path: "/媒体/电影", Recursive: true},
			expect: false,
		},
		{
			name: "非递归任务仅精确命中",
			path: "/媒体/电影",
			task: &domain.StrmTask{Path: "/媒体/电影", Recursive: false},
			expect: true,
		},
		{
			name: "非递归任务子目录不命中",
			path: "/媒体/电影/新电影A",
			task: &domain.StrmTask{Path: "/媒体/电影", Recursive: false},
			expect: false,
		},
		{
			name: "任务根为整个空间",
			path: "/任意/目录",
			task: &domain.StrmTask{Path: "", Recursive: true},
			expect: true,
		},
		{
			name: "前缀陷阱：/a/b 不命中 /a/bc",
			path: "/媒体/电影2/内容",
			task: &domain.StrmTask{Path: "/媒体/电影", Recursive: true},
			expect: false,
		},
		{
			name: "根目录尾斜杠归一化",
			path: "/媒体/电影/",
			task: &domain.StrmTask{Path: "/媒体/电影", Recursive: true},
			expect: true,
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
