package pan115

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"litepan/internal/domain"
)

// 验证 enableLifeEvents 发送正确的开关请求并在成功时返回 nil。
func TestEnableLifeEvents(t *testing.T) {
	old := lifeSetOptionURL
	defer func() { lifeSetOptionURL = old }()

	var gotForm string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotForm = r.FormValue("locus") + "," + r.FormValue("open_life")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"state":true}`))
	}))
	defer server.Close()
	lifeSetOptionURL = server.URL + "/setoption"

	d := &Driver{}
	d.cookie = "UID=1;CID=2;SEID=3"
	d.client = server.Client()
	if err := d.enableLifeEvents(context.Background()); err != nil {
		t.Fatalf("enableLifeEvents error: %v", err)
	}
	if gotForm != "1,1" {
		t.Errorf("setoption form = %q, want 1,1", gotForm)
	}
}

// 验证 enableLifeEvents 在开关设置失败时返回错误。
func TestEnableLifeEventsStateFalse(t *testing.T) {
	old := lifeSetOptionURL
	defer func() { lifeSetOptionURL = old }()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"state":false}`))
	}))
	defer server.Close()
	lifeSetOptionURL = server.URL + "/setoption"

	d := &Driver{}
	d.cookie = "UID=1;CID=2;SEID=3"
	d.client = server.Client()
	if err := d.enableLifeEvents(context.Background()); err == nil {
		t.Error("expected error when state=false")
	}
}

func TestMapBehaviorType(t *testing.T) {
	cases := []struct {
		typ  int
		want string
		ok   bool
	}{
		{1, domain.OperationEventUpload, true},
		{2, domain.OperationEventUpload, true},
		{14, domain.OperationEventCreate, true},
		{17, domain.OperationEventCreate, true},
		{18, domain.OperationEventCopy, true},
		{23, domain.OperationEventCopy, true},
		{22, domain.OperationEventDelete, true},
		{5, domain.OperationEventMove, true},
		{6, domain.OperationEventMove, true},
		{20, domain.OperationEventRename, true},
		{24, domain.OperationEventRename, true},
		{3, "", false},
		{7, "", false},
		{8, "", false},
		{19, "", false},
	}
	for _, c := range cases {
		got, ok := mapBehaviorType(c.typ)
		if got != c.want || ok != c.ok {
			t.Errorf("mapBehaviorType(%d) = %q,%v; want %q,%v", c.typ, got, ok, c.want, c.ok)
		}
	}
}

func TestEventIDValue(t *testing.T) {
	if got := eventIDValue("123"); got != 123 {
		t.Errorf("eventIDValue(123) = %d", got)
	}
	if got := eventIDValue(""); got != 0 {
		t.Errorf("eventIDValue(empty) = %d", got)
	}
	if got := eventIDValue("abc"); got != 0 {
		t.Errorf("eventIDValue(abc) = %d", got)
	}
}

// cannedEvents 返回按发生时间逆序的混合事件（含一条 browse 需被过滤）。
const cannedEvents = `{"state":true,"data":{"count":3,"next_page":false,"list":[
	{"id":"1003","type":22,"file_id":"f3","parent_id":"p1","file_name":"del.mp4","file_size":10,"update_time":1700000003},
	{"id":"1002","type":8,"file_id":"f2","parent_id":"p1","file_name":"browse.mp4","file_size":10,"update_time":1700000002},
	{"id":"1001","type":2,"file_id":"f1","parent_id":"p1","file_name":"new.mp4","file_size":10,"update_time":1700000001}
]}}`

func TestRecentOperationsBaselineAndCursor(t *testing.T) {
	old := behaviorDetailURL
	defer func() { behaviorDetailURL = old }()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(cannedEvents))
	}))
	defer server.Close()
	behaviorDetailURL = server.URL + "/behavior/detail"

	d := &Driver{}
	d.cookie = "UID=1;CID=2;SEID=3"
	d.client = server.Client()

	// 基线：只建立游标，不返回事件。
	events, next, err := d.RecentOperations(context.Background(), "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Errorf("baseline should return no events, got %d", len(events))
	}
	if next != "1003" {
		t.Errorf("baseline cursor = %q, want 1003", next)
	}

	// 从最新游标：无新事件，游标不回退。
	events, next, err = d.RecentOperations(context.Background(), "1003", 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Errorf("expected no new events, got %d", len(events))
	}
	if next != "1003" {
		t.Errorf("cursor should not regress, got %q", next)
	}

	// 从 1001：返回 1003(删除)，1002 是浏览事件被过滤，1001 触发停止。
	events, next, err = d.RecentOperations(context.Background(), "1001", 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event (delete), got %d: %+v", len(events), events)
	}
	if events[0].Type != domain.OperationEventDelete || events[0].FileID != "f3" {
		t.Errorf("unexpected event: %+v", events[0])
	}
	if next != "1003" {
		t.Errorf("cursor = %q, want 1003", next)
	}
}

// 验证：同文件多条事件均保留（按事件 id 去重而非 file_id）；目录改名事件标记 IsDir。
func TestRecentOperationsSameFileAndDirRename(t *testing.T) {
	old := behaviorDetailURL
	defer func() { behaviorDetailURL = old }()

	// id 2002（目录改名 type=20）与 2001（同文件 f1 的上传）均应返回。
	body := `{"state":true,"data":{"count":2,"next_page":false,"list":[
		{"id":"2002","type":20,"file_id":"d1","parent_id":"p0","file_name":"新名","update_time":1700000002},
		{"id":"2001","type":2,"file_id":"f1","parent_id":"p1","file_name":"a.mp4","update_time":1700000001}
	]}}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()
	behaviorDetailURL = server.URL + "/behavior/detail"

	d := &Driver{}
	d.cookie = "UID=1;CID=2;SEID=3"
	d.client = server.Client()

	events, next, err := d.RecentOperations(context.Background(), "1000", 1000)
	if err != nil {
		t.Fatal(err)
	}
	if next != "2002" {
		t.Errorf("cursor = %q, want 2002", next)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events (same file_id must not be deduped), got %d: %+v", len(events), events)
	}
	if !events[0].IsDir || events[0].Type != domain.OperationEventRename {
		t.Errorf("folder rename should be IsDir rename, got %+v", events[0])
	}
}

// 验证：事件列表同页内 id 非严格逆序时，游标之后的乱序新事件不被提前中断丢弃。
func TestRecentOperationsOutOfOrderPage(t *testing.T) {
	old := behaviorDetailURL
	defer func() { behaviorDetailURL = old }()

	// 逆序中出现旧事件 999（≤游标 1000）后，同页仍有新事件 1500。
	body := `{"state":true,"data":{"count":3,"next_page":false,"list":[
		{"id":"2000","type":2,"file_id":"f1","parent_id":"p1","file_name":"a.mp4","update_time":1700000003},
		{"id":"999","type":2,"file_id":"f2","parent_id":"p1","file_name":"old.mp4","update_time":1700000001},
		{"id":"1500","type":2,"file_id":"f3","parent_id":"p1","file_name":"b.mp4","update_time":1700000002}
	]}}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()
	behaviorDetailURL = server.URL + "/behavior/detail"

	d := &Driver{}
	d.cookie = "UID=1;CID=2;SEID=3"
	d.client = server.Client()

	events, next, err := d.RecentOperations(context.Background(), "1000", 1000)
	if err != nil {
		t.Fatal(err)
	}
	if next != "2000" {
		t.Errorf("cursor = %q, want 2000", next)
	}
	ids := map[string]bool{}
	for _, e := range events {
		ids[e.ID] = true
	}
	if !ids["2000"] || !ids["1500"] {
		t.Fatalf("out-of-order new event 1500 was dropped, got %v", ids)
	}
	if ids["999"] {
		t.Fatal("event at or below cursor should be filtered")
	}
}
