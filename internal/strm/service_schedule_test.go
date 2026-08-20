package strm

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"litepan/internal/domain"
	"litepan/internal/settings"
	"litepan/internal/store"
)

func testService(t *testing.T) (*Service, *store.Store) {
	t.Helper()
	ctx := context.Background()
	db, err := store.Open(ctx, store.Options{Memory: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	st := store.New(db)
	settingsSvc, err := settings.New(ctx, st.Configs)
	if err != nil {
		t.Fatal(err)
	}
	svc := NewService(ServiceOptions{
		Repo:     st.StrmTasks,
		Settings: settingsSvc,
		Log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	return svc, st
}

type reciprocalRetentionBusy struct {
	other RunningAccountLister
}

func (r reciprocalRetentionBusy) GetRunningAccountIDs() []int64 {
	if r.other != nil {
		_ = r.other.GetRunningAccountIDs()
	}
	return []int64{7}
}

type constBusy struct{ ids []int64 }

func (c constBusy) GetRunningAccountIDs() []int64 { return c.ids }

func TestRunTaskNowEnqueuesWhenBusy(t *testing.T) {
	cases := []struct {
		name    string
		setBusy func(svc *Service, accID int64)
		wantMsg string
	}{
		{"retention", func(svc *Service, accID int64) { svc.SetRetentionBusyChecker(constBusy{ids: []int64{accID}}) }, "缓存保持"},
		{"organize", func(svc *Service, accID int64) { svc.SetOrganizeBusyChecker(constBusy{ids: []int64{accID}}) }, "媒体整理"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			svc, st := testService(t)
			ctx := context.Background()
			accID, err := st.Accounts.Create(ctx, &domain.Account{Name: "acc", DriverType: "localfs", IsActive: true})
			if err != nil {
				t.Fatal(err)
			}
			task := &domain.StrmTask{Name: "t", AccountID: accID, Path: "/dir", Recursive: true}
			id, err := st.StrmTasks.Create(ctx, task)
			if err != nil {
				t.Fatal(err)
			}
			c.setBusy(svc, accID)
			got, err := svc.RunTaskNow(ctx, id, domain.StrmRunModeAuto)
			if err == nil || !strings.Contains(err.Error(), c.wantMsg) || !strings.Contains(err.Error(), "已加入队列") {
				t.Fatalf("err = %v，期望包含 %q 与 '已加入队列'", err, c.wantMsg)
			}
			if got != nil {
				t.Fatalf("期望返回 nil 任务，实际 %+v", got)
			}
			svc.mu.Lock()
			defer svc.mu.Unlock()
			if svc.pendingRun[id] != domain.StrmRunModeAuto {
				t.Fatalf("pendingRun 未设置")
			}
			if !svc.dirtyAccounts[accID] {
				t.Fatalf("dirtyAccounts 未设置")
			}
		})
	}
}

func TestShouldRunTriggersPendingRun(t *testing.T) {
	svc, _ := testService(t)
	svc.mu.Lock()
	svc.pendingRun[1] = domain.StrmRunModeAuto
	svc.mu.Unlock()
	task := &domain.StrmTask{ID: 1, AccountID: 1}
	if !svc.shouldRun(task, time.Now()) {
		t.Fatal("存在待执行请求且无互斥时应触发执行")
	}
}

func TestShouldRunCrossBusyCheckNoDeadlock(t *testing.T) {
	svc, _ := testService(t)
	svc.SetRetentionBusyChecker(reciprocalRetentionBusy{other: svc})

	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		svc.mu.Lock()
		time.Sleep(200 * time.Millisecond)
		svc.mu.Unlock()
	}()
	go func() {
		defer wg.Done()
		task := &domain.StrmTask{ID: 1, AccountID: 7, LastScan: time.Now().Add(-2 * time.Hour)}
		svc.shouldRun(task, time.Now())
	}()
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("shouldRun cross busy check deadlocked")
	}
}

func TestTaskRunContextHasNoFixedDeadline(t *testing.T) {
	ctx, cancel := taskRunContext(context.Background())
	if _, ok := ctx.Deadline(); ok {
		cancel()
		t.Fatal("STRM 任务不应有固定执行期限")
	}

	cancel()
	<-ctx.Done()
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatalf("取消任务后错误 = %v，期望 context.Canceled", ctx.Err())
	}
}

func TestTaskStartLimitMatchesLegacyScheduler(t *testing.T) {
	svc, _ := testService(t)

	svc.mu.Lock()
	svc.running[1] = true
	svc.runningAccounts[7] = struct{}{}
	if svc.canStartTaskLocked(&domain.StrmTask{ID: 2, AccountID: 7}, 3) {
		t.Fatal("同一账号的 STRM 任务应串行")
	}
	if !svc.canStartTaskLocked(&domain.StrmTask{ID: 2, AccountID: 8}, 3) {
		t.Fatal("不同账号且未达到全局上限时应允许并发")
	}
	svc.running[2] = true
	svc.running[3] = true
	if svc.canStartTaskLocked(&domain.StrmTask{ID: 4, AccountID: 9}, 3) {
		t.Fatal("达到全局任务并发上限后应等待")
	}
	svc.mu.Unlock()
}

func TestShouldRunUsesGlobalIntervalForLegacyTasks(t *testing.T) {
	svc, _ := testService(t)
	if err := svc.settings.Update(context.Background(), map[string]string{
		settings.KeyStrmDefaultScanInterval: "360",
	}); err != nil {
		t.Fatal(err)
	}
	task := &domain.StrmTask{
		ID:           1,
		AccountID:    1,
		ScanInterval: 10, // 历史任务里固化过的旧值
		LastScan:     time.Now().Add(-20 * time.Minute),
	}
	if svc.shouldRun(task, time.Now()) {
		t.Fatal("全局扫描间隔应优先于历史任务级间隔，20 分钟后不应触发")
	}
}

func TestShouldRunFallsBackToLegacyTaskIntervalWhenGlobalMissing(t *testing.T) {
	svc, _ := testService(t)
	svc.settings = nil
	task := &domain.StrmTask{
		ID:           1,
		AccountID:    1,
		ScanInterval: 10,
		LastScan:     time.Now().Add(-20 * time.Minute),
	}
	if !svc.shouldRun(task, time.Now()) {
		t.Fatal("全局配置缺失时应回退历史任务级间隔，避免升级后停调度")
	}
}
