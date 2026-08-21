package strm

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"litepan/internal/core/driverexec"
	"litepan/internal/domain"
	"litepan/internal/driver"
	"litepan/internal/file"
)

// branchGapTestDriver 按脚本返回目录列表，模拟分支检查扫描的远端树。
type branchGapTestDriver struct {
	mu     sync.Mutex
	lists  map[string][]domain.FileItem
	listed []string
}

func (d *branchGapTestDriver) Config() driver.Config      { return driver.Config{Name: "branch-gap-test"} }
func (d *branchGapTestDriver) GetAddition() any           { return &struct{}{} }
func (d *branchGapTestDriver) Init(context.Context) error { return nil }
func (d *branchGapTestDriver) Drop(context.Context) error { return nil }
func (d *branchGapTestDriver) Ping(context.Context) error { return nil }
func (d *branchGapTestDriver) ListFiles(_ context.Context, parentID string) ([]domain.FileItem, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.listed = append(d.listed, parentID)
	return d.lists[parentID], nil
}

func (d *branchGapTestDriver) hasListed(parentID string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, p := range d.listed {
		if p == parentID {
			return true
		}
	}
	return false
}

// memBranchRepo 是内存版分支仓储，记录续期调用。
type memBranchRepo struct {
	mu       sync.Mutex
	branches []*domain.StrmBranch
	nextID   int64
	renewed  int
}

func (r *memBranchRepo) Create(_ context.Context, b *domain.StrmBranch) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextID++
	b.ID = r.nextID
	cp := *b
	r.branches = append(r.branches, &cp)
	return b.ID, nil
}

func (r *memBranchRepo) Update(_ context.Context, b *domain.StrmBranch) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, existing := range r.branches {
		if existing.ID == b.ID {
			cp := *b
			r.branches[i] = &cp
			return nil
		}
	}
	return domain.Errorf(domain.CodeNotFound, "分支不存在")
}

func (r *memBranchRepo) Delete(_ context.Context, id int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, existing := range r.branches {
		if existing.ID == id {
			r.branches = append(r.branches[:i], r.branches[i+1:]...)
			return nil
		}
	}
	return nil
}

func (r *memBranchRepo) Get(_ context.Context, id int64) (*domain.StrmBranch, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, existing := range r.branches {
		if existing.ID == id {
			cp := *existing
			return &cp, nil
		}
	}
	return nil, domain.Errorf(domain.CodeNotFound, "分支不存在")
}

func (r *memBranchRepo) ListByTask(_ context.Context, taskID int64) ([]*domain.StrmBranch, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*domain.StrmBranch
	for _, existing := range r.branches {
		if existing.TaskID == taskID {
			cp := *existing
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (r *memBranchRepo) DeleteExpired(_ context.Context, taskID int64) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	kept := r.branches[:0]
	n := 0
	for _, existing := range r.branches {
		if existing.TaskID == taskID && !existing.ExpiresAt.IsZero() && existing.ExpiresAt.Before(now) {
			n++
			continue
		}
		kept = append(kept, existing)
	}
	r.branches = kept
	return n, nil
}

func (r *memBranchRepo) RenewTemporaryExpiry(_ context.Context, taskID int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.renewed++
	now := time.Now()
	for _, existing := range r.branches {
		if existing.TaskID == taskID && existing.BranchType == domain.StrmBranchTypeTemporary && existing.RetentionDays > 0 {
			existing.ExpiresAt = now.Add(time.Duration(existing.RetentionDays) * 24 * time.Hour)
		}
	}
	return nil
}

func (r *memBranchRepo) findTemporaryByParentID(parentID string) *domain.StrmBranch {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, existing := range r.branches {
		if existing.ParentID == parentID && existing.BranchType == domain.StrmBranchTypeTemporary {
			cp := *existing
			return &cp
		}
	}
	return nil
}

// TestSkippedDirWithNestedStrmReRegistersAsBranch 验证分支检查扫描的"已同步目录跳过"缺口修复：
// 本地已有嵌套 strm 的未注册目录被跳过时应补注册为临时分支，下一轮作为分支被重扫并同步新增文件。
func TestSkippedDirWithNestedStrmReRegistersAsBranch(t *testing.T) {
	root := t.TempDir()
	// 预置本地嵌套 strm：任务输出目录下"剧集A"已有 strm，模拟历史已同步子树。
	showDir := filepath.Join(root, "任务", "剧集A")
	if err := os.MkdirAll(showDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(showDir, "S01E01.mkv.strm"), []byte("http://example/old"), 0o644); err != nil {
		t.Fatal(err)
	}

	drv := &branchGapTestDriver{lists: map[string][]domain.FileItem{
		"lib":   {{ID: "show1", Name: "剧集A", IsDir: true}},
		"show1": {{ID: "f1", Name: "S01E02.mkv", Size: 1024}},
	}}
	files := file.NewService(driverexec.New(enhancedTestProvider{drv: drv}, nil), nil, nil, nil, nil, nil)
	branches := &memBranchRepo{branches: []*domain.StrmBranch{{
		TaskID:     1,
		AccountID:  1,
		ParentID:   "lib",
		Path:       "/库",
		BranchType: domain.StrmBranchTypeBase,
	}}}
	task := &domain.StrmTask{
		ID:                 1,
		AccountID:          1,
		ParentID:           "lib",
		Path:               "/库",
		Recursive:          true,
		BranchCheckEnabled: true,
		ScanMode:           domain.StrmScanModeIncrementalUpdate,
		Extensions:         "mkv",
		OutputFolder:       "任务",
	}
	deps := ScanDeps{Files: files, Branches: branches, StrmDir: root}

	// 第一轮：剧集A 因本地嵌套 strm 被跳过，但应补注册为临时分支。
	if _, err := ScanTask(context.Background(), task, deps, domain.StrmRunModeAuto); err != nil {
		t.Fatalf("首轮扫描失败: %v", err)
	}
	reg := branches.findTemporaryByParentID("show1")
	if reg == nil {
		t.Fatal("被跳过的目录未补注册为临时分支，将永久失联")
	}
	if reg.RelativePath != "剧集A" || reg.Path != "/库/剧集A" {
		t.Fatalf("补注册分支路径异常: %+v", reg)
	}
	if drv.hasListed("show1") {
		t.Fatal("首轮不应深入被跳过目录（保留跳过优化）")
	}
	if branches.renewed == 0 {
		t.Fatal("扫描成功后应调用临时分支续期")
	}

	// 第二轮：剧集A 已是分支，应被重扫并同步远端新增文件。
	result, err := ScanTask(context.Background(), task, deps, domain.StrmRunModeAuto)
	if err != nil {
		t.Fatalf("第二轮扫描失败: %v", err)
	}
	if !drv.hasListed("show1") {
		t.Fatal("注册为分支后目录仍未被重扫")
	}
	if result.GeneratedCount < 1 {
		t.Fatalf("远端新增文件未生成 strm: %+v", result)
	}
	// 命名约定为替换扩展名（S01E02.mkv → S01E02.strm）；
	// 预置的远端已不存在文件 S01E01.mkv.strm 应同时被对账清理。
	if _, err := os.Stat(filepath.Join(showDir, "S01E02.strm")); err != nil {
		t.Fatalf("新增 strm 未落盘: %v", err)
	}
	if _, err := os.Stat(filepath.Join(showDir, "S01E01.mkv.strm")); !os.IsNotExist(err) {
		t.Fatalf("远端已删除文件的 strm 未被清理: %v", err)
	}
}

// TestRenewTemporaryExpiryKeepsBranchAlive 验证续期后的临时分支不会被过期清理删除。
func TestRenewTemporaryExpiryKeepsBranchAlive(t *testing.T) {
	svc, st := testService(t)
	svc.branches = st.StrmBranches
	ctx := t.Context()

	accountID, err := st.Accounts.Create(ctx, &domain.Account{
		Name:       "测试账号",
		DriverType: "localfs",
		IsActive:   true,
	})
	if err != nil {
		t.Fatal(err)
	}
	taskID, err := st.StrmTasks.Create(ctx, &domain.StrmTask{
		Name:      "电视剧",
		AccountID: accountID,
		ParentID:  "library",
		Path:      "/云影音/电视剧",
		Status:    domain.StrmStatusActive,
	})
	if err != nil {
		t.Fatal(err)
	}
	branch, err := svc.CreateBranch(ctx, &domain.StrmBranch{
		TaskID:        taskID,
		ParentID:      "one-piece",
		Path:          "/云影音/电视剧/海贼王",
		Recursive:     true,
		RetentionDays: 30,
		BranchType:    domain.StrmBranchTypeTemporary,
	})
	if err != nil {
		t.Fatal(err)
	}

	// 模拟分支闲置到过期：直接把过期时间改为过去。
	branch.ExpiresAt = time.Now().Add(-time.Hour)
	if err := st.StrmBranches.Update(ctx, branch); err != nil {
		t.Fatal(err)
	}
	// 未续期时会被清理。
	if n, err := st.StrmBranches.DeleteExpired(ctx, taskID); err != nil || n != 1 {
		t.Fatalf("过期分支应被清理: n=%d err=%v", n, err)
	}

	branch.ID = 0
	branch.ExpiresAt = time.Time{}
	created, err := svc.CreateBranch(ctx, branch)
	if err != nil {
		t.Fatal(err)
	}
	created.ExpiresAt = time.Now().Add(-time.Hour)
	if err := st.StrmBranches.Update(ctx, created); err != nil {
		t.Fatal(err)
	}
	if err := st.StrmBranches.RenewTemporaryExpiry(ctx, taskID); err != nil {
		t.Fatal(err)
	}
	if n, err := st.StrmBranches.DeleteExpired(ctx, taskID); err != nil || n != 0 {
		t.Fatalf("续期后的分支不应被过期清理: n=%d err=%v", n, err)
	}
}
