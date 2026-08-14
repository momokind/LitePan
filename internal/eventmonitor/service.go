package eventmonitor

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"litepan/internal/cache"
	"litepan/internal/domain"
	"litepan/internal/driver"
	"litepan/internal/eventbus"
	"litepan/internal/settings"
	"litepan/internal/strm"
)

// 启动退避与轮询配置。
const (
	startupDelay    = 30 * time.Second
	refreshPeriod   = 60 * time.Second // 周期性重扫受管账号的间隔
	defaultPollSec  = 60
	defaultCooldown = 2 * time.Minute
	// transientBackoff 是事件接口被 115 风控（如 405）后的账号级退避时长。
	transientBackoff = 5 * time.Minute
	// eventPollExtraMS 与 115 驱动默认 800ms 闸门叠加，强制事件接口两次调用 ≥5s。
	eventPollExtraMS = 4200
)

// Options 是事件监控服务的依赖。
type Options struct {
	Accounts  domain.AccountRepository
	Cursors   domain.EventMonitorCursorRepository
	StrmTasks domain.StrmTaskRepository
	Drivers   driver.Provider
	Strm      *strm.Service
	Cache     *cache.Service
	Settings  *settings.Service
	Log       *slog.Logger
}

// Service 轮询支持 OperationEventProvider 的驱动账号，把远端增删改事件转为 STRM 重扫触发。
type Service struct {
	accounts  domain.AccountRepository
	cursors   domain.EventMonitorCursorRepository
	strmTasks domain.StrmTaskRepository
	drivers   driver.Provider
	strm      *strm.Service
	cache     *cache.Service
	settings  *settings.Service
	log       *slog.Logger

	mu             sync.Mutex
	managed        map[int64]struct{}
	pendingTrigger map[int64]bool
	lastTrigger    map[int64]time.Time
	backoffUntil   map[int64]time.Time
	recalc         chan struct{}
	appCtx         context.Context
	started        bool
	startupReadyAt time.Time
}

// NewService 构造事件监控服务。
func NewService(opts Options) *Service {
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}
	return &Service{
		accounts:       opts.Accounts,
		cursors:        opts.Cursors,
		strmTasks:      opts.StrmTasks,
		drivers:        opts.Drivers,
		strm:           opts.Strm,
		cache:          opts.Cache,
		settings:       opts.Settings,
		log:            log,
		managed:        make(map[int64]struct{}),
		pendingTrigger: make(map[int64]bool),
		lastTrigger:    make(map[int64]time.Time),
		backoffUntil:   make(map[int64]time.Time),
		recalc:         make(chan struct{}, 1),
	}
}

// Start 启动轮询循环；启用开关关闭时循环空转，开启后立即生效。
func (s *Service) Start(ctx context.Context) {
	if s == nil || s.accounts == nil || s.cursors == nil || s.drivers == nil {
		return
	}
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return
	}
	s.started = true
	s.appCtx = ctx
	s.startupReadyAt = time.Now().Add(startupDelay)
	s.mu.Unlock()
	s.log.Info("事件监控已启动")
	go s.loop(ctx)
}

func (s *Service) loop(ctx context.Context) {
	s.drainRecalc()
	s.waitStartup(ctx)
	s.refreshManaged(ctx)
	lastRefresh := time.Now()

	ticker := time.NewTicker(s.pollInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.recalc:
			s.refreshManaged(ctx)
			lastRefresh = time.Now()
		case <-ticker.C:
			if !s.enabled() {
				continue
			}
			if time.Since(lastRefresh) > refreshPeriod {
				s.refreshManaged(ctx)
				lastRefresh = time.Now()
			}
			s.pollOnce(ctx)
		}
	}
}

// LoadManagedAccounts 重建受管账号集合：活跃、驱动支持事件能力、且存在未暂停的 STRM 任务。
func (s *Service) LoadManagedAccounts(ctx context.Context) {
	s.refreshManaged(ctx)
}

func (s *Service) refreshManaged(ctx context.Context) {
	if s.accounts == nil || s.strmTasks == nil {
		return
	}
	accounts, err := s.accounts.List(ctx)
	if err != nil {
		s.log.Warn("事件监控账号列表获取失败", "err", err)
		return
	}
	activeTaskAccounts := make(map[int64]struct{})
	if tasks, err := s.strmTasks.List(ctx); err == nil {
		for _, t := range tasks {
			// 非暂停的任务都纳入受管：扫描进行中状态为 running，仍需持续轮询推进游标。
			if t != nil && t.AccountID > 0 && t.Status != domain.StrmStatusPaused {
				activeTaskAccounts[t.AccountID] = struct{}{}
			}
		}
	}

	s.mu.Lock()
	s.managed = make(map[int64]struct{})
	for _, a := range accounts {
		if a == nil || !a.IsActive {
			continue
		}
		if !s.supportsEvents(a.DriverType) {
			continue
		}
		if _, ok := activeTaskAccounts[a.ID]; !ok {
			continue
		}
		s.managed[a.ID] = struct{}{}
	}
	s.mu.Unlock()
}

func (s *Service) supportsEvents(driverType string) bool {
	drv, ok := driver.New(driverType)
	if !ok {
		return false
	}
	_, ok = drv.(driver.OperationEventProvider)
	return ok
}

func (s *Service) managedIDs() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]int64, 0, len(s.managed))
	for id := range s.managed {
		out = append(out, id)
	}
	return out
}

// TriggerRecalculation 请求重算受管账号（非阻塞）。
func (s *Service) TriggerRecalculation() {
	select {
	case s.recalc <- struct{}{}:
	default:
	}
}

// CleanupAccount 在账号被删除时清理游标并触发重算。
func (s *Service) CleanupAccount(ctx context.Context, accountID int64) {
	s.mu.Lock()
	delete(s.managed, accountID)
	delete(s.pendingTrigger, accountID)
	delete(s.lastTrigger, accountID)
	delete(s.backoffUntil, accountID)
	s.mu.Unlock()
	if s.cursors != nil {
		_ = s.cursors.Delete(ctx, accountID)
	}
	s.TriggerRecalculation()
}

func (s *Service) pollOnce(ctx context.Context) {
	for _, accountID := range s.managedIDs() {
		s.pollAccount(ctx, accountID)
	}
}

func (s *Service) pollAccount(ctx context.Context, accountID int64) {
	if s.inBackoff(accountID) {
		return
	}
	drv, err := s.drivers.Get(ctx, accountID)
	if err != nil {
		s.log.Warn("事件监控获取驱动失败", "account_id", accountID, "err", err)
		return
	}
	prov, ok := drv.(driver.OperationEventProvider)
	if !ok {
		s.unmanage(accountID)
		return
	}

	cursor := ""
	if c, err := s.cursors.Get(ctx, accountID); err == nil && c != nil {
		cursor = c.LastEventID
	}

	pollCtx := driver.WithExtraAPIDelay(ctx, eventPollExtraMS)
	events, next, err := prov.RecentOperations(pollCtx, cursor, 1000)
	if err != nil {
		s.logPollError(accountID, err)
		if isTransientError(err) {
			s.setBackoff(accountID)
		}
		return
	}
	s.clearBackoff(accountID)

	if cursor == "" {
		// 基线初始化：只建立游标，不回放历史事件。
		if next != "" {
			s.upsertCursor(ctx, accountID, next)
		}
		return
	}
	if next != "" && next != cursor {
		s.upsertCursor(ctx, accountID, next)
	}
	if len(events) == 0 {
		return
	}
	// 失效受影响目录的缓存，确保随后的 STRM 重扫看到最新变更（而非陈旧缓存）。
	s.invalidateEventDirs(accountID, events)
	s.mu.Lock()
	s.pendingTrigger[accountID] = true
	s.mu.Unlock()
	s.maybeTrigger(ctx, accountID)
}

// invalidateEventDirs 失效事件涉及的父目录缓存键。
func (s *Service) invalidateEventDirs(accountID int64, events []domain.OperationEvent) {
	if s.cache == nil || len(events) == 0 {
		return
	}
	seen := make(map[string]struct{})
	for _, e := range events {
		parent := cache.NormalizeDirParentID(e.ParentID)
		if parent == "" {
			continue
		}
		if _, ok := seen[parent]; ok {
			continue
		}
		seen[parent] = struct{}{}
		cache.InvalidateDirKeys(s.cache, accountID, parent)
	}
}

func (s *Service) maybeTrigger(ctx context.Context, accountID int64) {
	cooldown := s.triggerCooldown()
	s.mu.Lock()
	if !s.pendingTrigger[accountID] {
		s.mu.Unlock()
		return
	}
	if last := s.lastTrigger[accountID]; !last.IsZero() && time.Since(last) < cooldown {
		s.mu.Unlock()
		return
	}
	s.pendingTrigger[accountID] = false
	s.lastTrigger[accountID] = time.Now()
	s.mu.Unlock()
	s.trigger(ctx, accountID)
}

func (s *Service) trigger(ctx context.Context, accountID int64) {
	if s.strm == nil {
		return
	}
	s.strm.OnFileMutated(ctx, eventbus.FileMutated{AccountID: accountID, Op: "create"})
	s.log.Info("事件监控触发 STRM 重扫", "account_id", accountID)
}

func (s *Service) upsertCursor(ctx context.Context, accountID int64, lastEventID string) {
	if s.cursors == nil || lastEventID == "" {
		return
	}
	if err := s.cursors.Upsert(ctx, &domain.EventMonitorCursor{AccountID: accountID, LastEventID: lastEventID}); err != nil {
		s.log.Warn("事件监控游标保存失败", "account_id", accountID, "err", err)
	}
}

func (s *Service) unmanage(accountID int64) {
	s.mu.Lock()
	delete(s.managed, accountID)
	delete(s.pendingTrigger, accountID)
	delete(s.lastTrigger, accountID)
	delete(s.backoffUntil, accountID)
	s.mu.Unlock()
}

func (s *Service) inBackoff(accountID int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return time.Now().Before(s.backoffUntil[accountID])
}

func (s *Service) setBackoff(accountID int64) {
	s.mu.Lock()
	s.backoffUntil[accountID] = time.Now().Add(transientBackoff)
	s.mu.Unlock()
}

func (s *Service) clearBackoff(accountID int64) {
	s.mu.Lock()
	delete(s.backoffUntil, accountID)
	s.mu.Unlock()
}

// isTransientError 判定轮询错误是否为可自动恢复的瞬态错误（非认证失效）。
func isTransientError(err error) bool {
	if ae, ok := domain.AsAppError(err); ok {
		return ae.Code != domain.CodeAuthExpired
	}
	return true
}

func (s *Service) logPollError(accountID int64, err error) {
	if ae, ok := domain.AsAppError(err); ok && ae.Code == domain.CodeAuthExpired {
		s.log.Debug("事件监控账号认证失效", "account_id", accountID, "err", err)
		return
	}
	s.log.Warn("事件监控轮询失败", "account_id", accountID, "err", err)
}

func (s *Service) drainRecalc() {
	for {
		select {
		case <-s.recalc:
		default:
			return
		}
	}
}

func (s *Service) waitStartup(ctx context.Context) {
	s.mu.Lock()
	readyAt := s.startupReadyAt
	s.mu.Unlock()
	if rem := time.Until(readyAt); rem > 0 {
		timer := time.NewTimer(rem)
		defer timer.Stop()
		select {
		case <-ctx.Done():
		case <-timer.C:
		}
	}
}

func (s *Service) enabled() bool {
	return s.settings != nil && s.settings.Bool(settings.KeyStrmEventMonitorEnabled)
}

func (s *Service) pollInterval() time.Duration {
	sec := defaultPollSec
	if s.settings != nil {
		sec = s.settings.Int(settings.KeyStrmEventMonitorIntervalSec)
	}
	if sec < 30 {
		sec = 30
	}
	return time.Duration(sec) * time.Second
}

func (s *Service) triggerCooldown() time.Duration {
	min := defaultCooldown
	if s.settings != nil {
		sec := s.settings.Int(settings.KeyStrmEventTriggerCooldownMin)
		if sec >= 1 {
			min = time.Duration(sec) * time.Minute
		}
	}
	return min
}
