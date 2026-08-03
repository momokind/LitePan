package strm

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"litepan/internal/settings"
	"litepan/internal/store"
)

func TestEffectiveStrmDirHonorsSettingAndFallsBack(t *testing.T) {
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
		StrmDir:  "/startup/strm",
		Log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	if got := svc.effectiveStrmDir(); got != "/startup/strm" {
		t.Fatalf("未设置 strm_dir 时应回落启动目录，got %q", got)
	}

	if err := settingsSvc.Update(ctx, map[string]string{settings.KeyStrmDir: "/custom/strm"}); err != nil {
		t.Fatal(err)
	}
	if got := svc.effectiveStrmDir(); got != "/custom/strm" {
		t.Fatalf("设置 strm_dir 后应返回该目录，got %q", got)
	}

	if err := settingsSvc.Update(ctx, map[string]string{settings.KeyStrmDir: ""}); err != nil {
		t.Fatal(err)
	}
	if got := svc.effectiveStrmDir(); got != "/startup/strm" {
		t.Fatalf("清空 strm_dir 后应回落启动目录，got %q", got)
	}
}

func TestEffectiveStrmDirPackageHelper(t *testing.T) {
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

	if got := EffectiveStrmDir(nil, "/fallback"); got != "/fallback" {
		t.Fatalf("settings 为 nil 时返回 fallback，got %q", got)
	}
	if got := EffectiveStrmDir(settingsSvc, "/fallback"); got != "/fallback" {
		t.Fatalf("未配置时返回 fallback，got %q", got)
	}
	if err := settingsSvc.Update(ctx, map[string]string{settings.KeyStrmDir: "/custom"}); err != nil {
		t.Fatal(err)
	}
	if got := EffectiveStrmDir(settingsSvc, "/fallback"); got != "/custom" {
		t.Fatalf("配置后返回设置值，got %q", got)
	}
}
