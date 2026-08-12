package pan115

import (
	"context"
	"errors"
	"strings"
	"time"

	driver115 "github.com/SheltonZhu/115driver/pkg/driver"

	"litepan/internal/domain"
	"litepan/internal/driver"
)

const (
	ua115Browser = driver115.UA115Browser

	defaultOperationDelayMS = 800
	listPageSize            = int64(1150)

	downloadPartSize    = 10 * 1024 * 1024
	downloadConcurrency = 2
	downloadLinkTTL     = 30 * time.Minute

	singlePartUploadLimit = 5 * 1024 * 1024 * 1024
)

// beforeCall 在平台 API 请求前等待账号级间隔门。
func (d *Driver) beforeCall(ctx context.Context) error {
	return driver.WaitRequestInterval(ctx, d.intervalGate, defaultOperationDelayMS)
}

// mapLibraryError 将 115driver 库错误映射为 LitePan 领域错误。
func mapLibraryError(err error) error {
	if err == nil {
		return nil
	}
	if ae, ok := domain.AsAppError(err); ok {
		return ae
	}
	switch {
	case errors.Is(err, driver115.ErrBadCookie),
		errors.Is(err, driver115.ErrNotLogin),
		errors.Is(err, driver115.ErrCredentialInvalid),
		errors.Is(err, driver115.ErrDoesLoggedOut),
		errors.Is(err, driver115.ErrSessionExited):
		return domain.Errorf(domain.CodeAuthExpired, "115 认证失败：%s", err.Error())
	case errors.Is(err, driver115.ErrOfflineNoTimes):
		return domain.Errorf(domain.CodeRateLimited, "115 离线下载额度已用完：%s", err.Error())
	case errors.Is(err, driver115.ErrNotExist),
		errors.Is(err, driver115.ErrPickCodeNotExist),
		errors.Is(err, driver115.ErrPickCodeIsNotExistOrHasDeleted):
		return domain.Errorf(domain.CodeNotFound, "115 目标不存在：%s", err.Error())
	case errors.Is(err, driver115.ErrExist):
		return domain.Errorf(domain.CodeValidation, "115 目标已存在：%s", err.Error())
	case errors.Is(err, driver115.ErrDownloadFileTooBig):
		return domain.Errorf(domain.CodeDriverError, "115 文件过大无法下载：%s", err.Error())
	default:
		msg := strings.TrimSpace(err.Error())
		if msg == "" {
			msg = "未知错误"
		}
		return domain.Errorf(domain.CodeDriverError, "115 API 错误：%s", msg)
	}
}
