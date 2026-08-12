package pan115

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"time"

	driver115 "github.com/SheltonZhu/115driver/pkg/driver"

	"litepan/internal/domain"
	"litepan/internal/driver"
	"litepan/internal/httpx"
)

// Driver 是 115Cloud 驱动实例，基于 SheltonZhu/115driver 库以 Cookie 方式访问 115 网盘。
type Driver struct {
	add    Addition
	client *http.Client
	pan    *driver115.Pan115Client

	intervalGate driver.RequestIntervalGate
	persist      driver.AuthPersistFunc

	mu      sync.RWMutex
	cookie  string
	userID  int64
	userkey string

	pickMu sync.RWMutex
	pickBy map[string]string
}

var config = driver.Config{
	Name:                "115_cloud",
	DisplayName:         "115网盘",
	Description:         "115网盘 Cookie 接入（基于 115driver 库），支持文件管理、上传下载、离线下载等功能",
	CardTags:            []string{"Cookie", "支持302", "SHA1"},
	SortOrder:           2,
	AuthLabel:           "Cookie",
	CardColor:           "#22A7F0",
	CardLogo:            "/logos/115.png",
	DefaultRoot:         "0",
	AuthType:            driver.AuthCookie,
	HealthCheckInterval: 70 * time.Minute,
	ProvideHashes:       []string{"sha1"},
}

func New() driver.Driver { return &Driver{} }

func init() { driver.Register(New) }

func (d *Driver) Config() driver.Config { return config }

func (d *Driver) GetAddition() any { return &d.add }

func (d *Driver) Init(ctx context.Context) error {
	if d.pan == nil {
		if err := d.buildClient(ctx); err != nil {
			return err
		}
	}
	cookie := d.resolveCookie()
	d.mu.Lock()
	d.cookie = cookie
	d.mu.Unlock()
	if cookie == "" {
		return domain.Errorf(domain.CodeValidation, "Cookie 不能为空")
	}
	if err := d.ensureUserInfo(ctx); err != nil {
		return err
	}
	return d.Ping(ctx)
}

func (d *Driver) Drop(context.Context) error {
	if d.client != nil {
		httpx.CloseClient(d.client)
	}
	d.client = nil
	d.pan = nil
	return nil
}

// Ping 通过登录检查验证 Cookie 是否仍有效。
func (d *Driver) Ping(ctx context.Context) error {
	if err := d.beforeCall(ctx); err != nil {
		return err
	}
	if err := d.pan.LoginCheck(); err != nil {
		return mapLibraryError(err)
	}
	return nil
}

func (d *Driver) ExplainConnectionError(technical string, saving bool) string {
	prefix := "添加失败"
	if saving {
		prefix = "保存失败"
	}
	lower := strings.ToLower(technical)
	switch {
	case strings.Contains(technical, "Cookie 不能为空"):
		return prefix + "：请填写 115 网盘 Cookie（UID、CID、SEID 等）"
	case strings.Contains(technical, "bad cookie"),
		strings.Contains(technical, "认证失败"),
		strings.Contains(lower, "auth_expired"),
		strings.Contains(lower, "not login"),
		strings.Contains(lower, "sso"):
		return prefix + "：115 Cookie 无效或已过期，请重新登录 115 网页版抓取完整 Cookie"
	default:
		return ""
	}
}

func (d *Driver) ListFiles(ctx context.Context, parentID string) ([]domain.FileItem, error) {
	parent := d.normalizeParent(parentID)
	var items []domain.FileItem
	offset := int64(0)
	for {
		if err := d.beforeCall(ctx); err != nil {
			return nil, err
		}
		files, err := d.pan.ListPage(parent, offset, listPageSize)
		if err != nil {
			return nil, mapLibraryError(err)
		}
		if files == nil || len(*files) == 0 {
			break
		}
		for i := range *files {
			f := &(*files)[i]
			if strings.TrimSpace(f.FileID) != "" {
				d.rememberPickCode(f.FileID, f.PickCode)
			}
			items = append(items, fileToItem(f))
		}
		if int64(len(*files)) < listPageSize {
			break
		}
		offset += int64(len(*files))
	}
	return items, nil
}

func fileToItem(f *driver115.File) domain.FileItem {
	item := domain.FileItem{
		ID:      f.FileID,
		Name:    f.Name,
		Size:    f.Size,
		IsDir:   f.IsDirectory,
		IDKind:  domain.IDStable,
		ModTime: f.UpdateTime,
	}
	if sha1 := strings.TrimSpace(f.Sha1); sha1 != "" {
		item.Hash = map[domain.HashType]string{domain.HashSHA1: sha1}
	}
	if thumb := strings.TrimSpace(f.ThumbURL); thumb != "" {
		item.Thumb = thumb
	}
	return item
}

func (d *Driver) rootID() string {
	if id := strings.TrimSpace(d.add.RootFolderID); id != "" {
		return id
	}
	return "0"
}

func (d *Driver) normalizeParent(parentID string) string {
	p := strings.TrimSpace(parentID)
	if p == "" || p == "/" || p == "root" || p == "0" {
		return d.rootID()
	}
	return p
}

var (
	_ driver.Driver                   = (*Driver)(nil)
	_ driver.InfoGetter               = (*Driver)(nil)
	_ driver.Downloader               = (*Driver)(nil)
	_ driver.Deleter                  = (*Driver)(nil)
	_ driver.Mover                    = (*Driver)(nil)
	_ driver.Copier                   = (*Driver)(nil)
	_ driver.Renamer                  = (*Driver)(nil)
	_ driver.FolderCreator            = (*Driver)(nil)
	_ driver.AuthRefresher            = (*Driver)(nil)
	_ driver.AuthCredentialConsumer   = (*Driver)(nil)
	_ driver.AuthPersistConsumer      = (*Driver)(nil)
	_ driver.ConnectionErrorExplainer = (*Driver)(nil)
	_ driver.RequestIntervalConsumer  = (*Driver)(nil)
	_ driver.LocalUploader            = (*Driver)(nil)
	_ driver.OfflineDownloadProvider  = (*Driver)(nil)
	_ driver.OfflineURLDownloader     = (*Driver)(nil)
	_ driver.OfflineTaskRefresher     = (*Driver)(nil)
	_ driver.OfflineTaskDeleter       = (*Driver)(nil)
)
