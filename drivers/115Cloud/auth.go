package pan115

import (
	"context"
	"crypto/tls"
	"net/http"
	"strings"
	"time"

	driver115 "github.com/SheltonZhu/115driver/pkg/driver"

	"litepan/internal/domain"
	"litepan/internal/driver"
	"litepan/internal/httpx"
)

func (d *Driver) SetAuthCredentials(creds domain.AuthCredentials) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if c := strings.TrimSpace(creds.Cookie); c != "" {
		d.cookie = c
	}
}

func (d *Driver) SetAuthPersister(fn driver.AuthPersistFunc) { d.persist = fn }

func (d *Driver) SetRequestIntervalGate(gate driver.RequestIntervalGate) { d.intervalGate = gate }

func (d *Driver) currentCookie() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.cookie
}

// resolveCookie 优先返回运行期注入的 Cookie，其次回退到 Addition 表单中的 Cookie。
func (d *Driver) resolveCookie() string {
	if c := strings.TrimSpace(d.currentCookie()); c != "" {
		return c
	}
	return strings.TrimSpace(d.add.Cookie)
}

// RefreshAuth 健康检查；Cookie 认证无 refresh_token，仅验 Cookie 有效性。
func (d *Driver) RefreshAuth(ctx context.Context, _ driver.RefreshCaller) (driver.RefreshOutcome, error) {
	if err := d.Ping(ctx); err != nil {
		if ae, ok := domain.AsAppError(err); ok && ae.Code == domain.CodeAuthExpired {
			return driver.RefreshFatal, err
		}
		return driver.RefreshRetryable, err
	}
	return driver.RefreshSuccess, nil
}

// buildClient 用当前 Cookie 构造 115driver 客户端。
func (d *Driver) buildClient(ctx context.Context) error {
	cookie := d.resolveCookie()
	if cookie == "" {
		return domain.Errorf(domain.CodeValidation, "Cookie 不能为空")
	}
	cr := &driver115.Credential{}
	if err := cr.FromCookie(cookie); err != nil {
		return domain.Errorf(domain.CodeValidation, "Cookie 格式错误：%v", err)
	}
	if d.client == nil {
		d.client = httpx.NewClient(httpx.ClientOptions{Timeout: 60 * time.Second})
	}
	pan := driver115.New(
		// 注意顺序：WithClient 会重建 resty 客户端并清空已设的 UA 头，
		// 因此必须先用 WithClient 注入 http.Client，再 UA() 设置 User-Agent。
		driver115.WithClient(d.client),
		driver115.UA(d.resolveUserAgent()),
		func(c *driver115.Pan115Client) {
			c.Client.SetTLSClientConfig(&tls.Config{InsecureSkipVerify: true})
		},
	)
	pan.ImportCredential(cr)
	d.pan = pan
	return nil
}

// ensureUserInfo 预取 UserID/Userkey，供上传与离线任务签名使用。
func (d *Driver) ensureUserInfo(ctx context.Context) error {
	if err := d.beforeCall(ctx); err != nil {
		return err
	}
	if err := d.pan.LoginCheck(); err != nil {
		return mapLibraryError(err)
	}
	if d.pan.UserID <= 0 {
		user, err := d.pan.GetUser()
		if err != nil {
			return mapLibraryError(err)
		}
		d.pan.UserID = user.UserID
	}
	d.mu.Lock()
	d.userID = d.pan.UserID
	d.mu.Unlock()
	return nil
}

// absorbSetCookie 吸收 115 下发的增量 Cookie 并回写 account_auth_states。
func (d *Driver) absorbSetCookie(ctx context.Context, header http.Header) {
	raw := header.Values("Set-Cookie")
	if len(raw) == 0 {
		return
	}
	parsed := (&http.Response{Header: http.Header{"Set-Cookie": raw}}).Cookies()
	keep := map[string]struct{}{
		driver115.CookieNameUid:  {},
		driver115.CookieNameCid:  {},
		driver115.CookieNameSeid: {},
		driver115.CookieNameKid:  {},
	}
	incoming := map[string]string{}
	for _, c := range parsed {
		if _, ok := keep[c.Name]; ok && c.Value != "" {
			incoming[c.Name] = c.Value
		}
	}
	if len(incoming) == 0 {
		return
	}

	d.mu.Lock()
	keys, vals := parseCookie(d.cookie)
	changed := false
	for name, val := range incoming {
		if vals[name] != val {
			if _, ok := vals[name]; !ok {
				keys = append(keys, name)
			}
			vals[name] = val
			changed = true
		}
	}
	if !changed {
		d.mu.Unlock()
		return
	}
	newCookie := buildCookie(keys, vals)
	d.cookie = newCookie
	persist := d.persist
	d.mu.Unlock()

	if persist != nil {
		// 回写失败不影响本次请求：新 Cookie 已在内存生效。
		_ = persist(ctx, domain.AuthCredentials{Cookie: newCookie})
	}
}

func parseCookie(s string) ([]string, map[string]string) {
	keys := []string{}
	vals := map[string]string{}
	for _, part := range strings.Split(s, ";") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		i := strings.Index(part, "=")
		if i < 0 {
			continue
		}
		k := strings.TrimSpace(part[:i])
		if k == "" {
			continue
		}
		if _, ok := vals[k]; !ok {
			keys = append(keys, k)
		}
		vals[k] = strings.TrimSpace(part[i+1:])
	}
	return keys, vals
}

func buildCookie(keys []string, vals map[string]string) string {
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+vals[k])
	}
	return strings.Join(parts, "; ")
}

func (d *Driver) rememberPickCode(fileID, pickCode string) {
	id := strings.TrimSpace(fileID)
	pc := strings.TrimSpace(pickCode)
	if id == "" || pc == "" {
		return
	}
	d.pickMu.Lock()
	if d.pickBy == nil {
		d.pickBy = make(map[string]string)
	}
	d.pickBy[id] = pc
	d.pickMu.Unlock()
}

func (d *Driver) cachedPickCode(fileID string) string {
	id := strings.TrimSpace(fileID)
	if id == "" {
		return ""
	}
	d.pickMu.RLock()
	pc := d.pickBy[id]
	d.pickMu.RUnlock()
	return pc
}
