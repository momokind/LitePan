package pan115

import (
	"strings"

	"litepan/pkg/jsonvalue"
)

type flexString = jsonvalue.FlexibleString

// Addition 115Cloud 账号配置；Cookie 落 account_auth_states，运行期注入。
type Addition struct {
	Cookie       string     `json:"cookie" label:"Cookie" type:"password" form:"required,full"`
	DownloadMode string     `json:"download_mode" label:"下载模式" type:"select" options:"redirect:302重定向,proxy:本机代理" default:"redirect" form:"pair=opts2"`
	DeleteMode   string     `json:"delete_mode" label:"删除模式" type:"select" options:"trash:移到回收站,delete:永久删除" default:"trash" form:"pair=opts2"`
	RootFolderID string     `json:"root_folder_id" label:"根目录ID（默认 0）" default:"0" form:"pair=opts1"`
	CacheTTL     flexString `json:"cache_ttl" label:"缓存时间(分钟)" type:"number" default:"30" form:"pair=opts1"`
	UserAgent    string     `json:"user_agent" label:"User-Agent（留空使用默认）" form:"full"`
}

func normalizeDeleteMode(value string) string {
	if strings.EqualFold(strings.TrimSpace(value), "delete") {
		return "delete"
	}
	return "trash"
}

func normalizeDownloadMode(value string) string {
	if strings.EqualFold(strings.TrimSpace(value), "proxy") {
		return "proxy"
	}
	return "redirect"
}

// resolveUserAgent 返回用户配置的 User-Agent，未配置时回落 115driver 默认浏览器 UA。
func (d *Driver) resolveUserAgent() string {
	if ua := strings.TrimSpace(d.add.UserAgent); ua != "" {
		return ua
	}
	return ua115Browser
}
