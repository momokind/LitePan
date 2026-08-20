package pan115

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"litepan/internal/domain"
	"litepan/internal/httpx"
)

// behaviorDetailURL 是 115「生活操作事件」明细接口（web 端，风控较轻）。
// 测试可通过覆盖该变量指向本地 mock。
var behaviorDetailURL = "https://webapi.115.com/behavior/detail"

// lifeSetOptionURL 是 115 生活事件开关接口；用于账号初始化时确保事件收集开启。
var lifeSetOptionURL = "https://life.115.com/api/1.0/web/1.0/calendar/setoption"

// behaviorDetailResp 对应接口返回结构（data.list 按事件发生时间逆序）。
type behaviorDetailResp struct {
	State   bool   `json:"state"`
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		Count    int                   `json:"count"`
		NextPage bool                  `json:"next_page"`
		List     []behaviorDetailEvent `json:"list"`
	} `json:"data"`
}

// behaviorDetailEvent 对应单条操作事件。
type behaviorDetailEvent struct {
	ID         string `json:"id"`
	Type       int    `json:"type"`
	FileID     string `json:"file_id"`
	ParentID   string `json:"parent_id"`
	FileName   string `json:"file_name"`
	FileSize   int64  `json:"file_size"`
	SHA1       string `json:"sha1"`
	UpdateTime int64  `json:"update_time"`
}

// mapBehaviorType 把 115 操作类型码映射为领域事件类型；返回 false 表示忽略（浏览/星标/标签等）。
func mapBehaviorType(t int) (string, bool) {
	switch t {
	case 1, 2: // upload_image_file / upload_file
		return domain.OperationEventUpload, true
	case 14, 17: // receive_files / new_folder
		return domain.OperationEventCreate, true
	case 18, 23: // copy_folder / copy_file
		return domain.OperationEventCopy, true
	case 22: // delete_file
		return domain.OperationEventDelete, true
	case 5, 6: // move_image_file / move_file
		return domain.OperationEventMove, true
	case 20, 24: // folder_rename / file_rename
		return domain.OperationEventRename, true
	default:
		return "", false
	}
}

// isFolderBehavior 报告事件对象是否为目录（仅事件类型能确定的目录类事件）。
func isFolderBehavior(t int) bool {
	return t == 17 || t == 18
}

// eventIDValue 把事件 id 字符串解析为数值，用于游标比较；非法返回 0。
func eventIDValue(id string) int64 {
	v, _ := strconv.ParseInt(strings.TrimSpace(id), 10, 64)
	return v
}

// fetchBehaviorPage 拉取一页操作事件（带账号级间隔门）。
func (d *Driver) fetchBehaviorPage(ctx context.Context, offset, limit int) (*behaviorDetailResp, error) {
	if err := d.beforeCall(ctx); err != nil {
		return nil, err
	}
	u := behaviorDetailURL + "?limit=" + strconv.Itoa(limit) + "&offset=" + strconv.Itoa(offset)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, domain.Errorf(domain.CodeInternal, "构造 115 事件请求失败：%v", err)
	}
	if cookie := d.resolveCookie(); cookie != "" {
		req.Header.Set("Cookie", cookie)
	}
	req.Header.Set("User-Agent", d.resolveUserAgent())
	req.Header.Set("Accept", "application/json, text/plain, */*")

	client := d.client
	if client == nil {
		return nil, domain.Errorf(domain.CodeDriverError, "115 客户端未初始化")
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, mapLibraryError(err)
	}
	defer resp.Body.Close()
	body, err := httpx.ReadLimited(resp.Body, 8<<20)
	if err != nil {
		return nil, domain.Errorf(domain.CodeDriverError, "读取 115 事件响应失败：%v", err)
	}
	if resp.StatusCode != http.StatusOK {
		// 401/403 视为认证失效（交由认证调度处理）；405 等为临时风控，保持驱动错误让轮询器跳过。
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			return nil, domain.Errorf(domain.CodeAuthExpired, "115 事件接口拒绝访问（HTTP %d）", resp.StatusCode)
		}
		return nil, domain.Errorf(domain.CodeDriverError, "115 事件接口 HTTP %d：%s", resp.StatusCode, httpx.Truncate(body, 200))
	}
	var out behaviorDetailResp
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, domain.Errorf(domain.CodeDriverError, "115 事件响应解析失败：%v", err)
	}
	if !out.State {
		return nil, domain.Errorf(domain.CodeDriverError, "115 事件接口返回失败：%s", httpx.Truncate([]byte(out.Message), 200))
	}
	return &out, nil
}

// RecentOperations 拉取自 fromID 之后的最近操作事件。
//
// fromID 为空时表示基线初始化：只建立游标（最新事件 id），不返回事件，避免回放历史事件。
// 返回事件切片（按发生时间逆序）与新的游标；游标仅在发现更新事件时前移。
func (d *Driver) RecentOperations(ctx context.Context, fromID string, limit int) ([]domain.OperationEvent, string, error) {
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	fromVal := eventIDValue(fromID)
	baseline := fromVal == 0

	var events []domain.OperationEvent
	seen := make(map[string]struct{})
	var maxID int64

	offset := 0
	for {
		page, err := d.fetchBehaviorPage(ctx, offset, limit)
		if err != nil {
			return nil, fromID, err
		}
		items := page.Data.List
		if len(items) == 0 {
			break
		}
		stop := false
		for _, it := range items {
			idv := eventIDValue(it.ID)
			if idv > maxID {
				maxID = idv
			}
			if baseline {
				stop = true
				break
			}
			if fromVal > 0 && idv <= fromVal {
				stop = true
				break
			}
			typ, ok := mapBehaviorType(it.Type)
			if !ok {
				continue
			}
			fid := strings.TrimSpace(it.FileID)
			if fid == "" {
				continue
			}
			if _, dup := seen[fid]; dup {
				continue
			}
			seen[fid] = struct{}{}
			events = append(events, domain.OperationEvent{
				ID:       it.ID,
				Type:     typ,
				FileID:   fid,
				ParentID: strings.TrimSpace(it.ParentID),
				FileName: it.FileName,
				FileSize: it.FileSize,
				IsDir:    isFolderBehavior(it.Type),
				Time:     time.Unix(it.UpdateTime, 0),
			})
		}
		if baseline || stop || !page.Data.NextPage {
			break
		}
		offset += len(items)
		if offset >= 10000 {
			break
		}
	}

	next := fromID
	if maxID > fromVal {
		next = strconv.FormatInt(maxID, 10)
	}
	return events, next, nil
}

// enableLifeEvents 确保 115「生活事件」收集开启（open_life=1）。
//
// 115 的操作事件接口只有在生活事件开关开启后才会返回新事件；关闭时事件流停滞
// （如历史事件停在多年前），导致事件监控无法感知远端变更。此调用为 best-effort，
// 失败不阻塞主流程，事件监控无事件时自然降级为定时扫描。
func (d *Driver) enableLifeEvents(ctx context.Context) error {
	if err := d.beforeCall(ctx); err != nil {
		return err
	}
	form := url.Values{}
	form.Set("locus", "1")
	form.Set("open_life", "1")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, lifeSetOptionURL, strings.NewReader(form.Encode()))
	if err != nil {
		return domain.Errorf(domain.CodeInternal, "构造 115 生活开关请求失败：%v", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if cookie := d.resolveCookie(); cookie != "" {
		req.Header.Set("Cookie", cookie)
	}
	req.Header.Set("User-Agent", d.resolveUserAgent())
	req.Header.Set("Accept", "application/json")

	client := d.client
	if client == nil {
		return domain.Errorf(domain.CodeDriverError, "115 客户端未初始化")
	}
	resp, err := client.Do(req)
	if err != nil {
		return mapLibraryError(err)
	}
	defer resp.Body.Close()
	body, err := httpx.ReadLimited(resp.Body, 8<<20)
	if err != nil {
		return domain.Errorf(domain.CodeDriverError, "读取 115 生活开关响应失败：%v", err)
	}
	if resp.StatusCode != http.StatusOK {
		return domain.Errorf(domain.CodeDriverError, "115 生活开关 HTTP %d：%s", resp.StatusCode, httpx.Truncate(body, 200))
	}
	var out struct {
		State bool `json:"state"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return domain.Errorf(domain.CodeDriverError, "115 生活开关响应解析失败：%v", err)
	}
	if !out.State {
		return domain.Errorf(domain.CodeDriverError, "115 生活开关设置失败")
	}
	return nil
}
