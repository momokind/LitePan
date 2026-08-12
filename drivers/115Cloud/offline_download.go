package pan115

import (
	"context"
	"strings"

	driver115 "github.com/SheltonZhu/115driver/pkg/driver"

	"litepan/internal/domain"
	"litepan/internal/driver"
)

func (d *Driver) OfflineDownloadCapabilities() driver.OfflineDownloadCapabilities {
	return driver.OfflineDownloadCapabilities{
		SupportsURLs:      true,
		SupportsBatchURLs: true,
		SupportsTorrent:   false,
		URLSchemes:        []string{"http", "https", "magnet", "ed2k"},
		RootTargetAllowed: true,
		RemoteDelete:      true,
	}
}

func (d *Driver) AddOfflineURLs(ctx context.Context, req driver.OfflineURLRequest) ([]driver.OfflineAddResult, error) {
	uris := make([]string, 0, len(req.URLs))
	for _, raw := range req.URLs {
		if uri := strings.TrimSpace(raw); uri != "" {
			uris = append(uris, uri)
		}
	}
	if len(uris) == 0 {
		return nil, domain.Errorf(domain.CodeValidation, "离线下载链接不能为空")
	}
	if err := d.beforeCall(ctx); err != nil {
		return nil, err
	}
	hashes, err := d.pan.AddOfflineTaskURIs(uris, d.normalizeParent(req.ParentID))
	if err != nil {
		return nil, mapLibraryError(err)
	}
	results := make([]driver.OfflineAddResult, 0, len(uris))
	for i, uri := range uris {
		name := ""
		if i == 0 {
			name = strings.TrimSpace(req.FileName)
		}
		hash := ""
		if i < len(hashes) {
			hash = strings.TrimSpace(hashes[i])
		}
		result := driver.OfflineAddResult{
			Source:         uri,
			InfoHash:       hash,
			ProviderTaskID: hash,
			Name:           name,
		}
		if hash != "" {
			result.Success = true
			result.Message = "已提交到 115 离线下载"
		} else {
			result.Success = false
			result.Message = "115 未能创建该离线任务"
		}
		results = append(results, result)
	}
	return results, nil
}

func (d *Driver) RefreshOfflineTasks(ctx context.Context, refs []driver.OfflineTaskRef) ([]driver.OfflineTaskUpdate, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	want := make(map[string]struct{}, len(refs))
	for _, ref := range refs {
		if id := strings.TrimSpace(ref.ProviderTaskID); id != "" {
			want[id] = struct{}{}
		}
	}
	if len(want) == 0 {
		return nil, nil
	}

	found := make(map[string]driver115.OfflineTask)
	for page := int64(1); len(found) < len(want); page++ {
		if err := d.beforeCall(ctx); err != nil {
			return nil, err
		}
		resp, err := d.pan.ListOfflineTask(page)
		if err != nil {
			return nil, mapLibraryError(err)
		}
		for _, task := range resp.Tasks {
			if task == nil {
				continue
			}
			hash := strings.TrimSpace(task.InfoHash)
			if _, ok := want[hash]; ok {
				found[hash] = *task
			}
		}
		if page >= resp.PageCount || len(resp.Tasks) == 0 {
			break
		}
	}

	updates := make([]driver.OfflineTaskUpdate, 0, len(refs))
	for _, ref := range refs {
		hash := strings.TrimSpace(ref.ProviderTaskID)
		update := driver.OfflineTaskUpdate{
			ProviderTaskID: hash,
			InfoHash:       hash,
		}
		task, ok := found[hash]
		if !ok {
			update.Status = driver.OfflineStatusPending
			update.Message = "等待 115 创建离线任务"
			updates = append(updates, update)
			continue
		}
		update.Name = task.Name
		update.Size = task.Size
		update.FileID = task.FileId
		update.Progress = int(task.Percent*100 + 0.5)
		switch {
		case task.IsDone():
			update.Status = driver.OfflineStatusSuccess
			update.Progress = 100
			update.Message = "离线下载完成"
		case task.IsFailed():
			update.Status = driver.OfflineStatusFailed
			update.Progress = 0
			update.Message = "离线下载失败"
			update.Error = "115 离线下载失败"
		case task.IsRunning():
			update.Status = driver.OfflineStatusRunning
			update.Message = "正在由 115 离线下载"
		case task.IsTodo():
			update.Status = driver.OfflineStatusPending
			update.Message = "准备开始离线下载"
		default:
			update.Status = driver.OfflineStatusPending
			update.Message = task.GetStatus()
		}
		updates = append(updates, update)
	}
	return updates, nil
}

func (d *Driver) DeleteOfflineTask(ctx context.Context, ref driver.OfflineTaskRef, deleteSourceFile bool) error {
	hash := strings.TrimSpace(ref.ProviderTaskID)
	if hash == "" {
		return domain.Errorf(domain.CodeValidation, "离线任务 ID 不能为空")
	}
	if err := d.beforeCall(ctx); err != nil {
		return err
	}
	if err := d.pan.DeleteOfflineTasks([]string{hash}, deleteSourceFile); err != nil {
		return mapLibraryError(err)
	}
	return nil
}
