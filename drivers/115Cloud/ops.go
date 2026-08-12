package pan115

import (
	"context"
	"net/http"
	"sort"
	"strings"
	"time"

	driver115 "github.com/SheltonZhu/115driver/pkg/driver"

	"litepan/internal/domain"
	"litepan/internal/driver"
)

func normalizeIDs(fileIDs []string) []string {
	out := make([]string, 0, len(fileIDs))
	for _, id := range fileIDs {
		if id = strings.TrimSpace(id); id != "" {
			out = append(out, id)
		}
	}
	return out
}

func (d *Driver) GetFileInfo(ctx context.Context, fileID string) (*domain.FileItem, error) {
	id := strings.TrimSpace(fileID)
	root := d.rootID()
	if id == "" || id == "0" || id == root {
		return &domain.FileItem{
			ID:     root,
			Name:   "根目录",
			IsDir:  true,
			IDKind: domain.IDStable,
		}, nil
	}
	if err := d.beforeCall(ctx); err != nil {
		return nil, err
	}
	file, err := d.pan.GetFile(id)
	if err != nil {
		return nil, mapLibraryError(err)
	}
	if file == nil || strings.TrimSpace(file.FileID) == "" {
		return nil, domain.Errf(domain.CodeNotFound)
	}
	d.rememberPickCode(file.FileID, file.PickCode)
	item := fileToItem(file)
	return &item, nil
}

func (d *Driver) ResolveDownload(ctx context.Context, req driver.DownloadRequest) (*domain.DownloadInfo, error) {
	fileID := strings.TrimSpace(req.FileID)
	if fileID == "" {
		return nil, domain.Errorf(domain.CodeValidation, "file_id 不能为空")
	}
	pickCode := d.cachedPickCode(fileID)
	var entry *driver115.File
	if pickCode == "" {
		if err := d.beforeCall(ctx); err != nil {
			return nil, err
		}
		file, err := d.pan.GetFile(fileID)
		if err != nil {
			return nil, mapLibraryError(err)
		}
		if file != nil {
			entry = file
			d.rememberPickCode(file.FileID, file.PickCode)
			pickCode = file.PickCode
		}
	}
	if pickCode == "" {
		name := fileID
		if entry != nil {
			name = entry.Name
		}
		return nil, domain.Errorf(domain.CodeDriverError, "文件 %s 缺少 pick_code，无法获取下载链接", name)
	}

	ua := strings.TrimSpace(req.UA)
	if ua == "" {
		ua = d.resolveUserAgent()
	}
	if err := d.beforeCall(ctx); err != nil {
		return nil, err
	}
	info, err := d.pan.DownloadWithUA(pickCode, ua)
	if err != nil {
		return nil, mapLibraryError(err)
	}
	if info == nil || strings.TrimSpace(info.Url.Url) == "" {
		return nil, domain.Errorf(domain.CodeDriverError, "115 未返回有效下载链接")
	}

	size := int64(info.FileSize)
	if size <= 0 && entry != nil {
		size = entry.Size
	}
	name := strings.TrimSpace(info.FileName)
	if name == "" && entry != nil {
		name = entry.Name
	}

	headers := http.Header{}
	if info.Header != nil {
		for key, values := range info.Header {
			for _, value := range values {
				headers.Add(key, value)
			}
		}
	}
	if headers.Get("Cookie") == "" {
		headers.Set("Cookie", d.resolveCookie())
	}
	if headers.Get("Referer") == "" {
		headers.Set("Referer", "https://115.com/")
	}

	result := &domain.DownloadInfo{
		URL:         strings.TrimSpace(info.Url.Url),
		Headers:     headers,
		Mode:        domain.DownloadRedirect,
		Size:        size,
		FileName:    name,
		ChunkSize:   downloadPartSize,
		Concurrency: downloadConcurrency,
	}
	if normalizeDownloadMode(d.add.DownloadMode) == "proxy" {
		result.Mode = domain.DownloadProxy
		result.ForceProxy = true
		result.Expiration = downloadLinkTTL
	}
	return result, nil
}

func (d *Driver) DeleteFiles(ctx context.Context, fileIDs []string) error {
	ids := normalizeIDs(fileIDs)
	if len(ids) == 0 {
		return nil
	}
	if normalizeDeleteMode(d.add.DeleteMode) == "delete" {
		return d.permanentDelete(ctx, ids)
	}
	if err := d.beforeCall(ctx); err != nil {
		return err
	}
	if err := d.pan.Delete(ids...); err != nil {
		return mapLibraryError(err)
	}
	return nil
}

// permanentDelete 先移入回收站，再从回收站清空匹配记录。
func (d *Driver) permanentDelete(ctx context.Context, ids []string) error {
	if err := d.beforeCall(ctx); err != nil {
		return err
	}
	if err := d.pan.Delete(ids...); err != nil {
		return mapLibraryError(err)
	}
	want := len(ids)
	time.Sleep(500 * time.Millisecond)
	for attempt := 0; attempt < 8; attempt++ {
		rids := d.collectRecentRecycleIDs(ctx, want)
		if len(rids) >= want {
			if err := d.beforeCall(ctx); err != nil {
				return err
			}
			if err := d.pan.CleanRecycleBin("", rids[:want]...); err != nil {
				return mapLibraryError(err)
			}
			return nil
		}
		if attempt < 7 {
			delay := 300 * time.Millisecond
			if attempt >= 4 {
				delay = 800 * time.Millisecond
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}
	}
	return domain.Errorf(domain.CodeDriverError, "115 永久删除未完成：回收站记录尚未同步，请稍后在 115 回收站手动清空")
}

func (d *Driver) collectRecentRecycleIDs(ctx context.Context, want int) []string {
	if err := d.beforeCall(ctx); err != nil {
		return nil
	}
	items, err := d.pan.ListRecycleBin(0, want+5)
	if err != nil {
		return nil
	}
	sort.Slice(items, func(i, j int) bool {
		return int64(items[i].DeleteTime) > int64(items[j].DeleteTime)
	})
	out := make([]string, 0, len(items))
	for _, item := range items {
		if id := strings.TrimSpace(item.FileId); id != "" {
			out = append(out, id)
		}
	}
	return out
}

func (d *Driver) MoveFiles(ctx context.Context, fileIDs []string, targetParentID, _ string) error {
	ids := normalizeIDs(fileIDs)
	if len(ids) == 0 {
		return nil
	}
	if err := d.beforeCall(ctx); err != nil {
		return err
	}
	if err := d.pan.Move(d.normalizeParent(targetParentID), ids...); err != nil {
		return mapLibraryError(err)
	}
	return nil
}

func (d *Driver) CopyFiles(ctx context.Context, fileIDs []string, targetParentID string) error {
	ids := normalizeIDs(fileIDs)
	if len(ids) == 0 {
		return nil
	}
	if err := d.beforeCall(ctx); err != nil {
		return err
	}
	if err := d.pan.Copy(d.normalizeParent(targetParentID), ids...); err != nil {
		return mapLibraryError(err)
	}
	return nil
}

func (d *Driver) RenameFile(ctx context.Context, fileID, newName string) error {
	id := strings.TrimSpace(fileID)
	name := strings.TrimSpace(newName)
	if id == "" {
		return domain.Errorf(domain.CodeValidation, "file_id 不能为空")
	}
	if name == "" {
		return domain.Errorf(domain.CodeValidation, "新名称不能为空")
	}
	if err := d.beforeCall(ctx); err != nil {
		return err
	}
	if err := d.pan.Rename(id, name); err != nil {
		return mapLibraryError(err)
	}
	return nil
}

func (d *Driver) CreateFolder(ctx context.Context, parentID, name string) (*domain.FileItem, error) {
	folderName := strings.TrimSpace(name)
	if folderName == "" {
		return nil, domain.Errorf(domain.CodeValidation, "文件夹名称不能为空")
	}
	if err := d.beforeCall(ctx); err != nil {
		return nil, err
	}
	folderID, err := d.pan.Mkdir(d.normalizeParent(parentID), folderName)
	if err != nil {
		return nil, mapLibraryError(err)
	}
	return &domain.FileItem{
		ID:     strings.TrimSpace(folderID),
		Name:   folderName,
		IsDir:  true,
		IDKind: domain.IDStable,
	}, nil
}
