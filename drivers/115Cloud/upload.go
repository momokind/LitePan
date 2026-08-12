package pan115

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	driver115 "github.com/SheltonZhu/115driver/pkg/driver"

	"litepan/internal/domain"
	"litepan/internal/driver"
	"litepan/internal/driver/uploadutil"
)

const (
	uploadConfirmAttempts = 6
	uploadConfirmDelay    = 700 * time.Millisecond
	digestReportStep      = 4 * 1024 * 1024
)

func (d *Driver) UploadLocalFile(ctx context.Context, req driver.LocalUploadRequest) (*driver.LocalUploadResult, error) {
	targetName := strings.TrimSpace(filepath.Base(strings.TrimSpace(req.FileName)))
	if targetName == "" || targetName == "." {
		return nil, domain.Errorf(domain.CodeValidation, "上传文件名不能为空")
	}
	if err := uploadutil.ValidateFileName(targetName); err != nil {
		return nil, err
	}
	localFile, err := uploadutil.StatLocalFile(req.LocalPath)
	if err != nil {
		return nil, err
	}
	parentID := d.normalizeParent(req.ParentID)
	policy := uploadutil.NormalizeConflictPolicy(req.ConflictPolicy)
	fileSize := localFile.Size
	localPath := localFile.Path

	uploadutil.NotifyProgress(req.OnProgress, 0, fileSize, "正在检查目标目录")
	resolvedName, skipped, err := d.resolveUploadTargetName(ctx, parentID, targetName, policy)
	if err != nil {
		return nil, err
	}
	if skipped != nil {
		uploadutil.NotifyProgress(req.OnProgress, fileSize, fileSize, skipped.Message)
		return skipped, nil
	}

	uploadutil.NotifyProgress(req.OnProgress, 0, fileSize, "正在计算文件哈希")
	file, err := os.Open(localPath)
	if err != nil {
		return nil, domain.Wrap(domain.CodeDriverError, err)
	}
	defer file.Close()

	digest, err := d.pan.GetDigestResult(file)
	if err != nil {
		return nil, mapLibraryError(err)
	}
	if fileSize != digest.Size {
		return nil, domain.Errorf(domain.CodeValidation, "上传文件大小计算不一致")
	}

	uploadutil.NotifyProgress(req.OnProgress, 0, fileSize, "正在执行秒传校验")
	fastInfo, err := d.pan.RapidUpload(digest.Size, resolvedName, parentID, digest.PreID, digest.QuickID, file)
	if err != nil {
		return nil, mapLibraryError(err)
	}
	if ok, err := fastInfo.Ok(); err != nil {
		return nil, mapLibraryError(err)
	} else if ok {
		uploadutil.NotifyProgress(req.OnProgress, fileSize, fileSize, "秒传成功")
		item, err := d.confirmUploadedFile(ctx, "", parentID, resolvedName, fileSize)
		if err != nil {
			return nil, err
		}
		return uploadResultFromItem(*item, parentID, resolvedName, fileSize, "秒传成功"), nil
	}

	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return nil, domain.Wrap(domain.CodeDriverError, err)
	}
	uploadutil.NotifyProgress(req.OnProgress, 0, fileSize, "正在获取上传凭证")
	if fileSize <= singlePartUploadLimit {
		progress := newProgressReader(file, fileSize, digestReportStep, func(uploaded int64) {
			uploadutil.NotifyProgress(req.OnProgress, uploaded, fileSize, "正在上传到115网盘")
		})
		err := d.pan.UploadByOSS(&fastInfo.UploadOSSParams, progress, parentID)
		// 库内 checkUploadStatus 会在 OSS 上传成功后立即列表确认，115 索引异步尚未就绪时
		// 会误报 ErrUploadFailed；此时文件其实已上传成功，交给下方 confirmUploadedFile 重试确认。
		if err != nil && !errors.Is(err, driver115.ErrUploadFailed) {
			return nil, mapLibraryError(err)
		}
	} else {
		uploadutil.NotifyProgress(req.OnProgress, 0, fileSize, "正在分片上传到115网盘")
		if err := d.pan.UploadByMultipart(&fastInfo.UploadOSSParams, fileSize, file, parentID); err != nil {
			return nil, mapLibraryError(err)
		}
	}
	uploadutil.NotifyProgress(req.OnProgress, fileSize, fileSize, "上传成功")

	item, err := d.confirmUploadedFile(ctx, "", parentID, resolvedName, fileSize)
	if err != nil {
		return nil, err
	}
	return uploadResultFromItem(*item, parentID, resolvedName, fileSize, "上传成功"), nil
}

func (d *Driver) resolveUploadTargetName(ctx context.Context, parentID, fileName, policy string) (string, *driver.LocalUploadResult, error) {
	items, err := d.ListFiles(ctx, parentID)
	if err != nil {
		return "", nil, err
	}
	var existing *domain.FileItem
	nameLower := strings.ToLower(fileName)
	for i := range items {
		if strings.ToLower(items[i].Name) == nameLower {
			existing = &items[i]
			break
		}
	}
	if existing == nil {
		return fileName, nil, nil
	}
	if existing.IsDir {
		return "", nil, domain.Errorf(domain.CodeValidation, "目标目录已存在同名文件夹: %s", fileName)
	}
	switch strings.ToLower(strings.TrimSpace(policy)) {
	case "skip":
		return fileName, &driver.LocalUploadResult{
			FileID:   existing.ID,
			ParentID: parentID,
			FileName: existing.Name,
			Size:     existing.Size,
			Message:  fmt.Sprintf("文件 '%s' 已存在，已跳过", fileName),
			Skipped:  true,
		}, nil
	case "keep_both":
		names := map[string]struct{}{}
		for _, item := range items {
			names[item.Name] = struct{}{}
		}
		return uploadutil.KeepBothName(fileName, names), nil, nil
	case "overwrite":
		if err := d.DeleteFiles(ctx, []string{existing.ID}); err != nil {
			return "", nil, err
		}
		return fileName, nil, nil
	default:
		return "", nil, domain.Errorf(domain.CodeValidation, "不支持的冲突处理策略: %s", policy)
	}
}

func (d *Driver) confirmUploadedFile(ctx context.Context, fileID, parentID, fileName string, fileSize int64) (*domain.FileItem, error) {
	var lastErr error
	for attempt := 0; attempt < uploadConfirmAttempts; attempt++ {
		if fileID != "" {
			item, err := d.GetFileInfo(ctx, fileID)
			if err != nil {
				lastErr = err
			} else if item != nil && !item.IsDir && item.Size > 0 {
				return item, nil
			}
		}
		items, err := d.ListFiles(ctx, parentID)
		if err != nil {
			lastErr = err
		} else if item, ok := findUploadedItem(items, fileID, fileName, fileSize); ok {
			return &item, nil
		}
		if attempt+1 < uploadConfirmAttempts {
			timer := time.NewTimer(uploadConfirmDelay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
		}
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, domain.Errorf(domain.CodeDriverError, "115 上传完成后未在网盘中确认到文件")
}

func findUploadedItem(items []domain.FileItem, fileID, fileName string, fileSize int64) (domain.FileItem, bool) {
	if fileID != "" {
		for _, item := range items {
			if item.ID == fileID && !item.IsDir {
				return item, true
			}
		}
	}
	for _, item := range items {
		if item.IsDir || item.Name != fileName {
			continue
		}
		if fileSize > 0 && item.Size > 0 && item.Size != fileSize {
			continue
		}
		return item, true
	}
	return domain.FileItem{}, false
}

func uploadResultFromItem(item domain.FileItem, parentID, fallbackName string, fallbackSize int64, action string) *driver.LocalUploadResult {
	name := item.Name
	if name == "" {
		name = fallbackName
	}
	size := item.Size
	if size == 0 {
		size = fallbackSize
	}
	return &driver.LocalUploadResult{
		FileID:   item.ID,
		ParentID: parentID,
		FileName: name,
		Size:     size,
		Message:  fmt.Sprintf("文件 '%s' %s", name, action),
	}
}

// progressReader 包装 io.Reader 上报上传进度。
type progressReader struct {
	r          io.Reader
	total      int64
	sent       int64
	step       int64
	lastEmit   int64
	onProgress func(uploaded int64)
}

func newProgressReader(r io.Reader, total, step int64, onProgress func(int64)) *progressReader {
	return &progressReader{r: r, total: total, step: step, onProgress: onProgress}
}

func (p *progressReader) Read(b []byte) (int, error) {
	n, err := p.r.Read(b)
	if n <= 0 {
		return n, err
	}
	p.sent += int64(n)
	if p.onProgress != nil {
		step := p.step
		if step <= 0 {
			step = uploadutil.DefaultReadProgressStep
		}
		if p.sent-p.lastEmit >= step || err == io.EOF {
			p.lastEmit = p.sent
			p.onProgress(p.sent)
		}
	}
	return n, err
}
