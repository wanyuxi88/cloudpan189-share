package media

import (
	"path"
	"slices"
	"strings"

	"github.com/xxcheng123/cloudpan189-share/internal/framework/context"
	"github.com/xxcheng123/cloudpan189-share/internal/framework/taskcontext"
	"github.com/xxcheng123/cloudpan189-share/internal/pkgs/ptr"
	"github.com/xxcheng123/cloudpan189-share/internal/services/mountpoint"
	"github.com/xxcheng123/cloudpan189-share/internal/services/virtualfile"
	"github.com/xxcheng123/cloudpan189-share/internal/shared"
	"github.com/xxcheng123/cloudpan189-share/internal/types/media"
	"go.uber.org/zap"

	verifySvi "github.com/xxcheng123/cloudpan189-share/internal/services/verify"
)

const maxRecursionDepth = 100 // 最大递归深度，防止栈溢出

func (h *handler) RebuildStrmFile() taskcontext.HandlerFunc {
	return func(ctx *taskcontext.Context) error {
		var (
			logger = ctx.GetContext().Logger
		)

		// 扫描文件
		// 判断是否符合 strm 文件格式
		mountpoints, err := h.mountpointService.List(ctx.GetContext(), &mountpoint.ListRequest{
			NoPaginate: true,
		})
		if err != nil {
			logger.Error("查询挂载点失败", zap.Error(err))

			return err
		}

		logger.Debug("查询到挂载点", zap.Int("count", len(mountpoints)))

		car := media.NewWriterCar(shared.MediaConfig.StoragePath, shared.MediaConfig.ConflictPolicy, shared.BaseURL)

		for _, mountpoint := range mountpoints {
			logger.Debug("处理挂载点", zap.String("mountpoint", mountpoint.FullPath))
			h.walkBuildStrm(ctx.GetContext(), mountpoint.FileId, car.NewSubCar(mountpoint.FullPath), 0)
		}

		return nil
	}
}

func (h *handler) walkBuildStrm(ctx context.Context, fid int64, car media.WriterCar, depth int) {
	// 检查递归深度，防止栈溢出
	if depth >= maxRecursionDepth {
		ctx.Warn("达到最大递归深度，停止处理", zap.Int("depth", depth), zap.Int64("parent_id", fid))

		return
	}

	files, err := h.virtualfileService.List(ctx, &virtualfile.ListRequest{
		ParentId: ptr.Of(fid),
	})
	if err != nil {
		ctx.Error("查询子文件失败", zap.Error(err), zap.Int64("parent_id", fid))

		return
	}

	for _, file := range files {
		if file.IsDir {
			h.walkBuildStrm(ctx, file.ID, car.NewSubCar(file.Name), depth+1)

			continue
		}

		// 获取文件后缀
		extName := path.Ext(file.Name)
		if len(shared.MediaConfig.IncludedSuffixes) > 0 && !slices.Contains(shared.MediaConfig.IncludedSuffixes, extName) {
			ctx.Debug("批量创建文件 - 跳过文件", zap.String("file_name", file.Name))

			continue
		}

		// 重新生成文件名: video.mp4 -> video.strm
		filename := strings.TrimSuffix(file.Name, extName) + ".strm"

		// 生成URL
		values, err := h.verifyService.SignV1(ctx, file.ID, verifySvi.WithV1NoExpire())
		if err != nil {
			ctx.Error("批量创建文件 - 遍历 - 获取文件签名失败", zap.Int64("file_id", file.ID), zap.Error(err))

			continue
		}

		if id, err := h.mediaFileService.WriteStrm(ctx, car.NewSubCar(filename), file.ID, shared.JoinDownloadURL(file.ID, values)); err != nil {
			ctx.Error("批量创建文件 - 遍历 - 创建 strm 文件失败", zap.Int64("file_id", file.ID), zap.Error(err))

			continue
		} else {
			ctx.Debug("批量创建文件 - 遍历 - 创建 strm 文件成功", zap.Int64("file_id", file.ID), zap.Int64("media_file_id", id))
		}
	}
}
