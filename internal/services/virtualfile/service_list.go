package virtualfile

import (
	"github.com/xxcheng123/cloudpan189-share/internal/framework/context"
	"github.com/xxcheng123/cloudpan189-share/internal/repository/models"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type ListRequest struct {
	ParentId *int64 `form:"parentId" binding:"omitempty"`
	TopId    *int64 `form:"topId" binding:"omitempty"`
	LinkId   int64  `form:"-"`
	IsFolder *int8  `form:"-"`
	IsTop    *int8  `form:"-"`

	CurrentPage int    `form:"currentPage" binding:"omitempty,min=1"`
	PageSize    int    `form:"pageSize" binding:"omitempty,min=1"`
	Name        string `form:"name" binding:"omitempty"`

	// ExcludeIdList 排除ID
	ExcludeIdList []int64 `form:"-"`
	TopIdList     []int64 `form:"-"`

	AscList  []string `form:"-"`
	DescList []string `form:"-"`
}

func (r *ListRequest) WithIsTop(isTops ...bool) *ListRequest {
	var (
		zero int8 = 0
		one  int8 = 1
	)

	if len(isTops) > 0 && isTops[0] {
		r.IsTop = &one
	} else {
		r.IsTop = &zero
	}

	return r
}

func (s *service) List(ctx context.Context, req *ListRequest) ([]*models.VirtualFile, error) {
	query := s.getListQuery(ctx, req)

	for _, k := range req.AscList {
		query = query.Order(clause.OrderByColumn{Column: clause.Column{Name: k}, Desc: false})
	}

	for _, k := range req.DescList {
		query = query.Order(clause.OrderByColumn{Column: clause.Column{Name: k}})
	}

	if req.CurrentPage > 0 && req.PageSize > 0 {
		query = query.Offset((req.CurrentPage - 1) * req.PageSize).Limit(req.PageSize)
	}

	list := make([]*models.VirtualFile, 0)

	return list, query.Find(&list).Error
}

func (s *service) Count(ctx context.Context, req *ListRequest) (count int64, err error) {
	if err = s.getListQuery(ctx, req).Count(&count).Error; err != nil {
		ctx.Error("查询文件数量失败", zap.Error(err))
	}

	return count, err
}

func (s *service) getListQuery(ctx context.Context, req *ListRequest) *gorm.DB {
	query := s.getDB(ctx)

	if req.ParentId != nil {
		query = query.Where("parent_id = ?", *req.ParentId)
	}

	if req.LinkId != 0 {
		query = query.Where("link_id = ?", req.LinkId)
	}

	if req.IsFolder != nil {
		query = query.Where("is_dir = ?", *req.IsFolder)
	}

	if req.IsTop != nil {
		query = query.Where("is_top = ?", *req.IsTop)
	}

	if req.Name != "" {
		query = query.Where("name like ?", "%"+req.Name+"%")
	}

	if req.TopId != nil {
		query = query.Where("top_id = ?", *req.TopId)
	}

	if len(req.ExcludeIdList) > 0 {
		query = query.Where("id not in (?)", req.ExcludeIdList)
	}

	if len(req.TopIdList) > 0 {
		query = query.Where("top_id in (?)", req.TopIdList)
	}

	return query
}
