package fs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"candyfs/pkg/meta"
)

// VFSConfig 虚拟文件系统配置
type VFSConfig struct {
	MetaURI        string        // 元数据服务器URI
	MetaConf       *meta.Config  // 元数据配置
	CacheSize      int           // 缓存大小
	CacheTTL       time.Duration // 缓存过期时间
	InodeTTL       time.Duration // inode缓存过期时间
	HandleTimeout  time.Duration // 文件句柄超时
}

// DirEntry 目录条目
type DirEntry struct {
	Name  string // 名称
	Inode uint64 // inode号
	IsDir bool   // 是否为目录
}

// FileAttr 文件属性
type FileAttr struct {
	Inode  uint64      // inode号
	Size   int64       // 文件大小
	Mode   os.FileMode // 文件模式
	Mtime  time.Time   // 修改时间
	Atime  time.Time   // 访问时间
	Ctime  time.Time   // 创建时间
	IsDir  bool        // 是否为目录
	Blocks int64       // 占用块数
}

// FileHandle 文件句柄
type FileHandle struct {
	inode uint64 // inode号
	fh    uint64 // 文件句柄ID
	isDir bool   // 是否为目录
}

// VFS 虚拟文件系统
type VFS struct {
	meta       meta.Meta  // 元数据服务
	config     *VFSConfig // 配置
	nextHandle uint64     // 下一个文件句柄ID
	inodeCache sync.Map   // inode缓存
	handles    sync.Map   // 打开的文件句柄
	root       uint64     // 根目录inode
}

// NewVFS 创建新的虚拟文件系统
func NewVFS(metaService meta.Meta, config *VFSConfig) (*VFS, error) {
	if config == nil {
		config = &VFSConfig{
			CacheSize:     1000,
			CacheTTL:      time.Minute * 5,
			InodeTTL:      time.Hour,
			HandleTimeout: time.Hour,
		}
	}

	if config.CacheSize <= 0 {
		config.CacheSize = 1000
	}

	if config.CacheTTL <= 0 {
		config.CacheTTL = time.Minute * 5
	}

	if config.InodeTTL <= 0 {
		config.InodeTTL = time.Hour
	}

	if config.HandleTimeout <= 0 {
		config.HandleTimeout = time.Hour
	}

	vfs := &VFS{
		meta:       metaService,
		config:     config,
		nextHandle: 1,
	}

	// 初始化根目录 - 注意我们假设inode 1是根目录
	rootAttr, err := vfs.meta.GetAttr(context.Background(), 1)
	if err != nil {
		// 如果根目录不存在，创建它
		if errors.Is(err, meta.ErrNoSuchFileOrDir) {
			entry, err := vfs.meta.MkDir(context.Background(), 0, "/", 0755|uint32(os.ModeDir))
			if err != nil {
				return nil, err
			}
			vfs.root = entry.Inode
		} else {
			return nil, err
		}
	} else {
		vfs.root = rootAttr.Inode
	}

	return vfs, nil
}

// Lookup 查找文件或目录
func (vfs *VFS) Lookup(ctx context.Context, path string) (*DirEntry, error) {
	// 规范化路径
	path = filepath.Clean(path)
	if path == "" {
		path = "/"
	}

	// 如果是根目录
	if path == "/" {
		return &DirEntry{
			Name:  "/",
			Inode: vfs.root,
			IsDir: true,
		}, nil
	}

	// 分解路径
	components := splitPath(path)
	if len(components) == 0 {
		return nil, ErrInvalidParam
	}

	// 从根目录开始查找
	parentInode := vfs.root
	var entry *meta.Entry
	var err error

	// 逐级查找
	for i, name := range components {
		isLast := i == len(components)-1
		entry, err = vfs.meta.Lookup(ctx, parentInode, name)
		if err != nil {
			return nil, mapError(err)
		}

		// 如果不是最后一级且不是目录，返回错误
		if !isLast && !entry.IsDir {
			return nil, ErrNotDir
		}

		// 更新父inode
		parentInode = entry.Inode
	}

	// 转换为DirEntry
	return &DirEntry{
		Name:  components[len(components)-1],
		Inode: entry.Inode,
		IsDir: entry.IsDir,
	}, nil
}

// Stat 获取文件属性
func (vfs *VFS) Stat(ctx context.Context, inode uint64) (*FileAttr, error) {
	attr, err := vfs.meta.GetAttr(ctx, inode)
	if err != nil {
		return nil, mapError(err)
	}

	// 转换为FileAttr
	return &FileAttr{
		Inode:  attr.Inode,
		Size:   attr.Size,
		Mode:   os.FileMode(attr.Mode),
		Mtime:  time.Unix(0, attr.Mtime),
		Atime:  time.Unix(0, attr.Atime),
		Ctime:  time.Unix(0, attr.Ctime),
		IsDir:  attr.IsDir,
		Blocks: attr.Blocks,
	}, nil
}

// ReadDir 读取目录内容
func (vfs *VFS) ReadDir(ctx context.Context, inode uint64) ([]*DirEntry, error) {
	entries, err := vfs.meta.ReadDir(ctx, inode)
	if err != nil {
		return nil, mapError(err)
	}

	// 转换为DirEntry数组
	result := make([]*DirEntry, 0, len(entries))
	for _, entry := range entries {
		result = append(result, &DirEntry{
			Name:  entry.Name,
			Inode: entry.Inode,
			IsDir: entry.IsDir,
		})
	}

	return result, nil
}

// OpenFile 打开文件
func (vfs *VFS) OpenFile(ctx context.Context, inode uint64, isDir bool) (*FileHandle, error) {
	// 生成文件句柄ID
	fh := atomic.AddUint64(&vfs.nextHandle, 1)

	// 创建文件句柄
	handle := &FileHandle{
		inode: inode,
		fh:    fh,
		isDir: isDir,
	}

	// 保存文件句柄
	vfs.handles.Store(fh, handle)

	return handle, nil
}

// CloseFile 关闭文件
func (vfs *VFS) CloseFile(ctx context.Context, inode uint64) error {
	// 这里我们不需要实际做什么，只需要在handles中删除对应的文件句柄
	vfs.handles.Range(func(key, value interface{}) bool {
		handle, ok := value.(*FileHandle)
		if ok && handle.inode == inode {
			vfs.handles.Delete(key)
			return false // 停止遍历
		}
		return true // 继续遍历
	})

	return nil
}

// Read 读取文件内容
func (vfs *VFS) Read(ctx context.Context, inode uint64, buf []byte, offset int64) (int, error) {
	return vfs.meta.Read(ctx, inode, buf, offset)
}

// Write 写入文件内容
func (vfs *VFS) Write(ctx context.Context, inode uint64, data []byte, offset int64) (int, error) {
	return vfs.meta.Write(ctx, inode, data, offset)
}

// Truncate 调整文件大小
func (vfs *VFS) Truncate(ctx context.Context, inode uint64, size int64) error {
	return vfs.meta.Truncate(ctx, inode, size)
}

// Flush 刷新文件
func (vfs *VFS) Flush(ctx context.Context, inode uint64) error {
	return vfs.meta.Flush(ctx, inode)
}

// CreateFile 创建文件
func (vfs *VFS) CreateFile(ctx context.Context, parentInode uint64, name string, mode os.FileMode) (*DirEntry, error) {
	entry, err := vfs.meta.Create(ctx, parentInode, name, uint32(mode))
	if err != nil {
		return nil, mapError(err)
	}

	return &DirEntry{
		Name:  name,
		Inode: entry.Inode,
		IsDir: entry.IsDir,
	}, nil
}

// Mkdir 创建目录
func (vfs *VFS) Mkdir(ctx context.Context, parentInode uint64, name string, mode os.FileMode) (*DirEntry, error) {
	entry, err := vfs.meta.MkDir(ctx, parentInode, name, uint32(mode))
	if err != nil {
		return nil, mapError(err)
	}

	return &DirEntry{
		Name:  name,
		Inode: entry.Inode,
		IsDir: entry.IsDir,
	}, nil
}

// Remove 删除文件或目录
func (vfs *VFS) Remove(ctx context.Context, parentInode, inode uint64, name string) error {
	err := vfs.meta.Delete(ctx, parentInode, name)
	return mapError(err)
}

// Rename 重命名文件或目录
func (vfs *VFS) Rename(ctx context.Context, srcParent, dstParent, inode uint64, srcName, dstName string) error {
	err := vfs.meta.Rename(ctx, srcParent, dstParent, srcName, dstName)
	return mapError(err)
}

// Close 关闭VFS
func (vfs *VFS) Close() error {
	// 清理所有打开的文件句柄
	vfs.handles.Range(func(key, value interface{}) bool {
		vfs.handles.Delete(key)
		return true
	})

	// 清理缓存
	vfs.inodeCache.Range(func(key, value interface{}) bool {
		vfs.inodeCache.Delete(key)
		return true
	})

	return nil
}

// 辅助函数

// splitPath 将路径分解为组件
func splitPath(path string) []string {
	// 移除前导斜杠
	if len(path) > 0 && path[0] == '/' {
		path = path[1:]
	}

	// 如果路径为空，返回空切片
	if path == "" {
		return []string{}
	}

	// 分割路径
	return strings.Split(path, "/")
}

// mapError 映射元数据错误到VFS错误
func mapError(err error) error {
	if err == nil {
		return nil
	}

	switch {
	case errors.Is(err, meta.ErrNoSuchFileOrDir):
		return ErrNotFound
	case errors.Is(err, meta.ErrNotEmpty):
		return ErrNotEmpty
	case errors.Is(err, meta.ErrIsDir):
		return ErrIsDir
	case errors.Is(err, meta.ErrNotDir):
		return ErrNotDir
	case errors.Is(err, meta.ErrFileExists):
		return ErrExists
	case errors.Is(err, meta.ErrAccessDenied):
		return ErrPermission
	default:
		return err
	}
} 