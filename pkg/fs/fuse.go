package fs

import (
	"candyfs/pkg/meta"
	"candyfs/utils/log"
	"context"
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// 示例文件系统的根目录
type rootNode struct {
	fs.Inode
	meta meta.Meta
}

// NewCandyFS 创建一个新的CandyFS实例
func NewCandyFS(metaClient meta.Meta) *CandyFS {
	return &CandyFS{
		meta: metaClient,
		root: &rootNode{meta: metaClient},
	}
}

// CandyFS FUSE文件系统实现
type CandyFS struct {
	meta   meta.Meta
	root   *rootNode
	server *fuse.Server
	mountPoint string
}

// Mount 挂载文件系统
func (c *CandyFS) Mount(mountPoint string, allowOther bool) error {
	c.mountPoint = mountPoint
	
	// 设置挂载选项
	opts := &fs.Options{
		MountOptions: fuse.MountOptions{
			AllowOther: allowOther,
			Name:       "candyfs",
			FsName:     "candyfs",
		},
	}
	
	// 创建文件系统
	server, err := fs.Mount(mountPoint, c.root, opts)
	if err != nil {
		return fmt.Errorf("挂载失败: %s", err)
	}
	
	c.server = server
	log.Infof("文件系统已挂载到 %s", mountPoint)
	
	return nil
}

// Serve 服务文件系统
func (c *CandyFS) Serve() {
	if c.server != nil {
		c.server.Wait()
	}
}

// Unmount 卸载文件系统
func (c *CandyFS) Unmount() error {
	if c.server != nil {
		err := c.server.Unmount()
		return err
	}
	return nil
}

// Lookup 实现查找功能
func (r *rootNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// 这是一个极简版本，只返回一个示例文件
	if name == "hello.txt" {
		// 设置文件属性
		out.Attr.Mode = uint32(0644)
		out.Attr.Size = uint64(len("Hello, World!\n"))
		out.SetAttrTimeout(1 * time.Minute)
		out.SetEntryTimeout(1 * time.Minute)
		
		// 创建文件节点
		child := &fileNode{
			content: []byte("Hello, World!\n"),
		}
		
		// 返回新的inode
		return r.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFREG}), 0
	}
	
	// 如果文件不存在
	return nil, syscall.ENOENT
}

// ReadDirAll 实现目录读取功能
func (r *rootNode) ReadDirAll(ctx context.Context) ([]fuse.DirEntry, syscall.Errno) {
	// 返回一个简单的目录列表
	entries := []fuse.DirEntry{
		{Name: "hello.txt", Mode: fuse.S_IFREG},
	}
	return entries, 0
}

// GetAttr 获取根目录属性
func (r *rootNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	// 设置根目录属性
	out.Attr.Mode = uint32(os.ModeDir | 0755)
	return 0
}

// 文件节点实现
type fileNode struct {
	fs.Inode
	content []byte
}

// GetAttr 获取文件属性
func (f *fileNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	// 设置文件属性
	out.Attr.Mode = uint32(0644)
	out.Attr.Size = uint64(len(f.content))
	return 0
}

// 打开文件
func (f *fileNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	return nil, 0, 0
}

// 读取文件内容
func (f *fileNode) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	// 如果偏移超出文件范围，返回空
	if off >= int64(len(f.content)) {
		return fuse.ReadResultData([]byte{}), 0
	}
	
	// 计算可以读取的内容
	end := off + int64(len(dest))
	if end > int64(len(f.content)) {
		end = int64(len(f.content))
	}
	
	// 返回读取的内容
	return fuse.ReadResultData(f.content[off:end]), 0
}

// 创建示例文件系统（已弃用，使用Lookup动态创建）
func createExampleFS(cfs *CandyFS) {
	// 不再需要
} 