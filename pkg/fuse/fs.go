package fuse

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"candyfs/pkg/fs"

	fusefs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/sirupsen/logrus"
)

// CandyFS 实现FUSE的文件系统接口
type CandyFS struct {
	filesystem *fusefs.FileSystem
	root       *rootInode
	session    uint64
	fsys       *fusefs.Server
	cfs        *fs.CandyFS
	logger     *logrus.Logger
}

// rootInode 是根节点的实现
type rootInode struct {
	fusefs.Inode
	candyfs *CandyFS
}

// 创建新的FUSE文件系统实现
func NewCandyFS(cfs *fs.CandyFS, logger *logrus.Logger) *CandyFS {
	candyfs := &CandyFS{
		cfs:    cfs,
		logger: logger,
	}

	// 创建会话
	candyfs.session = cfs.CreateSession()

	// 创建根节点
	rootInode := &rootInode{
		candyfs: candyfs,
	}

	// 创建FUSE文件系统
	candyfs.root = rootInode
	candyfs.filesystem = fusefs.NewNodeFS(rootInode, &fusefs.Options{
		MountOptions: fuse.MountOptions{
			Debug:      logger.GetLevel() == logrus.DebugLevel,
			AllowOther: true,
			FsName:     "candyfs",
		},
	})

	return candyfs
}

// Lookup 查找文件或目录
func (r *rootInode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fusefs.Inode, syscall.Errno) {
	session := r.candyfs.cfs.GetSession(r.candyfs.session)
	if session == nil {
		return nil, syscall.ENOENT
	}

	// 构建完整路径
	path := filepath.Join("/", name)

	// 获取文件信息
	fileInfo, err := session.Stat(path)
	if err != nil {
		if err == fs.ErrNotFound {
			return nil, syscall.ENOENT
		}
		r.candyfs.logger.Errorf("Lookup %s 失败: %s", path, err)
		return nil, syscall.EIO
	}

	// 设置属性
	setAttr(fileInfo, &out.Attr)
	out.NodeId = fileInfo.Inode
	out.Attr.Mode = uint32(fileInfo.Mode)
	setEntryTimes(fileInfo, out)

	// 创建节点
	var node fusefs.InodeEmbedder
	if fileInfo.IsDir {
		dirNode := &dirNode{
			rootInode: r,
			path:      path,
			inode:     fileInfo.Inode,
		}
		node = dirNode
	} else {
		fileNode := &fileNode{
			rootInode: r,
			path:      path,
			inode:     fileInfo.Inode,
		}
		node = fileNode
	}

	// 构建子节点
	childMode := fileInfo.Mode
	if fileInfo.IsDir {
		childMode = os.ModeDir | 0755
	}

	child := r.NewInode(ctx, node, fusefs.StableAttr{
		Mode:  uint32(childMode),
		Ino:   fileInfo.Inode,
		Owner: fuse.Owner{Uid: 0, Gid: 0},
	})

	return child, 0
}

// dirNode 目录节点
type dirNode struct {
	fusefs.Inode
	rootInode *rootInode
	path      string
	inode     uint64
}

// fileNode 文件节点
type fileNode struct {
	fusefs.Inode
	rootInode *rootInode
	path      string
	inode     uint64
	fh        uint64
}

// Getattr 获取文件属性
func (d *dirNode) Getattr(ctx context.Context, f fusefs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	session := d.rootInode.candyfs.cfs.GetSession(d.rootInode.candyfs.session)
	if session == nil {
		return syscall.ENOENT
	}

	fileInfo, err := session.Stat(d.path)
	if err != nil {
		if err == fs.ErrNotFound {
			return syscall.ENOENT
		}
		d.rootInode.candyfs.logger.Errorf("Getattr %s 失败: %s", d.path, err)
		return syscall.EIO
	}

	setAttr(fileInfo, &out.Attr)
	setTimes(fileInfo, out)
	return 0
}

// Getattr 获取文件属性
func (f *fileNode) Getattr(ctx context.Context, fileHandle fusefs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	session := f.rootInode.candyfs.cfs.GetSession(f.rootInode.candyfs.session)
	if session == nil {
		return syscall.ENOENT
	}

	fileInfo, err := session.Stat(f.path)
	if err != nil {
		if err == fs.ErrNotFound {
			return syscall.ENOENT
		}
		f.rootInode.candyfs.logger.Errorf("Getattr %s 失败: %s", f.path, err)
		return syscall.EIO
	}

	setAttr(fileInfo, &out.Attr)
	setTimes(fileInfo, out)
	return 0
}

// Open 打开文件
func (f *fileNode) Open(ctx context.Context, flags uint32) (fusefs.FileHandle, uint32, syscall.Errno) {
	session := f.rootInode.candyfs.cfs.GetSession(f.rootInode.candyfs.session)
	if session == nil {
		return nil, 0, syscall.ENOENT
	}

	// 转换标志位
	mode := os.FileMode(0644)
	goFlags := int(flags)

	// 打开文件
	fh, err := session.Open(f.path, goFlags, mode)
	if err != nil {
		if err == fs.ErrNotFound {
			return nil, 0, syscall.ENOENT
		} else if err == fs.ErrIsDir {
			return nil, 0, syscall.EISDIR
		}
		f.rootInode.candyfs.logger.Errorf("Open %s 失败: %s", f.path, err)
		return nil, 0, syscall.EIO
	}

	f.fh = fh
	handle := &fileHandle{
		fileNode: f,
		fh:       fh,
		flags:    goFlags,
	}

	// 设置DirectIO标志，这样读写操作就会直接传递给我们的实现
	return handle, fuse.FOPEN_DIRECT_IO, 0
}

// fileHandle 文件句柄
type fileHandle struct {
	fileNode *fileNode
	fh       uint64
	flags    int
}

// Read 读取文件内容
func (fh *fileHandle) Read(ctx context.Context, dest []byte, offset int64) (fuse.ReadResult, syscall.Errno) {
	session := fh.fileNode.rootInode.candyfs.cfs.GetSession(fh.fileNode.rootInode.candyfs.session)
	if session == nil {
		return nil, syscall.ENOENT
	}

	n, err := session.Read(fh.fh, dest, offset)
	if err != nil {
		if err == fs.ErrNotFound {
			return nil, syscall.ENOENT
		}
		fh.fileNode.rootInode.candyfs.logger.Errorf("Read %s 失败: %s", fh.fileNode.path, err)
		return nil, syscall.EIO
	}

	return fuse.ReadResultData(dest[:n]), 0
}

// Write 写入文件内容
func (fh *fileHandle) Write(ctx context.Context, data []byte, offset int64) (uint32, syscall.Errno) {
	session := fh.fileNode.rootInode.candyfs.cfs.GetSession(fh.fileNode.rootInode.candyfs.session)
	if session == nil {
		return 0, syscall.ENOENT
	}

	// 检查是否有写权限
	if fh.flags&(os.O_WRONLY|os.O_RDWR) == 0 {
		return 0, syscall.EACCES
	}

	n, err := session.Write(fh.fh, data, offset)
	if err != nil {
		if err == fs.ErrNotFound {
			return 0, syscall.ENOENT
		}
		fh.fileNode.rootInode.candyfs.logger.Errorf("Write %s 失败: %s", fh.fileNode.path, err)
		return 0, syscall.EIO
	}

	return uint32(n), 0
}

// Release 关闭文件
func (fh *fileHandle) Release(ctx context.Context) syscall.Errno {
	session := fh.fileNode.rootInode.candyfs.cfs.GetSession(fh.fileNode.rootInode.candyfs.session)
	if session == nil {
		return syscall.ENOENT
	}

	err := session.Close(fh.fh)
	if err != nil {
		if err == fs.ErrNotFound {
			return syscall.ENOENT
		}
		fh.fileNode.rootInode.candyfs.logger.Errorf("Release %s 失败: %s", fh.fileNode.path, err)
		return syscall.EIO
	}

	return 0
}

// Flush 刷新文件
func (fh *fileHandle) Flush(ctx context.Context) syscall.Errno {
	session := fh.fileNode.rootInode.candyfs.cfs.GetSession(fh.fileNode.rootInode.candyfs.session)
	if session == nil {
		return syscall.ENOENT
	}

	err := session.Flush(fh.fh)
	if err != nil {
		if err == fs.ErrNotFound {
			return syscall.ENOENT
		}
		fh.fileNode.rootInode.candyfs.logger.Errorf("Flush %s 失败: %s", fh.fileNode.path, err)
		return syscall.EIO
	}

	return 0
}

// Fsync 同步文件
func (fh *fileHandle) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	return fh.Flush(ctx)
}

// Readdir 读取目录内容
func (d *dirNode) Readdir(ctx context.Context) (fusefs.DirStream, syscall.Errno) {
	session := d.rootInode.candyfs.cfs.GetSession(d.rootInode.candyfs.session)
	if session == nil {
		return nil, syscall.ENOENT
	}

	entries, err := session.ReadDir(d.path)
	if err != nil {
		if err == fs.ErrNotFound {
			return nil, syscall.ENOENT
		} else if err == fs.ErrNotDir {
			return nil, syscall.ENOTDIR
		}
		d.rootInode.candyfs.logger.Errorf("Readdir %s 失败: %s", d.path, err)
		return nil, syscall.EIO
	}

	dirEntries := make([]fuse.DirEntry, 0, len(entries))
	for _, entry := range entries {
		// 创建FUSE目录条目
		mode := fuse.S_IFREG
		if entry.IsDir {
			mode = fuse.S_IFDIR
		}
		
		dirEntry := fuse.DirEntry{
			Name: entry.Name,
			Ino:  entry.Inode,
			Mode: mode,
		}
		
		dirEntries = append(dirEntries, dirEntry)
	}

	return fusefs.NewListDirStream(dirEntries), 0
}

// Mkdir 创建目录
func (d *dirNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fusefs.Inode, syscall.Errno) {
	session := d.rootInode.candyfs.cfs.GetSession(d.rootInode.candyfs.session)
	if session == nil {
		return nil, syscall.ENOENT
	}

	// 构建完整路径
	path := filepath.Join(d.path, name)

	// 创建目录
	err := session.Mkdir(path, os.FileMode(mode))
	if err != nil {
		if err == fs.ErrExists {
			return nil, syscall.EEXIST
		} else if err == fs.ErrNotDir {
			return nil, syscall.ENOTDIR
		}
		d.rootInode.candyfs.logger.Errorf("Mkdir %s 失败: %s", path, err)
		return nil, syscall.EIO
	}

	// 获取目录信息
	fileInfo, err := session.Stat(path)
	if err != nil {
		d.rootInode.candyfs.logger.Errorf("获取新创建目录信息失败 %s: %s", path, err)
		return nil, syscall.EIO
	}

	setAttr(fileInfo, &out.Attr)
	out.NodeId = fileInfo.Inode
	setEntryTimes(fileInfo, out)

	// 创建目录节点
	dirNode := &dirNode{
		rootInode: d.rootInode,
		path:      path,
		inode:     fileInfo.Inode,
	}

	child := d.NewInode(ctx, dirNode, fusefs.StableAttr{
		Mode:  uint32(os.ModeDir | os.FileMode(mode)),
		Ino:   fileInfo.Inode,
		Owner: fuse.Owner{Uid: 0, Gid: 0},
	})

	return child, 0
}

// Unlink 删除文件
func (d *dirNode) Unlink(ctx context.Context, name string) syscall.Errno {
	session := d.rootInode.candyfs.cfs.GetSession(d.rootInode.candyfs.session)
	if session == nil {
		return syscall.ENOENT
	}

	// 构建完整路径
	path := filepath.Join(d.path, name)

	// 删除文件
	err := session.Remove(path)
	if err != nil {
		if err == fs.ErrNotFound {
			return syscall.ENOENT
		} else if err == fs.ErrIsDir {
			return syscall.EISDIR
		} else if err == fs.ErrNotEmpty {
			return syscall.ENOTEMPTY
		}
		d.rootInode.candyfs.logger.Errorf("Unlink %s 失败: %s", path, err)
		return syscall.EIO
	}

	return 0
}

// Rmdir 删除目录
func (d *dirNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	session := d.rootInode.candyfs.cfs.GetSession(d.rootInode.candyfs.session)
	if session == nil {
		return syscall.ENOENT
	}

	// 构建完整路径
	path := filepath.Join(d.path, name)

	// 删除目录
	err := session.Remove(path)
	if err != nil {
		if err == fs.ErrNotFound {
			return syscall.ENOENT
		} else if err == fs.ErrNotDir {
			return syscall.ENOTDIR
		} else if err == fs.ErrNotEmpty {
			return syscall.ENOTEMPTY
		}
		d.rootInode.candyfs.logger.Errorf("Rmdir %s 失败: %s", path, err)
		return syscall.EIO
	}

	return 0
}

// Create 创建并打开文件
func (d *dirNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (node *fusefs.Inode, fh fusefs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	session := d.rootInode.candyfs.cfs.GetSession(d.rootInode.candyfs.session)
	if session == nil {
		return nil, nil, 0, syscall.ENOENT
	}

	// 构建完整路径
	path := filepath.Join(d.path, name)

	// 打开文件（会自动创建）
	fh, err := session.Open(path, int(flags)|os.O_CREATE, os.FileMode(mode))
	if err != nil {
		if err == fs.ErrExists {
			return nil, nil, 0, syscall.EEXIST
		} else if err == fs.ErrIsDir {
			return nil, nil, 0, syscall.EISDIR
		}
		d.rootInode.candyfs.logger.Errorf("Create %s 失败: %s", path, err)
		return nil, nil, 0, syscall.EIO
	}

	// 获取文件信息
	fileInfo, err := session.Stat(path)
	if err != nil {
		// 关闭文件
		_ = session.Close(fh)
		d.rootInode.candyfs.logger.Errorf("获取新创建文件信息失败 %s: %s", path, err)
		return nil, nil, 0, syscall.EIO
	}

	setAttr(fileInfo, &out.Attr)
	out.NodeId = fileInfo.Inode
	setEntryTimes(fileInfo, out)

	// 创建文件节点
	fileNode := &fileNode{
		rootInode: d.rootInode,
		path:      path,
		inode:     fileInfo.Inode,
		fh:        fh,
	}

	// 创建文件句柄
	handle := &fileHandle{
		fileNode: fileNode,
		fh:       fh,
		flags:    int(flags),
	}

	// 创建inode
	child := d.NewInode(ctx, fileNode, fusefs.StableAttr{
		Mode:  uint32(fileInfo.Mode),
		Ino:   fileInfo.Inode,
		Owner: fuse.Owner{Uid: 0, Gid: 0},
	})

	return child, handle, fuse.FOPEN_DIRECT_IO, 0
}

// Rename 重命名文件或目录
func (d *dirNode) Rename(ctx context.Context, oldName string, newParent fusefs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	session := d.rootInode.candyfs.cfs.GetSession(d.rootInode.candyfs.session)
	if session == nil {
		return syscall.ENOENT
	}

	// 构建源路径
	oldPath := filepath.Join(d.path, oldName)

	// 获取目标目录路径
	var newParentPath string
	switch newParent := newParent.(type) {
	case *dirNode:
		newParentPath = newParent.path
	case *rootInode:
		newParentPath = "/"
	default:
		return syscall.EINVAL
	}

	// 构建目标路径
	newPath := filepath.Join(newParentPath, newName)

	// 重命名文件或目录
	err := session.Rename(oldPath, newPath)
	if err != nil {
		if err == fusefs.ErrNotFound {
			return syscall.ENOENT
		} else if err == fusefs.ErrExists {
			return syscall.EEXIST
		} else if err == fusefs.ErrNotEmpty {
			return syscall.ENOTEMPTY
		}
		d.rootInode.candyfs.logger.Errorf("Rename %s -> %s 失败: %s", oldPath, newPath, err)
		return syscall.EIO
	}

	return 0
}

// Truncate 调整文件大小
func (f *fileNode) Truncate(ctx context.Context, size uint64) syscall.Errno {
	session := f.rootInode.candyfs.cfs.GetSession(f.rootInode.candyfs.session)
	if session == nil {
		return syscall.ENOENT
	}

	// 如果没有已打开的文件句柄，需要先打开文件
	var fh uint64
	var err error
	var needClose bool

	if f.fh == 0 {
		// 打开文件
		fh, err = session.Open(f.path, os.O_WRONLY, 0)
		if err != nil {
			if err == fs.ErrNotFound {
				return syscall.ENOENT
			} else if err == fs.ErrIsDir {
				return syscall.EISDIR
			}
			f.rootInode.candyfs.logger.Errorf("打开文件进行截断失败 %s: %s", f.path, err)
			return syscall.EIO
		}
		needClose = true
	} else {
		fh = f.fh
	}

	// 截断文件
	err = session.Truncate(fh, int64(size))
	if err != nil {
		if needClose {
			_ = session.Close(fh)
		}
		if err == fs.ErrNotFound {
			return syscall.ENOENT
		} else if err == fs.ErrIsDir {
			return syscall.EISDIR
		}
		f.rootInode.candyfs.logger.Errorf("Truncate %s 失败: %s", f.path, err)
		return syscall.EIO
	}

	// 如果我们打开了文件，需要关闭它
	if needClose {
		_ = session.Close(fh)
	}

	return 0
}

// 工具函数

// setAttr 设置文件属性
func setAttr(info *fs.FileInfo, attr *fuse.Attr) {
	attr.Ino = info.Inode
	attr.Size = uint64(info.Size)
	attr.Blocks = uint64((info.Size + 511) / 512)
	attr.Mode = uint32(info.Mode)
	
	// 设置所有者（这里简单设置为root:root）
	attr.Uid = 0
	attr.Gid = 0
	
	// 设置时间戳
	attr.Atime = uint64(time.Now().Unix())
	attr.Mtime = uint64(info.ModTime.Unix())
	attr.Ctime = uint64(info.ModTime.Unix())
}

// setTimes 设置时间
func setTimes(info *fs.FileInfo, out *fuse.AttrOut) {
	out.Atime = uint64(time.Now().Unix())
	out.Atimensec = 0
	out.Mtime = uint64(info.ModTime.Unix())
	out.Mtimensec = 0
	out.Ctime = uint64(info.ModTime.Unix())
	out.Ctimensec = 0
}

// setEntryTimes 设置条目时间
func setEntryTimes(info *fs.FileInfo, out *fuse.EntryOut) {
	out.Atime = uint64(time.Now().Unix())
	out.Atimensec = 0
	out.Mtime = uint64(info.ModTime.Unix())
	out.Mtimensec = 0
	out.Ctime = uint64(info.ModTime.Unix())
	out.Ctimensec = 0
} 