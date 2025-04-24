package fuse

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"candyfs/pkg/fs"

	fusefs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/sirupsen/logrus"
)

// Mounter FUSE挂载器
type Mounter struct {
	fs        *fs.FileSystem
	mountPath string
	server    *fuse.Server
	logger    *logrus.Logger
}

// NewMounter 创建新的挂载器
func NewMounter(fs *fs.FileSystem, mountPath string, logger *logrus.Logger) *Mounter {
	return &Mounter{
		fs:        fs,
		mountPath: mountPath,
		logger:    logger,
	}
}

// Mount 挂载文件系统
func (m *Mounter) Mount() error {
	// 确保挂载点存在
	err := os.MkdirAll(m.mountPath, 0755)
	if err != nil {
		return fmt.Errorf("创建挂载点失败: %w", err)
	}

	// 检查挂载点是否为空目录
	entries, err := os.ReadDir(m.mountPath)
	if err != nil {
		return fmt.Errorf("读取挂载点失败: %w", err)
	}
	if len(entries) > 0 {
		return fmt.Errorf("挂载点必须为空目录")
	}

	// 创建根节点
	rootNode := &fuseDirNode{
		fs:     m.fs,
		logger: m.logger,
		path:   "/",
	}

	// 创建节点文件系统
	nodeFS := fusefs.NewNodeFS(rootNode, &fusefs.Options{})

	// 创建FUSE服务器
	mountOptions := &fuse.MountOptions{
		FsName:     "candyfs",
		Name:       "candyfs",
		Debug:      m.logger.GetLevel() == logrus.DebugLevel,
		AllowOther: true,
		Options:    []string{"default_permissions"},
	}

	server, _, err := fuse.Mount(m.mountPath, nodeFS, mountOptions)
	if err != nil {
		return fmt.Errorf("挂载FUSE文件系统失败: %w", err)
	}

	m.server = server
	m.logger.Infof("CandyFS 已挂载到 %s", m.mountPath)

	// 启动服务
	go server.Serve()

	return nil
}

// Unmount 卸载文件系统
func (m *Mounter) Unmount() error {
	if m.server != nil {
		// 等待进行中的请求完成
		m.logger.Infof("等待请求完成...")
		time.Sleep(200 * time.Millisecond)

		// 卸载
		err := m.server.Unmount()
		if err != nil {
			// 如果正常卸载失败，尝试强制卸载
			m.logger.Warnf("正常卸载失败，尝试强制卸载: %s", err)
			cmd := exec.Command("fusermount", "-u", "-z", m.mountPath)
			if err := cmd.Run(); err != nil {
				return fmt.Errorf("强制卸载失败: %w", err)
			}
		}

		m.logger.Infof("CandyFS 已从 %s 卸载", m.mountPath)
		m.server = nil
	}
	return nil
}

// Wait 等待直到文件系统被卸载
func (m *Mounter) Wait() {
	if m.server == nil {
		return
	}

	// 设置信号处理
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	// 等待信号或服务器退出
	select {
	case sig := <-signalChan:
		m.logger.Infof("收到信号 %s, 卸载文件系统", sig)
		m.Unmount()
	}
}

// fuseDirNode 目录节点
type fuseDirNode struct {
	fusefs.Inode
	fs     *fs.FileSystem
	logger *logrus.Logger
	path   string
}

// Getattr 获取文件属性
func (d *fuseDirNode) Getattr(ctx context.Context, fh fusefs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	// 目录总是存在的
	out.Mode = uint32(os.ModeDir | 0755)
	out.Uid = uint32(os.Getuid())
	out.Gid = uint32(os.Getgid())
	
	// 设置时间
	now := uint64(time.Now().Unix())
	out.Atime = now
	out.Mtime = now
	out.Ctime = now
	
	return 0
}

// Lookup 查找文件或目录
func (d *fuseDirNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fusefs.Inode, syscall.Errno) {
	// 在这里实现文件查找逻辑
	// 这是一个基本示例，实际实现需要整合fs包的功能
	return nil, syscall.ENOSYS
}

// Opendir 打开目录
func (d *fuseDirNode) Opendir(ctx context.Context) (fusefs.FileHandle, syscall.Errno) {
	return nil, 0
}

// Readdir 读取目录内容
func (d *fuseDirNode) Readdir(ctx context.Context) (fusefs.DirStream, syscall.Errno) {
	// 在这里实现目录内容读取逻辑
	// 这是一个基本示例，实际实现需要整合fs包的功能
	return nil, syscall.ENOSYS
}

// Create 创建文件
func (d *fuseDirNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (fusefs.InodeEmbedder, fusefs.FileHandle, uint32, syscall.Errno) {
	// 在这里实现文件创建逻辑
	// 这是一个基本示例，实际实现需要整合fs包的功能
	return nil, nil, 0, syscall.ENOSYS
}

// Mkdir 创建目录
func (d *fuseDirNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fusefs.Inode, syscall.Errno) {
	// 在这里实现目录创建逻辑
	// 这是一个基本示例，实际实现需要整合fs包的功能
	return nil, syscall.ENOSYS
}

// Rmdir 删除目录
func (d *fuseDirNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	// 在这里实现目录删除逻辑
	// 这是一个基本示例，实际实现需要整合fs包的功能
	return syscall.ENOSYS
}

// Unlink 删除文件
func (d *fuseDirNode) Unlink(ctx context.Context, name string) syscall.Errno {
	// 在这里实现文件删除逻辑
	// 这是一个基本示例，实际实现需要整合fs包的功能
	return syscall.ENOSYS
}

// Rename 重命名文件或目录
func (d *fuseDirNode) Rename(ctx context.Context, oldName string, newParent fusefs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	// 在这里实现重命名逻辑
	// 这是一个基本示例，实际实现需要整合fs包的功能
	return syscall.ENOSYS
} 