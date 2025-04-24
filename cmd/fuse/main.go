package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"candyfs/utils/log"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// 根目录节点
type rootNode struct {
	fs.Inode
}

// 文件节点
type fileNode struct {
	fs.Inode
	content []byte
}

// Lookup 查找文件
func (r *rootNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// 实现一个简单的只读文件系统，只有几个文件
	switch name {
	case "hello.txt":
		content := []byte("Hello, CandyFS!\n欢迎使用CandyFS分布式文件系统！\n")
		out.Attr.Mode = 0644
		out.Attr.Size = uint64(len(content))
		
		child := &fileNode{
			content: content,
		}
		
		return r.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFREG}), 0
	
	case "README.md":
		content := []byte(`# CandyFS

CandyFS是一个高性能的分布式文件系统，基于FUSE实现。

## 主要特性

- 高可用性
- 可扩展性
- 简单易用
- 强一致性
`)
		out.Attr.Mode = 0644
		out.Attr.Size = uint64(len(content))
		
		child := &fileNode{
			content: content,
		}
		
		return r.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFREG}), 0
	}
	
	return nil, syscall.ENOENT
}

// Getattr 获取根目录属性
func (r *rootNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Attr.Mode = uint32(os.ModeDir | 0755)
	return 0
}

// ReadDirAll 读取目录内容
func (r *rootNode) ReadDirAll(ctx context.Context) ([]fuse.DirEntry, syscall.Errno) {
	entries := []fuse.DirEntry{
		{Name: "hello.txt", Mode: fuse.S_IFREG},
		{Name: "README.md", Mode: fuse.S_IFREG},
	}
	return entries, 0
}

// Getattr 获取文件属性
func (f *fileNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Attr.Mode = 0644
	out.Attr.Size = uint64(len(f.content))
	return 0
}

// Open 打开文件
func (f *fileNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	return nil, 0, 0
}

// Read 读取文件内容
func (f *fileNode) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if off >= int64(len(f.content)) {
		return fuse.ReadResultData([]byte{}), 0
	}
	
	end := off + int64(len(dest))
	if end > int64(len(f.content)) {
		end = int64(len(f.content))
	}
	
	return fuse.ReadResultData(f.content[off:end]), 0
}

func main() {
	// 解析命令行参数
	mountPoint := flag.String("mount", "", "挂载点路径")
	allowOther := flag.Bool("allow-other", false, "允许其他用户访问")
	debugMode := flag.Bool("debug", false, "启用调试模式")
	flag.Parse()

	// 验证挂载点
	if *mountPoint == "" {
		fmt.Println("错误: 必须指定挂载点路径")
		flag.Usage()
		os.Exit(1)
	}

	// 检查挂载点是否存在，如果不存在则创建
	if _, err := os.Stat(*mountPoint); os.IsNotExist(err) {
		if err := os.MkdirAll(*mountPoint, 0755); err != nil {
			fmt.Printf("无法创建挂载点目录: %s\n", err)
			os.Exit(1)
		}
	}

	// 设置日志级别
	if *debugMode {
		log.Debug("启用调试模式")
	} else {
		log.Info("启动CandyFS")
	}

	// 创建根节点
	rootNode := &rootNode{}

	// 设置挂载选项
	opts := &fs.Options{
		MountOptions: fuse.MountOptions{
			AllowOther: *allowOther,
			Name:       "candyfs",
			FsName:     "candyfs",
			Debug:      *debugMode,
		},
	}

	// 挂载文件系统
	server, err := fs.Mount(*mountPoint, rootNode, opts)
	if err != nil {
		fmt.Printf("挂载文件系统失败: %s\n", err)
		os.Exit(1)
	}

	// 输出成功信息
	fmt.Printf("CandyFS 已成功挂载到 %s\n", *mountPoint)
	fmt.Println("按 Ctrl+C 卸载并退出")

	// 捕获信号以优雅退出
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// 在后台启动服务
	go server.Wait()

	// 等待退出信号
	<-sigChan
	fmt.Println("\n正在卸载文件系统...")

	// 卸载文件系统
	if err := server.Unmount(); err != nil {
		fmt.Printf("卸载文件系统失败: %s\n", err)
		os.Exit(1)
	}

	fmt.Println("文件系统已成功卸载")
} 