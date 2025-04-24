package meta

import (
	"candyfs/utils/log"
	"context"
	"strings"
)

// Meta is a interface for a meta service for file system.
type Meta interface {
	// Name of database
	Name() string
	
	// Format initializes the metadata storage
	Format() error

	// GetAttr 获取文件属性
	GetAttr(ctx context.Context, inode uint64) (*Attr, error)

	// Lookup 查找文件或目录
	Lookup(ctx context.Context, parent uint64, name string) (*Entry, error)

	// ReadDir 读取目录内容
	ReadDir(ctx context.Context, inode uint64) ([]*Entry, error)

	// Create 创建文件
	Create(ctx context.Context, parent uint64, name string, mode uint32) (*Entry, error)

	// MkDir 创建目录
	MkDir(ctx context.Context, parent uint64, name string, mode uint32) (*Entry, error)

	// Delete 删除文件或目录
	Delete(ctx context.Context, parent uint64, name string) error

	// Rename 重命名文件或目录
	Rename(ctx context.Context, oldParent, newParent uint64, oldName, newName string) error

	// Read 读取文件内容
	Read(ctx context.Context, inode uint64, buf []byte, offset int64) (int, error)

	// Write 写入文件内容
	Write(ctx context.Context, inode uint64, data []byte, offset int64) (int, error)

	// Truncate 调整文件大小
	Truncate(ctx context.Context, inode uint64, size int64) error

	// Flush 刷新文件
	Flush(ctx context.Context, inode uint64) error

	// Close 关闭元数据服务
	Close() error
}

// MetaContext is the context for a transaction or session.
type MetaContext interface {
	context.Context
	Cancel()
	Canceled() bool
	Syscall() (uint32, string)
}

// NewClient creates a Meta client for given uri.
func NewClient(uri string, conf *Config) Meta {
	var err error
	if !strings.Contains(uri, "://") {
		uri = "redis://" + uri
	}
	p := strings.Index(uri, "://")
	if p < 0 {
		log.Fatalf("invalid uri: %s", uri)
	}

	log.Infof("Meta address: %s", uri)
	if conf == nil {
		conf = DefaultConf()
	} else {
		conf.SelfCheck()
	}
	m, err := newRedisMeta(uri[p+3:], conf)
	if err != nil {
		log.Fatalf("Meta %s is not available: %s", uri, err)
	}
	return m
}
