package meta

import (
	"os"
	"time"
)

// Entry 表示文件系统中的一个条目（文件或目录）
type Entry struct {
	Inode    uint64      // inode号
	Name     string      // 名称
	Size     int64       // 大小
	Mode     uint32      // 模式和权限
	Mtime    int64       // 修改时间（纳秒）
	Atime    int64       // 访问时间（纳秒）
	Ctime    int64       // 创建时间（纳秒）
	IsDir    bool        // 是否为目录
	Blocks   int64       // 占用的块数
	ParentID uint64      // 父目录inode
}

// Attr 文件属性结构
type Attr struct {
	Inode  uint64 // inode号
	Size   int64  // 文件大小
	Mode   uint32 // 文件模式
	Mtime  int64  // 修改时间（纳秒）
	Atime  int64  // 访问时间（纳秒）
	Ctime  int64  // 创建时间（纳秒）
	IsDir  bool   // 是否为目录
	Blocks int64  // 占用块数
}

// ToFileInfo 将Entry转换为os.FileInfo
func (e *Entry) ToFileInfo() os.FileInfo {
	return &fileInfo{e}
}

// 实现os.FileInfo接口的私有类型
type fileInfo struct {
	entry *Entry
}

func (f *fileInfo) Name() string {
	return f.entry.Name
}

func (f *fileInfo) Size() int64 {
	return f.entry.Size
}

func (f *fileInfo) Mode() os.FileMode {
	return os.FileMode(f.entry.Mode)
}

func (f *fileInfo) ModTime() time.Time {
	return time.Unix(0, f.entry.Mtime)
}

func (f *fileInfo) IsDir() bool {
	return f.entry.IsDir
}

func (f *fileInfo) Sys() interface{} {
	return f.entry
} 