package fs

import (
	"candyfs/pkg/meta"
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

var (
	ErrNotFound     = errors.New("文件或目录不存在")
	ErrIsDir        = errors.New("目标是目录")
	ErrNotDir       = errors.New("目标不是目录")
	ErrExists       = errors.New("文件或目录已存在")
	ErrPermission   = errors.New("权限不足")
	ErrInvalidParam = errors.New("无效参数")
	ErrNotEmpty     = errors.New("目录不为空")
)

// FileSystem 表示整个CandyFS文件系统的实现
type FileSystem struct {
	meta    meta.Meta   // 元数据服务
	vfs     *VFS        // 虚拟文件系统层
	rootDir *Dir        // 根目录
	files   sync.Map    // 已打开的文件映射 inode -> File
	mu      sync.RWMutex
	logger  *logrus.Logger
	sessions map[uint64]*Session
}

// Dir 表示文件系统中的目录
type Dir struct {
	fs     *FileSystem // 所属文件系统
	inode  uint64      // 目录inode号
	name   string      // 目录名称
	parent uint64      // 父目录inode号
	path   string      // 完整路径
	mu     sync.RWMutex
}

// File 表示一个打开的文件
type File struct {
	fs       *FileSystem // 所属文件系统
	inode    uint64      // 文件inode号
	name     string      // 文件名称
	size     int64       // 文件大小
	mode     os.FileMode // 文件权限
	mtime    time.Time   // 修改时间
	flags    int         // 打开标志
	position int64       // 当前读写位置
	writer   *Writer     // 写入处理器
	mu       sync.RWMutex
}

// FileStat 表示文件状态信息
type FileStat struct {
	Inode  uint64      // inode号
	Size   int64       // 文件大小
	Mode   os.FileMode // 文件类型和权限
	ModTime time.Time  // 修改时间
}

// Config 文件系统配置
type Config struct {
	VFSConfig      *VFSConfig  // 虚拟文件系统配置
	MountPath      string      // 挂载路径
	DebugMode      bool        // 调试模式
	LogLevel       string      // 日志级别
	MaxOpenFiles   int         // 最大打开文件数
	IdleTimeout    time.Duration // 会话空闲超时
}

// Session 表示一个用户会话
type Session struct {
	id          uint64
	fs          *FileSystem
	openFiles   map[uint64]*OpenFile
	lastAccess  time.Time
	mu          sync.Mutex
}

// OpenFile 表示一个打开的文件
type OpenFile struct {
	handle     *FileHandle
	flags      int
	lastAccess time.Time
}

// FileInfo 提供文件信息
type FileInfo struct {
	Name    string      // 文件名
	Size    int64       // 文件大小
	Mode    os.FileMode // 文件模式
	ModTime time.Time   // 修改时间
	IsDir   bool        // 是否为目录
	Inode   uint64      // inode号
}

// NewFileSystem 创建一个新的CandyFS文件系统
func NewFileSystem(metaClient meta.Meta, vfs *VFS) *FileSystem {
	fs := &FileSystem{
		meta: metaClient,
		vfs:  vfs,
	}
	
	// 创建根目录
	fs.rootDir = &Dir{
		fs:     fs,
		inode:  1, // 根目录的inode为1
		name:   "/",
		parent: 0, // 根目录没有父目录
		path:   "/",
	}
	
	return fs
}

// RootDir 返回文件系统的根目录
func (fs *FileSystem) RootDir() *Dir {
	return fs.rootDir
}

// Lookup 根据路径查找文件或目录
func (fs *FileSystem) Lookup(ctx context.Context, path string) (interface{}, error) {
	// 通过VFS层查找
	entry, err := fs.vfs.Lookup(ctx, path)
	if err != nil {
		return nil, err
	}
	
	// 根据类型创建Dir或File对象
	if entry.IsDir {
		return &Dir{
			fs:     fs,
			inode:  entry.Inode,
			name:   entry.Name,
			parent: entry.Parent,
			path:   path,
		}, nil
	}
	
	return &File{
		fs:     fs,
		inode:  entry.Inode,
		name:   entry.Name,
		size:   entry.Size,
		mode:   entry.Mode,
		mtime:  entry.ModTime,
	}, nil
}

// OpenFile 打开或创建文件
func (fs *FileSystem) OpenFile(ctx context.Context, path string, flags int, mode os.FileMode) (*File, error) {
	// 通过VFS层打开文件
	fh, err := fs.vfs.OpenFile(ctx, path, flags, mode)
	if err != nil {
		return nil, err
	}
	
	file := &File{
		fs:     fs,
		inode:  fh.Inode,
		name:   fh.Name,
		size:   fh.Size,
		mode:   fh.Mode,
		mtime:  fh.ModTime,
		flags:  flags,
		writer: NewWriter(fs.vfs, fh.Inode, fh.Size),
	}
	
	// 将文件存入打开文件映射
	fs.files.Store(fh.Inode, file)
	
	return file, nil
}

// Mkdir 创建目录
func (fs *FileSystem) Mkdir(ctx context.Context, path string, mode os.FileMode) error {
	return fs.vfs.Mkdir(ctx, path, mode)
}

// Remove 删除文件或目录
func (fs *FileSystem) Remove(ctx context.Context, path string) error {
	return fs.vfs.Remove(ctx, path)
}

// Rename 重命名文件或目录
func (fs *FileSystem) Rename(ctx context.Context, oldPath, newPath string) error {
	return fs.vfs.Rename(ctx, oldPath, newPath)
}

// Stat 获取文件或目录的状态信息
func (fs *FileSystem) Stat(ctx context.Context, path string) (*FileStat, error) {
	attr, err := fs.vfs.Stat(ctx, path)
	if err != nil {
		return nil, err
	}
	
	return &FileStat{
		Inode:   attr.Inode,
		Size:    attr.Size,
		Mode:    attr.Mode,
		ModTime: attr.ModTime,
	}, nil
}

// ReadDir 读取目录内容
func (d *Dir) ReadDir(ctx context.Context) ([]interface{}, error) {
	entries, err := d.fs.vfs.ReadDir(ctx, d.path)
	if err != nil {
		return nil, err
	}
	
	result := make([]interface{}, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir {
			dir := &Dir{
				fs:     d.fs,
				inode:  entry.Inode,
				name:   entry.Name,
				parent: d.inode,
				path:   d.path + "/" + entry.Name,
			}
			result = append(result, dir)
		} else {
			file := &File{
				fs:    d.fs,
				inode: entry.Inode,
				name:  entry.Name,
				size:  entry.Size,
				mode:  entry.Mode,
				mtime: entry.ModTime,
			}
			result = append(result, file)
		}
	}
	
	return result, nil
}

// Read 从文件读取数据
func (f *File) Read(p []byte) (int, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	
	if f.position >= f.size {
		return 0, io.EOF
	}
	
	n, err := f.fs.vfs.Read(context.Background(), f.inode, p, f.position)
	if err != nil {
		return n, err
	}
	
	f.position += int64(n)
	return n, nil
}

// Write 向文件写入数据
func (f *File) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	
	n, err := f.writer.Write(p)
	if err != nil {
		return n, err
	}
	
	f.position += int64(n)
	if f.position > f.size {
		f.size = f.position
	}
	
	return n, nil
}

// Seek 设置下一次读写的位置
func (f *File) Seek(offset int64, whence int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	
	var newPosition int64
	switch whence {
	case io.SeekStart:
		newPosition = offset
	case io.SeekCurrent:
		newPosition = f.position + offset
	case io.SeekEnd:
		newPosition = f.size + offset
	default:
		return f.position, os.ErrInvalid
	}
	
	if newPosition < 0 {
		return f.position, os.ErrInvalid
	}
	
	f.position = newPosition
	return f.position, nil
}

// Close 关闭文件
func (f *File) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	
	// 确保所有数据都写入存储
	if f.writer != nil {
		if err := f.writer.Flush(); err != nil {
			return err
		}
	}
	
	// 从打开文件映射中移除
	f.fs.files.Delete(f.inode)
	
	return f.fs.vfs.CloseFile(context.Background(), f.inode)
}

// Flush 强制将缓冲数据写入存储
func (f *File) Flush() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	
	if f.writer != nil {
		return f.writer.Flush()
	}
	
	return nil
}

// Stat 获取文件状态信息
func (f *File) Stat() *FileStat {
	f.mu.RLock()
	defer f.mu.RUnlock()
	
	return &FileStat{
		Inode:   f.inode,
		Size:    f.size,
		Mode:    f.mode,
		ModTime: f.mtime,
	}
}

// New 创建新的文件系统实例
func New(config *Config) (*FileSystem, error) {
	logger := logrus.New()
	level, err := logrus.ParseLevel(config.LogLevel)
	if err != nil {
		level = logrus.InfoLevel
	}
	logger.SetLevel(level)

	if config.DebugMode {
		logger.SetLevel(logrus.DebugLevel)
	}

	// 创建元数据服务客户端
	metaClient := meta.NewClient(config.VFSConfig.MetaURI, config.VFSConfig.MetaConf)

	// 创建虚拟文件系统
	vfs, err := NewVFS(metaClient, config.VFSConfig)
	if err != nil {
		return nil, err
	}

	if config.MaxOpenFiles <= 0 {
		config.MaxOpenFiles = 1000
	}

	if config.IdleTimeout <= 0 {
		config.IdleTimeout = 5 * time.Minute
	}

	fs := &FileSystem{
		vfs:       vfs,
		logger:    logger,
		sessions:  make(map[uint64]*Session),
	}

	// 启动会话清理器
	go fs.sessionCleaner(config.IdleTimeout)

	return fs, nil
}

// sessionCleaner 定期清理空闲会话
func (fs *FileSystem) sessionCleaner(idleTimeout time.Duration) {
	ticker := time.NewTicker(idleTimeout / 2)
	defer ticker.Stop()

	for range ticker.C {
		fs.cleanIdleSessions(idleTimeout)
	}
}

// cleanIdleSessions 清理空闲会话
func (fs *FileSystem) cleanIdleSessions(idleTimeout time.Duration) {
	now := time.Now()
	var sessionsToClean []uint64

	fs.mu.RLock()
	for id, session := range fs.sessions {
		if now.Sub(session.lastAccess) > idleTimeout {
			sessionsToClean = append(sessionsToClean, id)
		}
	}
	fs.mu.RUnlock()

	for _, id := range sessionsToClean {
		fs.CloseSession(id)
	}
}

// CreateSession 创建新会话
func (fs *FileSystem) CreateSession() uint64 {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// 生成唯一会话ID
	id := uint64(time.Now().UnixNano())
	session := &Session{
		id:         id,
		fs:         fs,
		openFiles:  make(map[uint64]*OpenFile),
		lastAccess: time.Now(),
	}
	
	fs.sessions[id] = session
	fs.logger.Debugf("创建新会话: %d", id)
	
	return id
}

// GetSession 获取会话
func (fs *FileSystem) GetSession(sessionID uint64) *Session {
	fs.mu.RLock()
	session, exists := fs.sessions[sessionID]
	fs.mu.RUnlock()

	if exists {
		session.mu.Lock()
		session.lastAccess = time.Now()
		session.mu.Unlock()
	}

	return session
}

// CloseSession 关闭会话
func (fs *FileSystem) CloseSession(sessionID uint64) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	session, exists := fs.sessions[sessionID]
	if !exists {
		return nil
	}

	// 关闭所有打开的文件
	session.mu.Lock()
	for _, file := range session.openFiles {
		_ = fs.vfs.CloseFile(context.Background(), file.handle.inode)
	}
	session.mu.Unlock()

	delete(fs.sessions, sessionID)
	fs.logger.Debugf("关闭会话: %d", sessionID)

	return nil
}

// 会话方法

// Open 打开文件
func (s *Session) Open(path string, flags int, mode os.FileMode) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.lastAccess = time.Now()
	ctx := context.Background()

	// 查找文件
	entry, err := s.fs.vfs.Lookup(ctx, path)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return 0, err
	}

	// 如果文件不存在且有创建标志，则创建文件
	if errors.Is(err, ErrNotFound) && (flags&os.O_CREATE != 0) {
		// 获取父目录
		parentPath := GetParentPath(path)
		parent, err := s.fs.vfs.Lookup(ctx, parentPath)
		if err != nil {
			return 0, err
		}

		if !parent.IsDir {
			return 0, ErrNotDir
		}

		// 创建新文件
		filename := GetFilename(path)
		entry, err = s.fs.vfs.CreateFile(ctx, parent.Inode, filename, mode)
		if err != nil {
			return 0, err
		}
	} else if err != nil {
		return 0, err
	} else if flags&os.O_EXCL != 0 && flags&os.O_CREATE != 0 {
		// O_EXCL与O_CREATE同时使用时，文件必须不存在
		return 0, ErrExists
	}

	// 检查是否为目录
	if entry.IsDir && (flags&os.O_WRONLY != 0 || flags&os.O_RDWR != 0) {
		return 0, ErrIsDir
	}

	// 截断文件
	if flags&os.O_TRUNC != 0 && !entry.IsDir {
		err = s.fs.vfs.Truncate(ctx, entry.Inode, 0)
		if err != nil {
			return 0, err
		}
	}

	// 打开文件
	handle, err := s.fs.vfs.OpenFile(ctx, entry.Inode, entry.IsDir)
	if err != nil {
		return 0, err
	}

	// 创建OpenFile结构
	openFile := &OpenFile{
		handle:     handle,
		flags:      flags,
		lastAccess: time.Now(),
	}

	s.openFiles[handle.fh] = openFile
	s.fs.logger.Debugf("打开文件: %s, fh: %d", path, handle.fh)

	return handle.fh, nil
}

// Close 关闭文件
func (s *Session) Close(fh uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.lastAccess = time.Now()
	
	openFile, exists := s.openFiles[fh]
	if !exists {
		return ErrNotFound
	}

	// 关闭文件
	err := s.fs.vfs.CloseFile(context.Background(), openFile.handle.inode)
	if err != nil {
		return err
	}

	delete(s.openFiles, fh)
	s.fs.logger.Debugf("关闭文件: fh: %d", fh)

	return nil
}

// Read 读取文件内容
func (s *Session) Read(fh uint64, buf []byte, offset int64) (int, error) {
	s.mu.Lock()
	openFile, exists := s.openFiles[fh]
	s.mu.Unlock()

	if !exists {
		return 0, ErrNotFound
	}

	// 更新访问时间
	openFile.lastAccess = time.Now()
	s.lastAccess = time.Now()

	// 检查是否有读权限
	if openFile.flags&os.O_WRONLY != 0 {
		return 0, ErrPermission
	}

	// 读取文件
	return s.fs.vfs.Read(context.Background(), openFile.handle.inode, buf, offset)
}

// Write 写入文件内容
func (s *Session) Write(fh uint64, data []byte, offset int64) (int, error) {
	s.mu.Lock()
	openFile, exists := s.openFiles[fh]
	s.mu.Unlock()

	if !exists {
		return 0, ErrNotFound
	}

	// 更新访问时间
	openFile.lastAccess = time.Now()
	s.lastAccess = time.Now()

	// 检查是否有写权限
	if openFile.flags&(os.O_WRONLY|os.O_RDWR) == 0 {
		return 0, ErrPermission
	}

	// 写入文件
	return s.fs.vfs.Write(context.Background(), openFile.handle.inode, data, offset)
}

// Mkdir 创建目录
func (s *Session) Mkdir(path string, mode os.FileMode) error {
	s.lastAccess = time.Now()
	ctx := context.Background()

	// 检查父目录是否存在
	parentPath := GetParentPath(path)
	parent, err := s.fs.vfs.Lookup(ctx, parentPath)
	if err != nil {
		return err
	}

	if !parent.IsDir {
		return ErrNotDir
	}

	// 检查路径是否已存在
	filename := GetFilename(path)
	_, err = s.fs.vfs.Lookup(ctx, path)
	if err == nil {
		return ErrExists
	} else if !errors.Is(err, ErrNotFound) {
		return err
	}

	// 创建目录
	_, err = s.fs.vfs.Mkdir(ctx, parent.Inode, filename, mode|os.ModeDir)
	return err
}

// Remove 删除文件或空目录
func (s *Session) Remove(path string) error {
	s.lastAccess = time.Now()
	ctx := context.Background()

	// 查找文件或目录
	entry, err := s.fs.vfs.Lookup(ctx, path)
	if err != nil {
		return err
	}

	// 如果是目录，检查是否为空
	if entry.IsDir {
		entries, err := s.fs.vfs.ReadDir(ctx, entry.Inode)
		if err != nil {
			return err
		}

		if len(entries) > 0 {
			return ErrNotEmpty
		}
	}

	// 获取父目录
	parentPath := GetParentPath(path)
	parent, err := s.fs.vfs.Lookup(ctx, parentPath)
	if err != nil {
		return err
	}

	// 删除文件或目录
	filename := GetFilename(path)
	return s.fs.vfs.Remove(ctx, parent.Inode, entry.Inode, filename)
}

// Rename 重命名文件或目录
func (s *Session) Rename(oldPath, newPath string) error {
	s.lastAccess = time.Now()
	ctx := context.Background()

	// 查找源文件或目录
	oldEntry, err := s.fs.vfs.Lookup(ctx, oldPath)
	if err != nil {
		return err
	}

	// 获取源和目标的父目录
	oldParentPath := GetParentPath(oldPath)
	oldParent, err := s.fs.vfs.Lookup(ctx, oldParentPath)
	if err != nil {
		return err
	}

	newParentPath := GetParentPath(newPath)
	newParent, err := s.fs.vfs.Lookup(ctx, newParentPath)
	if err != nil {
		return err
	}

	// 检查目标是否已存在
	newFilename := GetFilename(newPath)
	newEntry, err := s.fs.vfs.Lookup(ctx, newPath)
	if err == nil {
		// 目标已存在，如果是目录则确保为空
		if newEntry.IsDir {
			entries, err := s.fs.vfs.ReadDir(ctx, newEntry.Inode)
			if err != nil {
				return err
			}

			if len(entries) > 0 {
				return ErrNotEmpty
			}
		}
		
		// 删除目标
		err = s.fs.vfs.Remove(ctx, newParent.Inode, newEntry.Inode, newFilename)
		if err != nil {
			return err
		}
	} else if !errors.Is(err, ErrNotFound) {
		return err
	}

	// 重命名
	oldFilename := GetFilename(oldPath)
	return s.fs.vfs.Rename(ctx, oldParent.Inode, newParent.Inode, oldEntry.Inode, oldFilename, newFilename)
}

// Truncate 调整文件大小
func (s *Session) Truncate(fh uint64, size int64) error {
	s.mu.Lock()
	openFile, exists := s.openFiles[fh]
	s.mu.Unlock()

	if !exists {
		return ErrNotFound
	}

	// 更新访问时间
	openFile.lastAccess = time.Now()
	s.lastAccess = time.Now()

	// 检查是否有写权限
	if openFile.flags&(os.O_WRONLY|os.O_RDWR) == 0 {
		return ErrPermission
	}

	// 调整文件大小
	return s.fs.vfs.Truncate(context.Background(), openFile.handle.inode, size)
}

// Stat 获取文件信息
func (s *Session) Stat(path string) (*FileInfo, error) {
	s.lastAccess = time.Now()
	ctx := context.Background()

	// 查找文件或目录
	entry, err := s.fs.vfs.Lookup(ctx, path)
	if err != nil {
		return nil, err
	}

	// 获取文件属性
	attr, err := s.fs.vfs.Stat(ctx, entry.Inode)
	if err != nil {
		return nil, err
	}

	// 转换为FileInfo
	info := &FileInfo{
		Name:    GetFilename(path),
		Size:    attr.Size,
		Mode:    attr.Mode,
		ModTime: attr.Mtime,
		IsDir:   attr.IsDir,
		Inode:   attr.Inode,
	}

	return info, nil
}

// Flush 将缓存数据刷新到存储
func (s *Session) Flush(fh uint64) error {
	s.mu.Lock()
	openFile, exists := s.openFiles[fh]
	s.mu.Unlock()

	if !exists {
		return ErrNotFound
	}

	// 更新访问时间
	openFile.lastAccess = time.Now()
	s.lastAccess = time.Now()

	// 刷新文件
	return s.fs.vfs.Flush(context.Background(), openFile.handle.inode)
}

// ReadDir 读取目录内容
func (s *Session) ReadDir(path string) ([]*FileInfo, error) {
	s.lastAccess = time.Now()
	ctx := context.Background()

	// 查找目录
	entry, err := s.fs.vfs.Lookup(ctx, path)
	if err != nil {
		return nil, err
	}

	if !entry.IsDir {
		return nil, ErrNotDir
	}

	// 读取目录内容
	entries, err := s.fs.vfs.ReadDir(ctx, entry.Inode)
	if err != nil {
		return nil, err
	}

	// 转换为FileInfo切片
	infos := make([]*FileInfo, 0, len(entries))
	for _, e := range entries {
		// 获取文件属性
		attr, err := s.fs.vfs.Stat(ctx, e.Inode)
		if err != nil {
			continue // 跳过错误的条目
		}

		info := &FileInfo{
			Name:    e.Name,
			Size:    attr.Size,
			Mode:    attr.Mode,
			ModTime: attr.Mtime,
			IsDir:   attr.IsDir,
			Inode:   attr.Inode,
		}
		infos = append(infos, info)
	}

	return infos, nil
}

// Close 关闭文件系统
func (fs *FileSystem) Close() error {
	// 关闭所有会话
	fs.mu.Lock()
	for id := range fs.sessions {
		fs.CloseSession(id)
	}
	fs.mu.Unlock()

	// 关闭VFS
	return fs.vfs.Close()
}

// 辅助函数

// GetParentPath 获取路径的父目录部分
func GetParentPath(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			if i == 0 {
				return "/"
			}
			return path[:i]
		}
	}
	return "/"
}

// GetFilename 获取路径的文件名部分
func GetFilename(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			return path[i+1:]
		}
	}
	return path
} 