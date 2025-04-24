package fs

import (
	"context"
	"sync"
	"time"
)

// Writer 定义了文件数据写入接口
type Writer interface {
	// Write 写入数据到指定偏移位置
	Write(ctx context.Context, inode uint64, data []byte, offset int64) (int, error)
	
	// Flush 将缓存的数据写入到底层存储
	Flush(ctx context.Context, inode uint64) error
	
	// Close 关闭文件，完成所有待处理的写入
	Close(ctx context.Context, inode uint64) error
	
	// Truncate 调整文件大小
	Truncate(ctx context.Context, inode uint64, size int64) error
}

// BufferedWriter 实现基于缓冲区的Writer接口
type BufferedWriter struct {
	// 存储接口，用于实际数据存储
	storage Storage
	
	// 文件缓冲区映射表
	buffers   map[uint64]*fileBuffer
	
	// 互斥锁，保护buffers映射
	mu        sync.RWMutex
	
	// 缓冲区大小
	bufferSize int
	
	// 最大缓冲区数量
	maxBuffers int
	
	// 自动刷新间隔
	flushInterval time.Duration
	
	// 定时刷新器
	flushTimer *time.Timer
	
	// 是否正在运行
	running bool
}

// fileBuffer 表示单个文件的写缓冲区
type fileBuffer struct {
	inode       uint64         // 文件的inode号
	buffer      []byte         // 数据缓冲区
	size        int64          // 文件当前大小
	dirty       bool           // 缓冲区是否被修改过
	lastAccess  time.Time      // 最后访问时间
	lastFlush   time.Time      // 最后刷新时间
	mu          sync.Mutex     // 缓冲区锁
}

// NewWriter 创建一个新的BufferedWriter
func NewWriter(storage Storage, bufferSize int, maxBuffers int, flushInterval time.Duration) Writer {
	w := &BufferedWriter{
		storage:       storage,
		buffers:       make(map[uint64]*fileBuffer),
		bufferSize:    bufferSize,
		maxBuffers:    maxBuffers,
		flushInterval: flushInterval,
		running:       true,
	}
	
	// 启动定时刷新协程
	go w.flushLoop()
	
	return w
}

// flushLoop 定期自动刷新脏缓冲区
func (w *BufferedWriter) flushLoop() {
	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()
	
	for w.running {
		<-ticker.C
		w.flushAllDirty()
	}
}

// flushAllDirty 刷新所有脏缓冲区
func (w *BufferedWriter) flushAllDirty() {
	var dirtyBuffers []uint64
	
	// 找出所有脏缓冲区
	w.mu.RLock()
	for inode, buf := range w.buffers {
		if buf.dirty {
			dirtyBuffers = append(dirtyBuffers, inode)
		}
	}
	w.mu.RUnlock()
	
	// 刷新所有脏缓冲区
	ctx := context.Background()
	for _, inode := range dirtyBuffers {
		_ = w.Flush(ctx, inode) // 忽略错误，继续刷新其他缓冲区
	}
}

// getBuffer 获取文件缓冲区，如果不存在则创建
func (w *BufferedWriter) getBuffer(inode uint64) *fileBuffer {
	w.mu.RLock()
	buf, ok := w.buffers[inode]
	w.mu.RUnlock()
	
	if ok {
		buf.lastAccess = time.Now()
		return buf
	}
	
	// 缓冲区不存在，创建新的
	w.mu.Lock()
	defer w.mu.Unlock()
	
	// 再次检查，可能其他协程已经创建
	if buf, ok = w.buffers[inode]; ok {
		buf.lastAccess = time.Now()
		return buf
	}
	
	// 检查是否达到最大缓冲区数量
	if len(w.buffers) >= w.maxBuffers {
		w.evictOldestBuffer()
	}
	
	// 创建新缓冲区
	buf = &fileBuffer{
		inode:      inode,
		buffer:     make([]byte, w.bufferSize),
		lastAccess: time.Now(),
		lastFlush:  time.Now(),
	}
	
	// 获取文件当前大小
	fileSize, err := w.storage.GetSize(context.Background(), inode)
	if err == nil {
		buf.size = fileSize
	}
	
	w.buffers[inode] = buf
	return buf
}

// evictOldestBuffer 淘汰最旧的缓冲区以释放空间
func (w *BufferedWriter) evictOldestBuffer() {
	var oldestInode uint64
	var oldestTime time.Time
	var found bool
	
	// 找出最久未访问的缓冲区
	for inode, buf := range w.buffers {
		if !found || buf.lastAccess.Before(oldestTime) {
			oldestInode = inode
			oldestTime = buf.lastAccess
			found = true
		}
	}
	
	if found {
		// 如果是脏缓冲区，先刷新
		buf := w.buffers[oldestInode]
		if buf.dirty {
			ctx := context.Background()
			_ = w.Flush(ctx, oldestInode)
		}
		
		// 删除缓冲区
		delete(w.buffers, oldestInode)
	}
}

// Write 实现Writer接口的Write方法
func (w *BufferedWriter) Write(ctx context.Context, inode uint64, data []byte, offset int64) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	
	buf := w.getBuffer(inode)
	
	buf.mu.Lock()
	defer buf.mu.Unlock()
	
	// 如果写入位置超出缓冲区范围，直接写入存储
	if offset+int64(len(data)) > int64(w.bufferSize) {
		n, err := w.storage.Write(ctx, inode, data, offset)
		if err != nil {
			return 0, err
		}
		
		// 更新文件大小
		newSize := offset + int64(n)
		if newSize > buf.size {
			buf.size = newSize
		}
		
		return n, nil
	}
	
	// 写入缓冲区
	n := copy(buf.buffer[offset:], data)
	if n > 0 {
		buf.dirty = true
		
		// 更新文件大小
		newSize := offset + int64(n)
		if newSize > buf.size {
			buf.size = newSize
		}
	}
	
	return n, nil
}

// Flush 实现Writer接口的Flush方法
func (w *BufferedWriter) Flush(ctx context.Context, inode uint64) error {
	w.mu.RLock()
	buf, ok := w.buffers[inode]
	w.mu.RUnlock()
	
	if !ok || !buf.dirty {
		return nil // 没有缓冲区或不是脏的，无需刷新
	}
	
	buf.mu.Lock()
	defer buf.mu.Unlock()
	
	// 如果不是脏的，无需刷新
	if !buf.dirty {
		return nil
	}
	
	// 写入存储
	_, err := w.storage.Write(ctx, inode, buf.buffer[:buf.size], 0)
	if err != nil {
		return err
	}
	
	buf.dirty = false
	buf.lastFlush = time.Now()
	
	return nil
}

// Close 实现Writer接口的Close方法
func (w *BufferedWriter) Close(ctx context.Context, inode uint64) error {
	// 首先刷新文件
	err := w.Flush(ctx, inode)
	if err != nil {
		return err
	}
	
	// 从缓冲区映射中移除
	w.mu.Lock()
	delete(w.buffers, inode)
	w.mu.Unlock()
	
	// 通知存储关闭文件
	return w.storage.Close(ctx, inode)
}

// Truncate 实现Writer接口的Truncate方法
func (w *BufferedWriter) Truncate(ctx context.Context, inode uint64, size int64) error {
	buf := w.getBuffer(inode)
	
	buf.mu.Lock()
	defer buf.mu.Unlock()
	
	// 先刷新现有内容
	if buf.dirty {
		_, err := w.storage.Write(ctx, inode, buf.buffer[:buf.size], 0)
		if err != nil {
			return err
		}
		buf.dirty = false
	}
	
	// 调用存储的截断方法
	err := w.storage.Truncate(ctx, inode, size)
	if err != nil {
		return err
	}
	
	// 更新缓冲区大小
	buf.size = size
	
	return nil
}

// Storage 存储接口，由底层数据存储实现
type Storage interface {
	// Write 写入数据到存储
	Write(ctx context.Context, inode uint64, data []byte, offset int64) (int, error)
	
	// Read 从存储读取数据
	Read(ctx context.Context, inode uint64, buf []byte, offset int64) (int, error)
	
	// Truncate 调整文件大小
	Truncate(ctx context.Context, inode uint64, size int64) error
	
	// Close 关闭文件
	Close(ctx context.Context, inode uint64) error
	
	// GetSize 获取文件大小
	GetSize(ctx context.Context, inode uint64) (int64, error)
} 