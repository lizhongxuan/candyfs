package meta

import (
	"context"
	"sync"
	"time"
)

// Background 创建一个新的后台上下文
func Background() context.Context {
	return context.Background()
}

// MetaContext 实现
type fbContext struct {
	context.Context
	sync.Mutex
	// 下面的字段在应用到真实系统时需要扩展
	cancelled bool
	syscall   uint32
	name      string
}

func NewContext(ctx context.Context, op uint32, name string) MetaContext {
	return &fbContext{Context: ctx, syscall: op, name: name}
}

func (c *fbContext) Deadline() (deadline time.Time, ok bool) {
	return c.Context.Deadline()
}

func (c *fbContext) Done() <-chan struct{} {
	return c.Context.Done()
}

func (c *fbContext) Err() error {
	if err := c.Context.Err(); err != nil {
		return err
	}
	c.Lock()
	defer c.Unlock()
	if c.cancelled {
		return context.Canceled
	}
	return nil
}

func (c *fbContext) Value(key interface{}) interface{} {
	return c.Context.Value(key)
}

func (c *fbContext) Cancel() {
	c.Lock()
	c.cancelled = true
	c.Unlock()
}

func (c *fbContext) Canceled() bool {
	c.Lock()
	defer c.Unlock()
	return c.cancelled
}

func (c *fbContext) Syscall() (uint32, string) {
	return c.syscall, c.name
}
