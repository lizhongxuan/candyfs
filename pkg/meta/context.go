package meta

import (
	"context"
	"time"
)

type CtxKey string

// Context is a wrapper around context.Context with more features.
type Context interface {
	context.Context
	Gid() uint32
	Gids() []uint32
	Uid() uint32
	Pid() uint32
	WithValue(k, v interface{}) Context // should remain const semantics, so user can chain it
	Cancel()
	Canceled() bool
	CheckPermission() bool
}

// Background returns a non-nil, empty Context.
func Background() Context {
	return WrapContext(context.Background())
}

// WithTimeout returns a copy of parent context with timeout.
func WithTimeout(parent Context, timeout time.Duration) (Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(parent, timeout)
	return WrapContext(ctx), cancel
}

// WithValue returns a copy of parent in which the value associated with key is val.
func WithValue(parent Context, key, val interface{}) Context {
	return parent.WithValue(key, val)
}

type wrapContext struct {
	context.Context
	cancel func()
	pid    uint32
	uid    uint32
	gids   []uint32
}

func (c *wrapContext) Uid() uint32 {
	return c.uid
}

func (c *wrapContext) Gid() uint32 {
	return c.gids[0]
}

func (c *wrapContext) Gids() []uint32 {
	return c.gids
}

func (c *wrapContext) Pid() uint32 {
	return c.pid
}

func (c *wrapContext) Cancel() {
	c.cancel()
}

func (c *wrapContext) Canceled() bool {
	return c.Err() != nil
}

func (c *wrapContext) WithValue(k, v interface{}) Context {
	wc := *c // gids is a const, so it's safe to shallow copy
	wc.Context = context.WithValue(c.Context, k, v)
	return &wc
}

func (c *wrapContext) CheckPermission() bool {
	return true
}

func NewContext(pid, uid uint32, gids []uint32) Context {
	return wrap(context.Background(), pid, uid, gids)
}

func WrapContext(ctx context.Context) Context {
	return wrap(ctx, 0, 0, []uint32{0})
}

func wrap(ctx context.Context, pid, uid uint32, gids []uint32) Context {
	c, cancel := context.WithCancel(ctx)
	return &wrapContext{c, cancel, pid, uid, gids}
}

func containsGid(ctx Context, gid uint32) bool {
	for _, g := range ctx.Gids() {
		if g == gid {
			return true
		}
	}
	return false
}
