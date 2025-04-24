package meta

import "errors"

// 错误常量定义
var (
	ErrNoSuchFileOrDir = errors.New("no such file or directory")
	ErrIsDir           = errors.New("is a directory")
	ErrNotDir          = errors.New("not a directory")
	ErrFileExists      = errors.New("file exists")
	ErrNotEmpty        = errors.New("directory not empty")
	ErrAccessDenied    = errors.New("permission denied")
	ErrInvalidArgs     = errors.New("invalid arguments")
	ErrNotSupported    = errors.New("operation not supported")
	ErrTooManyLinks    = errors.New("too many links")
	ErrIO              = errors.New("I/O error")
	ErrNoSpace         = errors.New("no space left on device")
) 