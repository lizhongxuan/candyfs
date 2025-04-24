package meta

import (
	"candyfs/utils/log"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type redisMeta struct {
	*baseMeta
	rdb        redis.UniversalClient
	prefix     string
	shaLookup  string // The SHA returned by Redis for the loaded `scriptLookup`
	shaResolve string // The SHA returned by Redis for the loaded `scriptResolve`
}

var _ Meta = &redisMeta{}

// newRedisMeta return a meta store using Redis.
func newRedisMeta(addr string, conf *Config) (Meta, error) {
	uri := "redis://" + addr
	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("url parse %s: %s", uri, err)
	}
	values := u.Query()
	query := queryMap{&values}
	minRetryBackoff := query.duration("min-retry-backoff", "min_retry_backoff", time.Millisecond*20)
	maxRetryBackoff := query.duration("max-retry-backoff", "max_retry_backoff", time.Second*10)
	readTimeout := query.duration("read-timeout", "read_timeout", time.Second*30)
	writeTimeout := query.duration("write-timeout", "write_timeout", time.Second*5)
	routeRead := query.pop("route-read")
	skipVerify := query.pop("insecure-skip-verify")
	certFile := query.pop("tls-cert-file")
	keyFile := query.pop("tls-key-file")
	caCertFile := query.pop("tls-ca-cert-file")
	tlsServerName := query.pop("tls-server-name")
	u.RawQuery = values.Encode()

	hosts := u.Host
	opt, err := redis.ParseURL(u.String())
	if err != nil {
		return nil, fmt.Errorf("redis parse %s: %s", uri, err)
	}
	if opt.TLSConfig != nil {
		opt.TLSConfig.ServerName = tlsServerName // use the host of each connection as ServerName
		opt.TLSConfig.InsecureSkipVerify = skipVerify != ""
		if certFile != "" {
			cert, err := tls.LoadX509KeyPair(certFile, keyFile)
			if err != nil {
				return nil, fmt.Errorf("get certificate error certFile:%s keyFile:%s error:%s", certFile, keyFile, err)
			}
			opt.TLSConfig.Certificates = []tls.Certificate{cert}
		}
		if caCertFile != "" {
			caCert, err := os.ReadFile(caCertFile)
			if err != nil {
				return nil, fmt.Errorf("read ca cert file error path:%s error:%s", caCertFile, err)
			}
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)
			opt.TLSConfig.RootCAs = caCertPool
		}
	}
	if opt.Password == "" {
		opt.Password = os.Getenv("REDIS_PASSWORD")
	}
	if opt.Password == "" {
		opt.Password = os.Getenv("META_PASSWORD")
	}
	opt.MaxRetries = conf.Retries
	if opt.MaxRetries == 0 {
		opt.MaxRetries = -1 // Redis use -1 to disable retries
	}
	opt.MinRetryBackoff = minRetryBackoff
	opt.MaxRetryBackoff = maxRetryBackoff
	opt.ReadTimeout = readTimeout
	opt.WriteTimeout = writeTimeout
	var rdb redis.UniversalClient
	var prefix string

	// 创建Redis客户端
	if routeRead != "" {
		var addrs []string
		for _, s := range strings.Split(hosts, ",") {
			s = strings.TrimSpace(s)
			if s == "" {
				continue
			}
			if p := strings.Index(s, "://"); p > 0 {
				opt2, err := redis.ParseURL(u.Scheme + "://" + s)
				if err != nil {
					log.Warnf("parse %s: %s", s, err)
					addrs = append(addrs, s)
					continue
				}
				opt2.Password = opt.Password
				if hosts == s {
					rdb, prefix = newRedisClient(opt2, conf)
				}
				addrs = append(addrs, opt2.Addr)
			} else {
				if strings.HasPrefix(s, ":") { // if it's only a port
					host, _, _ := net.SplitHostPort(opt.Addr)
					s = host + s
				}
				if hosts == s {
					optx := *opt
					optx.Addr = s
					rdb, prefix = newRedisClient(&optx, conf)
				}
				addrs = append(addrs, s)
			}
		}
		if len(addrs) > 1 && rdb == nil {
			log.Warnf("missing master redis, only configure %s as read route", addrs[0])
			optx := *opt
			optx.Addr = addrs[0]
			rdb, prefix = newRedisClient(&optx, conf)
		}
	} else if strings.Contains(hosts, ",") {
		addrs := strings.Split(hosts, ",")
		for i, s := range addrs {
			if p := strings.Index(s, "://"); p > 0 {
				opt2, err := redis.ParseURL(u.Scheme + "://" + s)
				if err != nil {
					log.Warnf("parse %s: %s", s, err)
					continue
				}
				opt2.Password = opt.Password
				addrs[i] = opt2.Addr
			} else if strings.HasPrefix(s, ":") { // if it's only a port
				host, _, _ := net.SplitHostPort(opt.Addr)
				addrs[i] = host + s
			} else {
				addrs[i] = s
			}
		}
		opt := &redis.ClusterOptions{
			Addrs:           addrs,
			Password:        opt.Password,
			MaxRetries:      opt.MaxRetries,
			MinRetryBackoff: opt.MinRetryBackoff,
			MaxRetryBackoff: opt.MaxRetryBackoff,
			ReadTimeout:     opt.ReadTimeout,
			WriteTimeout:    opt.WriteTimeout,
			TLSConfig:       opt.TLSConfig,
		}
		c := redis.NewClusterClient(opt)
		rdb = c
	} else {
		rdb, prefix = newRedisClient(opt, conf)
	}

	m := &redisMeta{
		baseMeta: newBaseMeta(addr, conf),
		rdb:      rdb,
		prefix:   prefix,
	}
	return m, nil
}

func newRedisClient(opt *redis.Options, conf *Config) (redis.UniversalClient, string) {
	var rdb redis.UniversalClient
	var prefix string
	if strings.HasSuffix(opt.Addr, ":26379") {
		// sentinel
		addr := opt.Addr[:len(opt.Addr)-6] + ":6379"
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			log.Errorf("parsing addr %s: %s", addr, err)
			host = "mymaster"
		}
		sopt := &redis.FailoverOptions{
			MasterName:      host,
			SentinelAddrs:   []string{opt.Addr},
			Password:        opt.Password,
			DB:              opt.DB,
			MaxRetries:      opt.MaxRetries,
			MinRetryBackoff: opt.MinRetryBackoff,
			MaxRetryBackoff: opt.MaxRetryBackoff,
			ReadTimeout:     opt.ReadTimeout,
			WriteTimeout:    opt.WriteTimeout,
			TLSConfig:       opt.TLSConfig,
		}
		log.Infof("sentinel master name: %s, address: %s", host, opt.Addr)
		rdb = redis.NewFailoverClient(sopt)
	} else {
		rdb = redis.NewClient(opt)
	}
	return rdb, prefix
}

// Name implements the Meta interface
func (r *redisMeta) Name() string {
	return "redis"
}

// Format implements the Meta interface
func (r *redisMeta) Format() error {
	// 实现Format方法
	return nil
}

// GetAttr 获取文件属性
func (r *redisMeta) GetAttr(ctx context.Context, inode uint64) (*Attr, error) {
	// 实现GetAttr方法，这里只是一个简单的示例
	return &Attr{
		Inode:  inode,
		Size:   0,
		Mode:   0644,
		Mtime:  time.Now().UnixNano(),
		Atime:  time.Now().UnixNano(),
		Ctime:  time.Now().UnixNano(),
		IsDir:  false,
		Blocks: 0,
	}, nil
}

// Lookup 查找文件或目录
func (r *redisMeta) Lookup(ctx context.Context, parent uint64, name string) (*Entry, error) {
	// 实现Lookup方法，这里只是一个简单的示例
	return &Entry{
		Inode:    1000,
		Name:     name,
		Size:     0,
		Mode:     0644,
		Mtime:    time.Now().UnixNano(),
		Atime:    time.Now().UnixNano(),
		Ctime:    time.Now().UnixNano(),
		IsDir:    false,
		Blocks:   0,
		ParentID: parent,
	}, nil
}

// ReadDir 读取目录内容
func (r *redisMeta) ReadDir(ctx context.Context, inode uint64) ([]*Entry, error) {
	// 实现ReadDir方法，这里只是一个简单的示例
	entries := make([]*Entry, 0)
	entries = append(entries, &Entry{
		Inode:    1001,
		Name:     "file1",
		Size:     0,
		Mode:     0644,
		Mtime:    time.Now().UnixNano(),
		Atime:    time.Now().UnixNano(),
		Ctime:    time.Now().UnixNano(),
		IsDir:    false,
		Blocks:   0,
		ParentID: inode,
	})
	return entries, nil
}

// Create 创建文件
func (r *redisMeta) Create(ctx context.Context, parent uint64, name string, mode uint32) (*Entry, error) {
	// 实现Create方法，这里只是一个简单的示例
	return &Entry{
		Inode:    2000,
		Name:     name,
		Size:     0,
		Mode:     mode,
		Mtime:    time.Now().UnixNano(),
		Atime:    time.Now().UnixNano(),
		Ctime:    time.Now().UnixNano(),
		IsDir:    false,
		Blocks:   0,
		ParentID: parent,
	}, nil
}

// MkDir 创建目录
func (r *redisMeta) MkDir(ctx context.Context, parent uint64, name string, mode uint32) (*Entry, error) {
	// 实现MkDir方法，这里只是一个简单的示例
	return &Entry{
		Inode:    3000,
		Name:     name,
		Size:     0,
		Mode:     mode | 0040000, // 目录标志
		Mtime:    time.Now().UnixNano(),
		Atime:    time.Now().UnixNano(),
		Ctime:    time.Now().UnixNano(),
		IsDir:    true,
		Blocks:   0,
		ParentID: parent,
	}, nil
}

// Delete 删除文件或目录
func (r *redisMeta) Delete(ctx context.Context, parent uint64, name string) error {
	// 实现Delete方法，这里只是一个简单的示例
	return nil
}

// Rename 重命名文件或目录
func (r *redisMeta) Rename(ctx context.Context, oldParent, newParent uint64, oldName, newName string) error {
	// 实现Rename方法，这里只是一个简单的示例
	return nil
}

// Read 读取文件内容
func (r *redisMeta) Read(ctx context.Context, inode uint64, buf []byte, offset int64) (int, error) {
	// 实现Read方法，这里只是一个简单的示例
	return 0, nil
}

// Write 写入文件内容
func (r *redisMeta) Write(ctx context.Context, inode uint64, data []byte, offset int64) (int, error) {
	// 实现Write方法，这里只是一个简单的示例
	return len(data), nil
}

// Truncate 调整文件大小
func (r *redisMeta) Truncate(ctx context.Context, inode uint64, size int64) error {
	// 实现Truncate方法，这里只是一个简单的示例
	return nil
}

// Flush 刷新文件
func (r *redisMeta) Flush(ctx context.Context, inode uint64) error {
	// 实现Flush方法，这里只是一个简单的示例
	return nil
}

// Close 关闭元数据服务
func (r *redisMeta) Close() error {
	// 实现Close方法，这里只是一个简单的示例
	return r.rdb.Close()
}
