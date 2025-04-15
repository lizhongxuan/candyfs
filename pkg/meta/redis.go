package meta

import (
	"candyfs/utils/log"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/redis/go-redis/v9"
	"net"
	"net/url"
	"os"
	"strings"
	"time"
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
	if strings.Contains(hosts, ",") && strings.Index(hosts, ",") < strings.Index(hosts, ":") {
		var fopt redis.FailoverOptions
		ps := strings.Split(hosts, ",")
		fopt.MasterName = ps[0]
		fopt.SentinelAddrs = ps[1:]
		_, port, _ := net.SplitHostPort(fopt.SentinelAddrs[len(fopt.SentinelAddrs)-1])
		if port == "" {
			port = "26379"
		}
		for i, addr := range fopt.SentinelAddrs {
			h, p, e := net.SplitHostPort(addr)
			if e != nil {
				fopt.SentinelAddrs[i] = net.JoinHostPort(addr, port)
			} else if p == "" {
				fopt.SentinelAddrs[i] = net.JoinHostPort(h, port)
			}
		}
		fopt.SentinelPassword = os.Getenv("SENTINEL_PASSWORD")
		fopt.DB = opt.DB
		fopt.Username = opt.Username
		fopt.Password = opt.Password
		fopt.TLSConfig = opt.TLSConfig
		fopt.MaxRetries = opt.MaxRetries
		fopt.MinRetryBackoff = opt.MinRetryBackoff
		fopt.MaxRetryBackoff = opt.MaxRetryBackoff
		fopt.ReadTimeout = opt.ReadTimeout
		fopt.WriteTimeout = opt.WriteTimeout
		fopt.PoolSize = opt.PoolSize               // default: GOMAXPROCS * 10
		fopt.PoolTimeout = opt.PoolTimeout         // default: ReadTimeout + 1 second.
		fopt.MinIdleConns = opt.MinIdleConns       // disable by default
		fopt.MaxIdleConns = opt.MaxIdleConns       // disable by default
		fopt.ConnMaxIdleTime = opt.ConnMaxIdleTime // default: 30 minutes
		fopt.ConnMaxLifetime = opt.ConnMaxLifetime // disable by default
		if conf.ReadOnly {
			// NOTE: RouteByLatency and RouteRandomly are not supported since they require cluster client
			fopt.ReplicaOnly = routeRead == "replica"
		}
		rdb = redis.NewFailoverClient(&fopt)
	} else {
		if !strings.Contains(hosts, ",") {
			c := redis.NewClient(opt)
			info, err := c.ClusterInfo(Background()).Result()
			if err != nil && strings.Contains(err.Error(), "cluster mode") || err == nil && strings.Contains(info, "cluster_state:") {
				log.Infof("redis %s is in cluster mode", hosts)
			} else {
				rdb = c
			}
		}
		if rdb == nil {
			var copt redis.ClusterOptions
			copt.Addrs = strings.Split(hosts, ",")
			copt.MaxRedirects = 1
			copt.Username = opt.Username
			copt.Password = opt.Password
			copt.TLSConfig = opt.TLSConfig
			copt.MaxRetries = opt.MaxRetries
			copt.MinRetryBackoff = opt.MinRetryBackoff
			copt.MaxRetryBackoff = opt.MaxRetryBackoff
			copt.ReadTimeout = opt.ReadTimeout
			copt.WriteTimeout = opt.WriteTimeout
			copt.PoolSize = opt.PoolSize               // default: GOMAXPROCS * 10
			copt.PoolTimeout = opt.PoolTimeout         // default: ReadTimeout + 1 second.
			copt.MinIdleConns = opt.MinIdleConns       // disable by default
			copt.MaxIdleConns = opt.MaxIdleConns       // disable by default
			copt.ConnMaxIdleTime = opt.ConnMaxIdleTime // default: 30 minutes
			copt.ConnMaxLifetime = opt.ConnMaxLifetime // disable by default
			if conf.ReadOnly {
				switch routeRead {
				case "random":
					copt.RouteRandomly = true
				case "latency":
					copt.RouteByLatency = true
				case "replica":
					copt.ReadOnly = true
				default:
					// route to primary
				}
			}
			rdb = redis.NewClusterClient(&copt)
			prefix = fmt.Sprintf("{%d}", opt.DB)
		}
	}

	m := &redisMeta{
		baseMeta: newBaseMeta(addr, conf),
		rdb:      rdb,
		prefix:   prefix,
	}
	m.en = m
	m.checkServerConfig()
	return m, nil
}

func (m *redisMeta) Name() string {
	return "redis"
}

func (m *redisMeta) checkServerConfig() {
	rawInfo, err := m.rdb.Info(Background()).Result()
	if err != nil {
		log.Warnf("parse info: %s", err)
		return
	}
	rInfo, err := checkRedisInfo(rawInfo)
	if err != nil {
		log.Warnf("parse info: %s", err)
	}
	if rInfo.storageProvider == "" && rInfo.maxMemoryPolicy != "" && rInfo.maxMemoryPolicy != "noeviction" {
		log.Warnf("maxmemory_policy is %q,  we will try to reconfigure it to 'noeviction'.", rInfo.maxMemoryPolicy)
		if _, err := m.rdb.ConfigSet(Background(), "maxmemory-policy", "noeviction").Result(); err != nil {
			log.Errorf("try to reconfigure maxmemory-policy to 'noeviction' failed: %s", err)
		} else if result, err := m.rdb.ConfigGet(Background(), "maxmemory-policy").Result(); err != nil {
			log.Warnf("get config maxmemory-policy failed: %s", err)
		} else if len(result) == 1 && result["maxmemory-policy"] != "noeviction" {
			log.Warnf("reconfigured maxmemory-policy to 'noeviction', but it's still %s", result["maxmemory-policy"])
		} else {
			log.Infof("set maxmemory-policy to 'noeviction' successfully")
		}
	}
	start := time.Now()
	_, err = m.rdb.Ping(Background()).Result()
	if err != nil {
		log.Errorf("Ping redis: %s", err.Error())
		return
	}
	log.Infof("Ping redis latency: %s", time.Since(start))
}

type redisInfo struct {
	aofEnabled      bool
	maxMemoryPolicy string
	redisVersion    string
	storageProvider string // redis is "", keyDB is "none" or "flash"
}

func checkRedisInfo(rawInfo string) (info redisInfo, err error) {
	lines := strings.Split(strings.TrimSpace(rawInfo), "\n")
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if l == "" || strings.HasPrefix(l, "#") {
			continue
		}
		kvPair := strings.SplitN(l, ":", 2)
		if len(kvPair) < 2 {
			continue
		}
		key, val := kvPair[0], kvPair[1]
		switch key {
		case "aof_enabled":
			info.aofEnabled = val == "1"
			if val == "0" {
				log.Warnf("AOF is not enabled, you may lose data if Redis is not shutdown properly.")
			}
		case "maxmemory_policy":
			info.maxMemoryPolicy = val
		//case "redis_version":
		//	info.redisVersion = val
		//	ver, err := parseRedisVersion(val)
		//	if err != nil {
		//		log.Warnf("Failed to parse Redis server version %q: %s", ver, err)
		//	} else {
		//		if ver.olderThan(oldestSupportedVer) {
		//			log.Fatalf("Redis version should not be older than %s", oldestSupportedVer)
		//		}
		//	}
		case "storage_provider":
			// if storage_provider is none reset it to ""
			if val == "flash" {
				info.storageProvider = val
			}
		}
	}
	return
}
