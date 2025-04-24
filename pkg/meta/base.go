package meta

import (
	"candyfs/utils"
	"context"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

type baseMeta struct {
	sync.Mutex
	addr string
	conf *Config
	//fmt  *Format
	//
	//root         Ino
	//txlocks      [nlocks]sync.Mutex // Pessimistic locks to reduce conflict
	//subTrash     internalNode
	sid uint64
	//of           *openfiles
	//removedFiles map[Ino]bool
	compacting  map[uint64]bool
	maxDeleting chan struct{}
	//dslices      chan Slice // slices to delete
	//symlinks     *symlinkCache
	//msgCallbacks *msgCallbacks
	//reloadCb     []func(*Format)
	umounting bool
	sesMu     sync.Mutex
	//aclCache     aclAPI.Cache

	sessCtx  context.Context
	sessWG   sync.WaitGroup
	dSliceWG sync.WaitGroup

	dirStatsLock sync.Mutex
	//dirStats     map[Ino]dirStat

	fsStatsLock sync.Mutex
	//*fsStat

	parentMu sync.Mutex   // protect dirParents
	quotaMu  sync.RWMutex // protect dirQuotas
	//dirParents map[Ino]Ino    // directory inode -> parent inode
	//dirQuotas  map[Ino]*Quota // directory inode -> quota

	freeMu sync.Mutex
	//freeInodes       freeID
	//freeSlices       freeID
	prefetchMu sync.Mutex
	//prefetchedInodes freeID

	usedSpaceG   prometheus.Gauge
	usedInodesG  prometheus.Gauge
	totalSpaceG  prometheus.Gauge
	totalInodesG prometheus.Gauge
	txDist       prometheus.Histogram
	txRestart    *prometheus.CounterVec
	opDist       prometheus.Histogram
	opCount      *prometheus.CounterVec
	opDuration   *prometheus.CounterVec

	//en engine
}

func newBaseMeta(addr string, conf *Config) *baseMeta {
	return &baseMeta{
		addr: utils.RemovePassword(addr),
		conf: conf,
		sid:  conf.Sid,
		//root:         RootInode,
		//of:           newOpenFiles(conf.OpenCache, conf.OpenCacheLimit),
		//removedFiles: make(map[Ino]bool),
		compacting:  make(map[uint64]bool),
		maxDeleting: make(chan struct{}, 100),
		//symlinks:     newSymlinkCache(maxSymCacheNum),
		//fsStat:       new(fsStat),
		//dirStats:     make(map[Ino]dirStat),
		//dirParents:   make(map[Ino]Ino),
		//dirQuotas:    make(map[Ino]*Quota),
		//msgCallbacks: &msgCallbacks{
		//	callbacks: make(map[uint32]MsgCallback),
		//},
		//aclCache: aclAPI.NewCache(),

		usedSpaceG: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "used_space",
			Help: "Total used space in bytes.",
		}),
		usedInodesG: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "used_inodes",
			Help: "Total used number of inodes.",
		}),
		totalSpaceG: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "total_space",
			Help: "Total space in bytes.",
		}),
		totalInodesG: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "total_inodes",
			Help: "Total number of inodes.",
		}),
		txDist: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "transaction_durations_histogram_seconds",
			Help:    "Transactions latency distributions.",
			Buckets: prometheus.ExponentialBuckets(0.0001, 1.5, 30),
		}),
		txRestart: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "transaction_restart",
			Help: "The number of times a transaction is restarted.",
		}, []string{"method"}),
		opDist: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "meta_ops_durations_histogram_seconds",
			Help:    "Operation latency distributions.",
			Buckets: prometheus.ExponentialBuckets(0.0001, 1.5, 30),
		}),
		opCount: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "meta_ops_total",
			Help: "Meta operation count",
		}, []string{"method"}),
		opDuration: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "meta_ops_duration_seconds",
			Help: "Meta operation duration in seconds.",
		}, []string{"method"}),
	}
}
