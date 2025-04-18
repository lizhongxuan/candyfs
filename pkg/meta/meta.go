package meta

import (
	"candyfs/utils/log"
	"strings"
)

// Meta is a interface for a meta service for file system.
type Meta interface {
	// Name of database
	Name() string
	
	// Format initializes the metadata storage
	Format() error
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
