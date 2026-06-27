// Package config centralizes the tunable surface (spec §15) shared by the
// worker, generator, and (later) coordinator. Everything is overridable by
// flag or environment variable so the benchmark harness can vary it.
package config

import (
	"os"
	"strconv"
)

// Env reads an environment variable with a fallback default.
func Env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

// EnvInt reads an integer environment variable with a fallback default.
func EnvInt(key string, def int) int {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// Defaults captured in one place so flag definitions and docker-compose env
// stay in sync.
const (
	DefaultKafkaBrokers = "localhost:19092"
	DefaultTopic        = "events"
	DefaultS3Endpoint   = "http://localhost:9000"
	DefaultS3Bucket     = "streamforge"
	DefaultS3Region     = "us-east-1"
	DefaultS3AccessKey  = "minioadmin"
	DefaultS3SecretKey  = "minioadmin"

	DefaultWindowSizeMS = 10_000 // 10s tumbling windows
	DefaultBuckets      = 64     // hash buckets for keyBy
)
