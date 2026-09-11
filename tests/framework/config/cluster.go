// Copyright 2022 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"os"
	"time"

	serverconfig "go.etcd.io/etcd/server/v3/config"
)

type TLSConfig string

const (
	NoTLS     TLSConfig = ""
	AutoTLS   TLSConfig = "auto-tls"
	ManualTLS TLSConfig = "manual-tls"

	TickDuration = 10 * time.Millisecond
)

type ClusterConfig struct {
	ClusterSize         int
	PeerTLS             TLSConfig
	ClientTLS           TLSConfig
	QuotaBackendBytes   int64
	StrictReconfigCheck bool
	AuthToken           string
	SnapshotCount       uint64

	// ClusterContext is used by "e2e" or "integration" to extend the
	// ClusterConfig. The common test cases shouldn't care about what
	// data is encoded or included; instead "e2e" or "integration"
	// framework should decode or parse it separately.
	ClusterContext any
}

// ConfiguredStorageBackend returns the backend selected for the test suite.
// It uses the environment override when present and otherwise keeps the
// historical bbolt default.
func ConfiguredStorageBackend() serverconfig.StorageBackend {
	if configured := os.Getenv("ETCD_TEST_STORAGE_BACKEND"); configured != "" {
		return serverconfig.StorageBackend(configured)
	}
	return serverconfig.StorageBackendBbolt
}

func DefaultClusterConfig() ClusterConfig {
	return ClusterConfig{
		ClusterSize:         3,
		StrictReconfigCheck: true,
	}
}

func NewClusterConfig(opts ...ClusterOption) ClusterConfig {
	c := DefaultClusterConfig()
	for _, opt := range opts {
		opt(&c)
	}
	return c
}

type ClusterOption func(*ClusterConfig)

func WithClusterConfig(cfg ClusterConfig) ClusterOption {
	return func(c *ClusterConfig) { *c = cfg }
}

func WithClusterSize(size int) ClusterOption {
	return func(c *ClusterConfig) { c.ClusterSize = size }
}

func WithPeerTLS(tls TLSConfig) ClusterOption {
	return func(c *ClusterConfig) { c.PeerTLS = tls }
}

func WithClientTLS(tls TLSConfig) ClusterOption {
	return func(c *ClusterConfig) { c.ClientTLS = tls }
}

func WithQuotaBackendBytes(bytes int64) ClusterOption {
	return func(c *ClusterConfig) { c.QuotaBackendBytes = bytes }
}

func WithSnapshotCount(count uint64) ClusterOption {
	return func(c *ClusterConfig) { c.SnapshotCount = count }
}

func WithStrictReconfigCheck(strict bool) ClusterOption {
	return func(c *ClusterConfig) { c.StrictReconfigCheck = strict }
}
