// Copyright 2021 The etcd Authors
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

package betesting

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"

	"go.etcd.io/etcd/server/v3/config"
	"go.etcd.io/etcd/server/v3/storage/backend"
	pebblebackend "go.etcd.io/etcd/server/v3/storage/backend/pebble"
)

func NewTmpBackendFromCfg(tb testing.TB, bcfg backend.BackendConfig) (backend.Backend, string) {
	dir, err := os.MkdirTemp(tb.TempDir(), "etcd_backend_test")
	if err != nil {
		panic(err)
	}
	tmpPath := filepath.Join(dir, "database")
	bcfg.Path = tmpPath
	bcfg.Logger = zaptest.NewLogger(tb)
	return openBackend(tb, tmpPath, bcfg), tmpPath
}

// NewTmpBackend creates a backend implementation for testing.
func NewTmpBackend(tb testing.TB, batchInterval time.Duration, batchLimit int) (backend.Backend, string) {
	bcfg := backend.DefaultBackendConfig(zaptest.NewLogger(tb))
	bcfg.BatchInterval, bcfg.BatchLimit = batchInterval, batchLimit
	return NewTmpBackendFromCfg(tb, bcfg)
}

func NewDefaultTmpBackend(tb testing.TB) (backend.Backend, string) {
	return NewTmpBackendFromCfg(tb, backend.DefaultBackendConfig(zaptest.NewLogger(tb)))
}

// OpenBackendAtPath opens the backend selected by ETCD_TEST_STORAGE_BACKEND.
// It is used by tests that close and reopen the same backend path.
func OpenBackendAtPath(tb testing.TB, path string) backend.Backend {
	tb.Helper()
	bcfg := backend.DefaultBackendConfig(zaptest.NewLogger(tb))
	bcfg.Path = path
	return openBackend(tb, path, bcfg)
}

// ConfiguredBackend is the backend selected for backend-aware unit tests.

func ConfiguredBackend(tb testing.TB) config.StorageBackend {
	tb.Helper()
	configured := config.StorageBackend(os.Getenv("ETCD_TEST_STORAGE_BACKEND"))
	if configured == "" {
		return config.StorageBackendBbolt
	}
	if _, ok := backendOpeners[configured]; !ok {
		tb.Fatalf("unsupported ETCD_TEST_STORAGE_BACKEND=%q", configured)
	}
	return configured
}

type backendOpener func(testing.TB, backend.BackendConfig) backend.Backend

var backendOpeners = map[config.StorageBackend]backendOpener{
	config.StorageBackendBbolt: func(_ testing.TB, bcfg backend.BackendConfig) backend.Backend {
		return backend.New(bcfg)
	},
	config.StorageBackendPebble: func(tb testing.TB, bcfg backend.BackendConfig) backend.Backend {
		be, err := pebblebackend.Open(pebblebackend.Config{
			Path:          bcfg.Path,
			BatchInterval: bcfg.BatchInterval,
			BatchLimit:    bcfg.BatchLimit,
			UnsafeNoFsync: bcfg.UnsafeNoFsync,
			Logger:        bcfg.Logger,
			Hooks:         bcfg.Hooks,
		})
		if err != nil {
			tb.Fatalf("failed to open Pebble test backend: %v", err)
		}
		return be
	},
}

func openBackend(tb testing.TB, path string, bcfg backend.BackendConfig) backend.Backend {
	bcfg.Path = path
	return backendOpeners[ConfiguredBackend(tb)](tb, bcfg)
}

// RequireBbolt skips tests that exercise bbolt-only file or batching details.
func RequireBbolt(tb testing.TB) {
	tb.Helper()
	if ConfiguredBackend(tb) != config.StorageBackendBbolt {
		tb.Skip("test requires the bbolt backend")
	}
}

func Close(tb testing.TB, b backend.Backend) {
	assert.NoError(tb, b.Close())
}
