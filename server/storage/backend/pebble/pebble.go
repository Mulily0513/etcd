// Copyright 2026 The etcd Authors
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

// Package pebble adapts Pebble's atomic batches and snapshots to etcd's
// backend.Backend contract. Pebble remains an implementation detail: callers
// continue to use etcd's Bucket, ReadTx and BatchTx interfaces.
package pebble

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	pebbledb "github.com/cockroachdb/pebble"
	"go.uber.org/zap"

	"go.etcd.io/etcd/server/v3/storage/backend"
)

var (
	dataPrefix       = []byte{0x01}
	bucketPrefix     = []byte{0x00, 'r', 'l', 'k', 'v', '/', 'b', 'u', 'c', 'k', 'e', 't', '/'}
	lsmFlushIndexKey = []byte{0x00, 'r', 'l', 'k', 'v', '/', 'm', 'e', 't', 'a', '/', 'l', 's', 'm', '_', 'f', 'l', 'u', 's', 'h', '_', 'i', 'n', 'd', 'e', 'x'}
)

// DefaultBatchInterval matches bbolt's default backend batch interval so
// backend comparisons do not change two batching variables at once.
const DefaultBatchInterval = 100 * time.Millisecond

// Config controls a Pebble backend.
type Config struct {
	Path string
	// BatchInterval and BatchLimit are retained for the shared backend opener.
	// Pebble commits at etcd transaction boundaries and does not reproduce
	// bbolt's periodic write transaction.
	BatchInterval time.Duration
	BatchLimit    int
	DisableWAL    bool
	UnsafeNoFsync bool
	// LazySnapshotSize avoids walking a Pebble snapshot just to calculate its
	// stream size at creation time. The WAL-off backend uses this for local
	// snapshots, where the size is not needed; Size still computes the exact
	// value on demand for remote snapshot transfer and maintenance APIs.
	LazySnapshotSize bool
	// UseEstimatedSize uses Pebble's in-memory disk-usage metrics instead of
	// walking the database directory on every quota check. The WAL-off backend
	// enables this for its own path; keeping it opt-in preserves the existing
	// Pebble baseline measurement.
	UseEstimatedSize bool
	Logger           *zap.Logger
	Hooks            backend.Hooks
}

// Open opens a Pebble-backed implementation of backend.Backend.
//
// Path is a directory. It intentionally has the same lifetime as the etcd
// backend: callers must close the returned Backend before removing or
// replacing the directory.
func Open(cfg Config) (backend.Backend, error) {
	if cfg.Path == "" {
		return nil, errors.New("pebble backend: empty path")
	}
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}

	db, err := pebbledb.Open(cfg.Path, &pebbledb.Options{
		DisableWAL: cfg.DisableWAL,
		Logger:     pebbleLogger{lg: cfg.Logger},
		// etcd's BackendConfig.UnsafeNoFsync has the same intent as Pebble's
		// NoSync write option. It is applied when batches are committed below.
		FS: nil,
	})
	if err != nil {
		return nil, err
	}

	b := &pebbleBackend{
		db:               db,
		path:             cfg.Path,
		disableWAL:       cfg.DisableWAL,
		unsafeNoFsync:    cfg.UnsafeNoFsync,
		lazySnapshotSize: cfg.LazySnapshotSize,
		useEstimatedSize: cfg.UseEstimatedSize,
		lg:               cfg.Logger,
		hooks:            cfg.Hooks,
		buckets:          make(map[string]struct{}),
	}
	if err := b.loadBuckets(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := b.loadLSMFlushIndex(); err != nil {
		_ = db.Close()
		return nil, err
	}
	b.refreshEstimatedSize()
	b.tx = &batchTx{backend: b}
	b.readTx = &readTx{backend: b}
	return b, nil
}

// pebbleLogger routes Pebble's internal diagnostics through etcd's logger.
// Pebble's default logger writes directly to stderr, which would bypass etcd's
// structured logging and can corrupt tools that consume the process logs.
type pebbleLogger struct {
	lg *zap.Logger
}

func (l pebbleLogger) Infof(format string, args ...interface{}) {
	l.lg.Debug(fmt.Sprintf(format, args...))
}

func (l pebbleLogger) Fatalf(format string, args ...interface{}) {
	l.lg.Fatal(fmt.Sprintf(format, args...))
}

type pebbleBackend struct {
	db   *pebbledb.DB
	path string

	disableWAL       bool
	unsafeNoFsync    bool
	lazySnapshotSize bool
	useEstimatedSize bool
	estimatedSize    int64
	memtableSize     uint64
	lg               *zap.Logger

	// lifecycleMu prevents Close from racing with a Pebble DB operation. Read
	// and write transactions hold its read side for their lifetime, just as a
	// bbolt transaction keeps the backend alive until it is released.
	lifecycleMu sync.RWMutex
	// stateMu protects the committed bucket index and estimated memtable size.
	stateMu sync.RWMutex

	buckets       map[string]struct{}
	tx            *batchTx
	readTx        *readTx
	lsmFlushIndex uint64

	readOpen int64
	commits  int64

	hookMu sync.RWMutex
	hook   func()
	hooks  backend.Hooks
}

// LSMFlushIndex returns the last Raft index included in a completed Pebble
// flush. The value is loaded from Pebble on startup and only advances after
// FlushTo has completed successfully.
func (b *pebbleBackend) LSMFlushIndex() uint64 {
	return atomic.LoadUint64(&b.lsmFlushIndex)
}

// FlushTo commits pending backend state together with the LSM flush-index
// marker and then flushes the resulting memtables to stable Pebble storage.
// The marker is written before db.Flush, but is published to readers only
// after db.Flush succeeds.
func (b *pebbleBackend) FlushTo(index uint64) error {
	current := b.LSMFlushIndex()
	if index < current {
		return fmt.Errorf("pebble backend: LSM flush index moved backwards from %d to %d", current, index)
	}

	b.lifecycleMu.RLock()
	defer b.lifecycleMu.RUnlock()
	b.tx.mu.Lock()
	b.tx.locked = true
	if b.tx.batch == nil {
		b.tx.batch = b.db.NewIndexedBatch()
		b.tx.bucketChanges = make(map[string]bool)
	}
	defer func() {
		b.tx.locked = false
		b.tx.mu.Unlock()
	}()

	encoded := make([]byte, 8)
	binary.BigEndian.PutUint64(encoded, index)
	if err := b.tx.batch.Set(lsmFlushIndexKey, encoded, nil); err != nil {
		return err
	}
	if err := b.tx.commitLocked(); err != nil {
		return err
	}
	if err := b.db.Flush(); err != nil {
		return err
	}
	b.stateMu.Lock()
	b.memtableSize = 0
	b.stateMu.Unlock()
	atomic.StoreUint64(&b.lsmFlushIndex, index)
	b.refreshEstimatedSizeLocked()
	return nil
}

func (b *pebbleBackend) Close() error {
	b.lifecycleMu.Lock()
	defer b.lifecycleMu.Unlock()
	b.tx.mu.Lock()
	if b.tx.batch != nil {
		_ = b.tx.batch.Close()
		b.tx.batch = nil
	}
	b.tx.mu.Unlock()
	if err := b.db.Flush(); err != nil {
		return err
	}
	return b.db.Close()
}

func (b *pebbleBackend) loadLSMFlushIndex() error {
	atomic.StoreUint64(&b.lsmFlushIndex, 0)
	value, closer, err := b.db.Get(lsmFlushIndexKey)
	if errors.Is(err, pebbledb.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	defer closer.Close()
	if len(value) != 8 {
		return fmt.Errorf("pebble backend: invalid LSM flush index length %d", len(value))
	}
	atomic.StoreUint64(&b.lsmFlushIndex, binary.BigEndian.Uint64(value))
	return nil
}

func (b *pebbleBackend) loadBuckets() error {
	b.lifecycleMu.RLock()
	defer b.lifecycleMu.RUnlock()
	it, err := b.db.NewIter(&pebbledb.IterOptions{LowerBound: bucketPrefix, UpperBound: prefixEnd(bucketPrefix)})
	if err != nil {
		return err
	}
	for ok := it.First(); ok; ok = it.Next() {
		key := it.Key()
		name := key[len(bucketPrefix):]
		b.buckets[string(name)] = struct{}{}
	}
	return it.Close()
}

func (b *pebbleBackend) readBucketWithoutRangeSafety(bucket backend.Bucket, key, endKey []byte, limit int64, all bool) ([][]byte, [][]byte) {
	b.lifecycleMu.RLock()
	defer b.lifecycleMu.RUnlock()
	snapshot := b.db.NewSnapshot()
	defer snapshot.Close()
	return readFromView(snapshot, bucket, key, endKey, limit, all, false)
}

var _ backend.Backend = (*pebbleBackend)(nil)
var _ backend.BatchTx = (*batchTx)(nil)
var _ backend.ReadTx = (*readTx)(nil)
