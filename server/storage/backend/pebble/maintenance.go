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

package pebble

import (
	"encoding/binary"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"

	"go.etcd.io/etcd/server/v3/storage/backend"
)

func (b *pebbleBackend) Hash(ignores func(bucketName, keyName []byte) bool) (uint32, error) {
	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	tx := b.ConcurrentReadTx()
	defer tx.RUnlock()
	b.stateMu.RLock()
	buckets := cloneBuckets(b.buckets)
	b.stateMu.RUnlock()
	names := make([]string, 0, len(buckets))
	for name := range buckets {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		bucketName := []byte(name)
		h.Write(bucketName)
		keys, values := tx.UnsafeRange(bucketByName(bucketName), nil, nil, math.MaxInt64)
		for i, key := range keys {
			if ignores != nil && !ignores(bucketName, key) {
				continue
			}
			h.Write(key)
			h.Write(values[i])
		}
	}
	return h.Sum32(), nil
}

func (b *pebbleBackend) Size() int64 {
	if b.useEstimatedSize {
		return atomic.LoadInt64(&b.estimatedSize)
	}

	var size int64
	if info, err := os.Stat(b.path); err == nil && info.IsDir() {
		_ = walkSize(b.path, &size)
	}
	return size
}

// refreshEstimatedSize updates the WAL-off quota view outside the
// request-level Size call. Pebble's metrics are cheap compared with a full
// directory walk, but still take internal locks; doing this at backend commit
// and checkpoint boundaries keeps quota checks on the hot path lock-free.
func (b *pebbleBackend) refreshEstimatedSize() {
	if !b.useEstimatedSize {
		return
	}
	b.lifecycleMu.RLock()
	b.refreshEstimatedSizeLocked()
	b.lifecycleMu.RUnlock()
}

// refreshEstimatedSizeLocked is the lifecycle-lock-held form used by Flush.
func (b *pebbleBackend) refreshEstimatedSizeLocked() {
	if !b.useEstimatedSize {
		return
	}
	metrics := b.db.Metrics()
	usage := uint64(0)
	for i := range metrics.Levels {
		usage += uint64(metrics.Levels[i].Size) + metrics.Levels[i].VirtualSize
	}
	b.stateMu.RLock()
	usage += b.memtableSize
	b.stateMu.RUnlock()
	if usage > math.MaxInt64 {
		usage = math.MaxInt64
	}
	atomic.StoreInt64(&b.estimatedSize, int64(usage))
}

func (b *pebbleBackend) SizeInUse() int64   { return b.Size() }
func (b *pebbleBackend) OpenReadTxN() int64 { return atomic.LoadInt64(&b.readOpen) }
func (b *pebbleBackend) Commits() int64     { return atomic.LoadInt64(&b.commits) }

func (b *pebbleBackend) Defrag() error {
	b.lifecycleMu.RLock()
	defer b.lifecycleMu.RUnlock()
	if err := b.tx.commitWithLifecycleHeld(); err != nil {
		return err
	}
	if err := b.db.Flush(); err != nil {
		return err
	}
	// MVCC compaction removes obsolete revisions logically. Compact the LSM
	// range as part of Defrag so SizeInUse reflects the reclaimed space, just
	// as bbolt defragmentation does.
	if err := b.db.Compact(nil, []byte{0xff}, true); err != nil {
		return err
	}
	b.stateMu.Lock()
	b.memtableSize = 0
	b.stateMu.Unlock()
	b.refreshEstimatedSizeLocked()
	return nil
}

// bucketByName is used by Hash, which only needs a stable backend.Bucket
// descriptor and does not need schema-specific bucket IDs.
type bucketByNameType struct{ name []byte }

func bucketByName(name []byte) backend.Bucket { return bucketByNameType{name: clone(name)} }
func (b bucketByNameType) ID() backend.BucketID {
	return backend.BucketID(binary.BigEndian.Uint32(paddedID(b.name)))
}
func (b bucketByNameType) Name() []byte            { return b.name }
func (b bucketByNameType) String() string          { return string(b.name) }
func (b bucketByNameType) IsSafeRangeBucket() bool { return true }

func paddedID(name []byte) []byte {
	var id [4]byte
	for i := range name {
		id[i%len(id)] ^= name[i]
	}
	return id[:]
}

func walkSize(path string, size *int64) error {
	return filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			*size += info.Size()
		}
		return nil
	})
}
