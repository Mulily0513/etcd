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
	"errors"
	"io"
	"math"
	"sync"
	"sync/atomic"

	pebbledb "github.com/cockroachdb/pebble"

	"go.etcd.io/etcd/server/v3/storage/backend"
)

type pebbleReader interface {
	Get(key []byte) ([]byte, io.Closer, error)
	NewIter(*pebbledb.IterOptions) (*pebbledb.Iterator, error)
}

type readTx struct {
	backend *pebbleBackend

	// The regular ReadTx is shared by callers. Keep one snapshot while at
	// least one caller holds RLock, just like bbolt's shared read transaction,
	// and close it only after the last reader releases the lock.
	snapshotMu sync.Mutex
	snapshot   *pebbledb.Snapshot
	readers    int

	concurrent      bool
	lifecycleLocked bool
}

// ReadTx returns the stable reader used by the non-concurrent backend path,
// just like bbolt. The main request path uses ConcurrentReadTx, which returns
// an independent Pebble snapshot.
func (b *pebbleBackend) ReadTx() backend.ReadTx { return b.readTx }

func (b *pebbleBackend) ConcurrentReadTx() backend.ReadTx {
	b.lifecycleMu.RLock()
	r := &readTx{
		backend:         b,
		snapshot:        b.db.NewSnapshot(),
		concurrent:      true,
		lifecycleLocked: true,
	}
	atomic.AddInt64(&b.readOpen, 1)
	return r
}

func (r *readTx) RLock() {
	if r.concurrent {
		return
	}
	r.backend.lifecycleMu.RLock()
	r.snapshotMu.Lock()
	if r.snapshot == nil {
		r.snapshot = r.backend.db.NewSnapshot()
	}
	r.readers++
	r.snapshotMu.Unlock()
	atomic.AddInt64(&r.backend.readOpen, 1)
}

func (r *readTx) RUnlock() {
	if r.concurrent {
		if r.snapshot != nil {
			_ = r.snapshot.Close()
			r.snapshot = nil
			atomic.AddInt64(&r.backend.readOpen, -1)
		}
		if r.lifecycleLocked {
			r.backend.lifecycleMu.RUnlock()
			r.lifecycleLocked = false
		}
		return
	}
	r.snapshotMu.Lock()
	r.readers--
	if r.readers == 0 {
		_ = r.snapshot.Close()
		r.snapshot = nil
	}
	r.snapshotMu.Unlock()
	atomic.AddInt64(&r.backend.readOpen, -1)
	r.backend.lifecycleMu.RUnlock()
}

func (r *readTx) view() pebbleReader {
	r.snapshotMu.Lock()
	snapshot := r.snapshot
	r.snapshotMu.Unlock()
	if snapshot != nil {
		return snapshot
	}
	return r.backend.db
}

func (r *readTx) UnsafeRange(bucket backend.Bucket, key, endKey []byte, limit int64) ([][]byte, [][]byte) {
	return r.read(bucket, key, endKey, limit, false)
}

func (r *readTx) UnsafeForEach(bucket backend.Bucket, visitor func(k, v []byte) error) error {
	keys, values := r.read(bucket, nil, nil, math.MaxInt64, true)
	for i := range keys {
		if err := visitor(keys[i], values[i]); err != nil {
			return err
		}
	}
	return nil
}

func (r *readTx) read(bucket backend.Bucket, key, endKey []byte, limit int64, all bool) ([][]byte, [][]byte) {
	return readFromView(r.view(), bucket, key, endKey, limit, all, true)
}

func readFromView(view pebbleReader, bucket backend.Bucket, key, endKey []byte, limit int64, all, enforceSafeRange bool) ([][]byte, [][]byte) {
	if !bucketExists(view, bucket.Name()) {
		return nil, nil
	}
	if !all && endKey == nil {
		limit = 1
	}
	if limit <= 0 {
		limit = math.MaxInt64
	}
	if enforceSafeRange && !all && limit > 1 && !bucket.IsSafeRangeBucket() {
		panic("do not use unsafeRange on non-keys bucket")
	}

	lower := dataKey(bucket.Name(), key)
	if !all && endKey == nil {
		value, closer, err := view.Get(lower)
		if errors.Is(err, pebbledb.ErrNotFound) {
			return nil, nil
		}
		if err != nil {
			panic(err)
		}
		defer closer.Close()
		return [][]byte{clone(key)}, [][]byte{clone(value)}
	}

	upper := prefixEnd(lowerPrefix(bucket.Name()))
	if endKey != nil {
		upper = dataKey(bucket.Name(), endKey)
	}
	it, err := view.NewIter(&pebbledb.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		panic(err)
	}
	defer it.Close()

	prefixLen := len(lowerPrefix(bucket.Name()))
	var keys [][]byte
	var values [][]byte
	for ok := it.First(); ok && int64(len(keys)) < limit; ok = it.Next() {
		keys = append(keys, clone(it.Key()[prefixLen:]))
		values = append(values, clone(it.Value()))
	}
	if err := it.Error(); err != nil {
		panic(err)
	}
	return keys, values
}

func bucketExists(view pebbleReader, name []byte) bool {
	value, closer, err := view.Get(bucketMetaKey(name))
	if err == nil {
		_ = closer.Close()
		return len(value) != 0
	}
	if errors.Is(err, pebbledb.ErrNotFound) {
		return false
	}
	panic(err)
}
