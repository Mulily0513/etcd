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
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	pebbledb "github.com/cockroachdb/pebble"

	"go.etcd.io/etcd/server/v3/storage/backend"
)

// batchTx maps one etcd writable transaction to one Pebble IndexedBatch.
// IndexedBatch is important here: etcd may read through BatchTx before the
// transaction is committed, and Pebble already provides that read-your-writes
// view without a second Go-side overlay.
type batchTx struct {
	mu      sync.Mutex
	backend *pebbleBackend
	locked  bool
	batch   *pebbledb.Batch

	// bucketChanges is only metadata needed to keep the in-memory bucket index
	// in sync after the batch commits. The data itself remains in Pebble.
	bucketChanges map[string]bool
}

func (b *pebbleBackend) BatchTx() backend.BatchTx { return b.tx }

func (b *pebbleBackend) SetTxPostLockInsideApplyHook(hook func()) {
	b.hookMu.Lock()
	b.hook = hook
	b.hookMu.Unlock()
}

func (b *pebbleBackend) callHook() {
	b.hookMu.RLock()
	hook := b.hook
	b.hookMu.RUnlock()
	if hook != nil {
		hook()
	}
}

func (b *pebbleBackend) hasPostLockHook() bool {
	b.hookMu.RLock()
	defer b.hookMu.RUnlock()
	return b.hook != nil
}

func (b *pebbleBackend) ForceCommit() { b.tx.Commit() }

func (t *batchTx) lock() {
	// Keep the lifecycle read lock until the transaction is unlocked. Close
	// must not close the DB while this IndexedBatch is being used.
	t.backend.lifecycleMu.RLock()
	t.mu.Lock()
	t.locked = true
	t.batch = t.backend.db.NewIndexedBatch()
	t.bucketChanges = make(map[string]bool)
}

func (t *batchTx) Lock() {
	backend.ValidateCalledInsideUnittest(t.backend.lg)
	t.lock()
}

func (t *batchTx) LockInsideApply() {
	t.lock()
	if t.backend.hasPostLockHook() {
		backend.ValidateCalledInsideApply(t.backend.lg)
		t.backend.callHook()
	}
}

func (t *batchTx) LockOutsideApply() {
	backend.ValidateCalledOutSideApply(t.backend.lg)
	t.lock()
}

func (t *batchTx) Unlock() {
	if !t.locked {
		return
	}
	if err := t.commitLocked(); err != nil {
		panic(err)
	}
	t.finishLocked()
}

func (t *batchTx) Commit() {
	t.backend.lifecycleMu.RLock()
	t.mu.Lock()
	t.locked = true
	if t.batch == nil {
		t.batch = t.backend.db.NewIndexedBatch()
		t.bucketChanges = make(map[string]bool)
	}
	if err := t.commitLocked(); err != nil {
		t.mu.Unlock()
		t.backend.lifecycleMu.RUnlock()
		panic(err)
	}
	t.finishLocked()
}

func (t *batchTx) CommitAndStop() { t.Commit() }

// commitWithLifecycleHeld is used by backend operations that already hold
// lifecycleMu.RLock, such as snapshot and defrag. It avoids taking a nested
// lifecycle read lock while a Close is waiting for the outer operation.
func (t *batchTx) commitWithLifecycleHeld() error {
	t.mu.Lock()
	t.locked = true
	if t.batch == nil {
		t.batch = t.backend.db.NewIndexedBatch()
		t.bucketChanges = make(map[string]bool)
	}
	err := t.commitLocked()
	t.locked = false
	t.mu.Unlock()
	return err
}

func (t *batchTx) finishLocked() {
	if t.batch != nil {
		_ = t.batch.Close()
	}
	t.batch = nil
	t.bucketChanges = nil
	t.locked = false
	t.mu.Unlock()
	t.backend.lifecycleMu.RUnlock()
}

func (t *batchTx) commitLocked() error {
	if t.batch == nil || t.batch.Empty() {
		return nil
	}
	if t.backend.hooks != nil {
		t.backend.hooks.OnPreCommitUnsafe(t)
	}
	if t.batch.Empty() {
		return nil
	}

	writeOptions := pebbledb.Sync
	if t.backend.unsafeNoFsync || t.backend.disableWAL {
		writeOptions = pebbledb.NoSync
	}
	batchSize := t.batch.Len()
	if err := t.batch.Commit(writeOptions); err != nil {
		return err
	}

	t.backend.stateMu.Lock()
	t.backend.memtableSize += uint64(batchSize)
	for name, present := range t.bucketChanges {
		if present {
			t.backend.buckets[name] = struct{}{}
		} else {
			delete(t.backend.buckets, name)
		}
	}
	t.backend.stateMu.Unlock()
	atomic.AddInt64(&t.backend.commits, 1)
	t.backend.refreshEstimatedSizeLocked()

	_ = t.batch.Close()
	t.batch = nil
	t.bucketChanges = nil
	return nil
}

func (t *batchTx) ensureBatch() *pebbledb.Batch {
	if t.batch == nil {
		panic("pebble backend: batch transaction is not locked")
	}
	return t.batch
}

func (t *batchTx) bucketPresent(bucket backend.Bucket) bool {
	value, closer, err := t.ensureBatch().Get(bucketMetaKey(bucket.Name()))
	if err == nil {
		_ = closer.Close()
		return len(value) != 0
	}
	if err == pebbledb.ErrNotFound {
		return false
	}
	panic(err)
}

func (t *batchTx) UnsafeCreateBucket(bucket backend.Bucket) {
	if t.bucketPresent(bucket) {
		return
	}
	if err := t.ensureBatch().Set(bucketMetaKey(bucket.Name()), []byte{1}, nil); err != nil {
		panic(err)
	}
	t.bucketChanges[string(bucket.Name())] = true
}

func (t *batchTx) UnsafeDeleteBucket(bucket backend.Bucket) {
	if !t.bucketPresent(bucket) {
		return
	}
	if err := t.ensureBatch().Delete(bucketMetaKey(bucket.Name()), nil); err != nil {
		panic(err)
	}
	start := dataKey(bucket.Name(), nil)
	if err := t.ensureBatch().DeleteRange(start, prefixEnd(start), nil); err != nil {
		panic(err)
	}
	t.bucketChanges[string(bucket.Name())] = false
}

func (t *batchTx) UnsafePut(bucket backend.Bucket, key, value []byte) {
	t.put(bucket, key, value)
}

func (t *batchTx) UnsafeSeqPut(bucket backend.Bucket, key, value []byte) {
	t.put(bucket, key, value)
}

func (t *batchTx) put(bucket backend.Bucket, key, value []byte) {
	if !t.bucketPresent(bucket) {
		panic(fmt.Sprintf("pebble backend: bucket %q does not exist", bucket.Name()))
	}
	if err := setData(t.ensureBatch(), bucket.Name(), key, value); err != nil {
		panic(err)
	}
}

func (t *batchTx) UnsafeDelete(bucket backend.Bucket, key []byte) {
	if !t.bucketPresent(bucket) {
		panic(fmt.Sprintf("pebble backend: bucket %q does not exist", bucket.Name()))
	}
	if err := deleteData(t.ensureBatch(), bucket.Name(), key); err != nil {
		panic(err)
	}
}

func (t *batchTx) UnsafeRange(bucket backend.Bucket, key, endKey []byte, limit int64) ([][]byte, [][]byte) {
	if t.batch == nil {
		return t.backend.readBucketWithoutRangeSafety(bucket, key, endKey, limit, false)
	}
	return readFromView(t.ensureBatch(), bucket, key, endKey, limit, false, false)
}

func (t *batchTx) UnsafeForEach(bucket backend.Bucket, visitor func(k, v []byte) error) error {
	var keys, values [][]byte
	if t.batch == nil {
		keys, values = t.backend.readBucketWithoutRangeSafety(bucket, nil, nil, math.MaxInt64, true)
	} else {
		keys, values = readFromView(t.ensureBatch(), bucket, nil, nil, math.MaxInt64, true, false)
	}
	for i := range keys {
		if err := visitor(keys[i], values[i]); err != nil {
			return err
		}
	}
	return nil
}
