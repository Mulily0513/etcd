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
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sync"

	pebbledb "github.com/cockroachdb/pebble"

	"go.etcd.io/etcd/server/v3/storage/backend"
)

type snapshot struct {
	db       *pebbledb.Snapshot
	size     int64
	lazySize bool
	sizeOnce sync.Once
}

func (b *pebbleBackend) Snapshot() backend.Snapshot {
	b.lifecycleMu.RLock()
	if err := b.tx.commitWithLifecycleHeld(); err != nil {
		b.lifecycleMu.RUnlock()
		panic(err)
	}
	dbSnapshot := b.db.NewSnapshot()
	b.lifecycleMu.RUnlock()
	s := &snapshot{db: dbSnapshot, lazySize: b.lazySnapshotSize}
	if !s.lazySize {
		s.size = snapshotStreamSize(dbSnapshot)
	}
	return s
}

func (s *snapshot) Size() int64 {
	if s.lazySize {
		s.sizeOnce.Do(func() {
			s.size = snapshotStreamSize(s.db)
		})
	}
	return s.size
}

// WriteTo emits a portable key/value stream. This is intentionally separate
// from Pebble's on-disk checkpoint format; a future snapshotter can package
// this stream into a checkpoint without exposing Pebble internals to etcd.
func (s *snapshot) WriteTo(w io.Writer) (int64, error) {
	it, err := s.db.NewIter(nil)
	if err != nil {
		return 0, err
	}
	defer it.Close()
	var n int64
	for ok := it.First(); ok; ok = it.Next() {
		k := it.Key()
		v := it.Value()
		if err := writeFrame(w, k, v); err != nil {
			return n, err
		}
		n += int64(8 + len(k) + len(v))
	}
	if err := it.Error(); err != nil {
		return n, err
	}
	padding := paddedSnapshotSize(n) - n
	if padding > 0 {
		var zeros [512]byte
		for padding > 0 {
			chunk := padding
			if chunk > int64(len(zeros)) {
				chunk = int64(len(zeros))
			}
			if _, err := w.Write(zeros[:chunk]); err != nil {
				return n, err
			}
			n += chunk
			padding -= chunk
		}
	}
	return n, nil
}

func (s *snapshot) Close() error { return s.db.Close() }

// snapshotStreamSize returns the exact size of the portable stream emitted by
// WriteTo. backend.Snapshot.Size is used by rafthttp to frame the streamed
// database snapshot, so it must describe the wire representation rather than
// Pebble's on-disk directory size.
func snapshotStreamSize(db *pebbledb.Snapshot) int64 {
	it, err := db.NewIter(nil)
	if err != nil {
		return 0
	}
	defer it.Close()

	var size int64
	for ok := it.First(); ok; ok = it.Next() {
		size += int64(8 + len(it.Key()) + len(it.Value()))
	}
	if it.Error() != nil {
		return 0
	}
	return paddedSnapshotSize(size)
}

// RestoreSnapshot restores a snapshot produced by Snapshot.WriteTo into a new
// Pebble directory. The caller must close/remove any previous database at
// destination before calling this function.
func RestoreSnapshot(snapshotPath, destination string, syncWrite bool) error {
	if err := os.MkdirAll(destination, 0o700); err != nil {
		return err
	}
	// A synced restore must be able to commit through Pebble's normal WAL;
	// the newly opened runtime backend may disable that WAL again after the
	// restore is complete.
	db, err := pebbledb.Open(destination, &pebbledb.Options{DisableWAL: !syncWrite})
	if err != nil {
		return err
	}
	defer db.Close()
	return restoreSnapshotStream(db, snapshotPath, syncWrite, false)
}

// RestoreSnapshotIntoBackend replaces the contents of a live Pebble backend
// without replacing its DB handle. Existing read snapshots therefore remain
// valid while the Raft snapshot is applied.
func RestoreSnapshotIntoBackend(be backend.Backend, snapshotPath string, syncWrite bool) error {
	b, ok := be.(*pebbleBackend)
	if !ok {
		return errors.New("pebble backend: backend is not a Pebble backend")
	}

	b.lifecycleMu.RLock()
	defer b.lifecycleMu.RUnlock()
	b.tx.mu.Lock()
	b.tx.locked = true
	defer func() {
		b.tx.locked = false
		b.tx.mu.Unlock()
	}()
	if b.tx.batch != nil {
		_ = b.tx.batch.Close()
		b.tx.batch = nil
	}
	b.tx.bucketChanges = nil
	if err := restoreSnapshotStream(b.db, snapshotPath, syncWrite, true); err != nil {
		return err
	}
	if err := b.db.Flush(); err != nil {
		return err
	}

	buckets := make(map[string]struct{})
	it, err := b.db.NewIter(&pebbledb.IterOptions{LowerBound: bucketPrefix, UpperBound: prefixEnd(bucketPrefix)})
	if err != nil {
		return err
	}
	for ok := it.First(); ok; ok = it.Next() {
		buckets[string(it.Key()[len(bucketPrefix):])] = struct{}{}
	}
	if err := it.Close(); err != nil {
		return err
	}

	b.stateMu.Lock()
	b.buckets = buckets
	b.memtableSize = 0
	b.stateMu.Unlock()
	return b.loadLSMFlushIndex()
}

func restoreSnapshotStream(db *pebbledb.DB, snapshotPath string, syncWrite, replace bool) error {
	f, err := os.Open(snapshotPath)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	streamSize := info.Size()
	// etcdctl appends the SHA-256 digest to a snapshot stream. The backend
	// snapshot used by the raft transport does not contain that digest, so
	// identify and exclude it by the format used by the snapshot protocol.
	if streamSize >= sha256.Size && streamSize%512 == sha256.Size {
		streamSize -= sha256.Size
	}
	limited := &io.LimitedReader{R: f, N: streamSize}

	batch := db.NewBatch()
	defer batch.Close()
	if replace {
		it, err := db.NewIter(nil)
		if err != nil {
			return err
		}
		for ok := it.First(); ok; ok = it.Next() {
			if err := batch.Delete(clone(it.Key()), nil); err != nil {
				_ = it.Close()
				return err
			}
		}
		if err := it.Close(); err != nil {
			return err
		}
	}
	var header [8]byte
	for limited.N > 0 {
		if limited.N < int64(len(header)) {
			if err := consumeZeroPadding(limited); err != nil {
				return err
			}
			break
		}
		_, err := io.ReadFull(limited, header[:])
		if err != nil {
			return fmt.Errorf("pebble backend: read snapshot header: %w", err)
		}
		keyLen := binary.BigEndian.Uint32(header[:4])
		valueLen := binary.BigEndian.Uint32(header[4:])
		if keyLen == 0 && valueLen == 0 {
			if err := consumeZeroPadding(limited); err != nil {
				return err
			}
			break
		}
		if keyLen > math.MaxInt32 || valueLen > math.MaxInt32 {
			return errors.New("pebble backend: snapshot frame too large")
		}
		if uint64(keyLen)+uint64(valueLen) > uint64(limited.N) {
			return errors.New("pebble backend: truncated snapshot frame")
		}
		key := make([]byte, keyLen)
		value := make([]byte, valueLen)
		if _, err = io.ReadFull(limited, key); err != nil {
			return fmt.Errorf("pebble backend: read snapshot key: %w", err)
		}
		if _, err = io.ReadFull(limited, value); err != nil {
			return fmt.Errorf("pebble backend: read snapshot value: %w", err)
		}
		if err = batch.Set(key, value, nil); err != nil {
			return err
		}
	}
	writeOptions := pebbledb.Sync
	if !syncWrite {
		writeOptions = pebbledb.NoSync
	}
	return batch.Commit(writeOptions)
}

func paddedSnapshotSize(size int64) int64 {
	const blockSize int64 = 512
	if size == 0 {
		return 0
	}
	return ((size + blockSize - 1) / blockSize) * blockSize
}

func consumeZeroPadding(r *io.LimitedReader) error {
	var buf [512]byte
	for r.N > 0 {
		chunk := r.N
		if chunk > int64(len(buf)) {
			chunk = int64(len(buf))
		}
		if _, err := io.ReadFull(r, buf[:chunk]); err != nil {
			return fmt.Errorf("pebble backend: read snapshot padding: %w", err)
		}
		if !bytes.Equal(buf[:chunk], make([]byte, chunk)) {
			return errors.New("pebble backend: non-zero snapshot padding")
		}
	}
	return nil
}

func writeFrame(w io.Writer, key, value []byte) error {
	var header [8]byte
	binary.BigEndian.PutUint32(header[:4], uint32(len(key)))
	binary.BigEndian.PutUint32(header[4:], uint32(len(value)))
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	if _, err := w.Write(key); err != nil {
		return err
	}
	_, err := w.Write(value)
	return err
}
