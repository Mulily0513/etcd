// Copyright 2026 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

package pebble

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"go.etcd.io/etcd/server/v3/storage/backend"
	"go.etcd.io/etcd/server/v3/storage/schema"
)

func TestBackendRoundTripAndReadYourWrites(t *testing.T) {
	path := t.TempDir() + "/db"
	b, err := Open(Config{Path: path, BatchInterval: time.Hour, BatchLimit: 100, Logger: zap.NewNop()})
	require.NoError(t, err)

	tx := b.BatchTx()
	tx.LockOutsideApply()
	tx.UnsafeCreateBucket(schema.Key)
	tx.UnsafePut(schema.Key, []byte("b"), []byte("2"))
	tx.UnsafePut(schema.Key, []byte("a"), []byte("1"))
	keys, values := tx.UnsafeRange(schema.Key, []byte("a"), []byte("c"), 0)
	require.Equal(t, [][]byte{[]byte("a"), []byte("b")}, keys)
	require.Equal(t, [][]byte{[]byte("1"), []byte("2")}, values)
	tx.Unlock()

	read := b.ReadTx()
	read.RLock()
	keys, values = read.UnsafeRange(schema.Key, []byte("a"), []byte("c"), 0)
	read.RUnlock()
	require.Equal(t, [][]byte{[]byte("a"), []byte("b")}, keys)
	require.Equal(t, [][]byte{[]byte("1"), []byte("2")}, values)

	b.ForceCommit()
	require.NoError(t, b.Close())

	b, err = Open(Config{Path: path, BatchInterval: time.Hour, BatchLimit: 100, Logger: zap.NewNop()})
	require.NoError(t, err)
	defer b.Close()
	read = b.ConcurrentReadTx()
	keys, values = read.UnsafeRange(schema.Key, []byte("a"), []byte("c"), 0)
	read.RUnlock()
	require.Equal(t, [][]byte{[]byte("a"), []byte("b")}, keys)
	require.Equal(t, [][]byte{[]byte("1"), []byte("2")}, values)
}

func TestReadTxWithoutLockDoesNotBlockClose(t *testing.T) {
	b, err := Open(Config{Path: t.TempDir() + "/db", BatchInterval: time.Hour, Logger: zap.NewNop()})
	require.NoError(t, err)

	tx := b.BatchTx()
	tx.LockOutsideApply()
	tx.UnsafeCreateBucket(schema.Key)
	tx.UnsafePut(schema.Key, []byte("key"), []byte("value"))
	tx.Unlock()

	var seen int
	err = b.ReadTx().UnsafeForEach(schema.Key, func(_, _ []byte) error {
		seen++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, seen)
	require.NoError(t, b.Close())
}

func TestReadTxSnapshotLifetime(t *testing.T) {
	b, err := Open(Config{Path: t.TempDir() + "/db", BatchInterval: time.Hour, Logger: zap.NewNop()})
	require.NoError(t, err)

	tx := b.BatchTx()
	tx.LockOutsideApply()
	tx.UnsafeCreateBucket(schema.Key)
	tx.UnsafePut(schema.Key, []byte("key"), []byte("value"))
	tx.Unlock()

	rtx := b.ReadTx()
	rtx.RLock()
	rtx.RLock()
	require.Equal(t, int64(2), b.OpenReadTxN())

	rtx.RUnlock()
	require.Equal(t, int64(1), b.OpenReadTxN())
	keys, values := rtx.UnsafeRange(schema.Key, []byte("key"), nil, 0)
	require.Equal(t, [][]byte{[]byte("key")}, keys)
	require.Equal(t, [][]byte{[]byte("value")}, values)

	rtx.RUnlock()
	require.Equal(t, int64(0), b.OpenReadTxN())
	require.NoError(t, b.Close())
}

func TestBatchAndReadTransactionsSeeCompactionMarkers(t *testing.T) {
	b, err := Open(Config{Path: t.TempDir() + "/db", BatchInterval: time.Hour, Logger: zap.NewNop()})
	require.NoError(t, err)
	defer b.Close()

	tx := b.BatchTx()
	tx.LockOutsideApply()
	tx.UnsafeCreateBucket(schema.Meta)
	tx.UnsafePut(schema.Meta, schema.ScheduledCompactKeyName, []byte("scheduled"))
	tx.Unlock()
	b.ForceCommit()

	tx.LockOutsideApply()
	tx.UnsafePut(schema.Meta, schema.FinishedCompactKeyName, []byte("finished"))
	var writeKeys, writeValues [][]byte
	require.NoError(t, tx.UnsafeForEach(schema.Meta, func(k, v []byte) error {
		writeKeys = append(writeKeys, k)
		writeValues = append(writeValues, v)
		return nil
	}))
	tx.Unlock()

	rtx := b.ReadTx()
	rtx.RLock()
	var readKeys, readValues [][]byte
	require.NoError(t, rtx.UnsafeForEach(schema.Meta, func(k, v []byte) error {
		readKeys = append(readKeys, k)
		readValues = append(readValues, v)
		return nil
	}))
	rtx.RUnlock()
	require.Equal(t, writeKeys, readKeys)
	require.Equal(t, writeValues, readValues)
}

func TestBatchReadUsesCommittedAndPendingOperations(t *testing.T) {
	b, err := Open(Config{Path: t.TempDir() + "/db", BatchInterval: time.Hour, Logger: zap.NewNop()})
	require.NoError(t, err)
	defer b.Close()

	tx := b.BatchTx()
	tx.LockOutsideApply()
	tx.UnsafeCreateBucket(schema.Key)
	tx.UnsafePut(schema.Key, []byte("a"), []byte("old"))
	tx.UnsafePut(schema.Key, []byte("b"), []byte("value"))
	tx.UnsafePut(schema.Key, []byte("a"), []byte("new"))
	tx.UnsafeDelete(schema.Key, []byte("b"))
	keys, values := tx.UnsafeRange(schema.Key, []byte("a"), []byte("c"), 0)
	tx.Unlock()

	require.Equal(t, [][]byte{[]byte("a")}, keys)
	require.Equal(t, [][]byte{[]byte("new")}, values)
	b.ForceCommit()

	read := b.ConcurrentReadTx()
	keys, values = read.UnsafeRange(schema.Key, []byte("a"), []byte("c"), 0)
	read.RUnlock()
	require.Equal(t, [][]byte{[]byte("a")}, keys)
	require.Equal(t, [][]byte{[]byte("new")}, values)
}

func TestBackendDeleteAndSnapshot(t *testing.T) {
	b, err := Open(Config{Path: t.TempDir() + "/db", BatchInterval: time.Hour, Logger: zap.NewNop()})
	require.NoError(t, err)
	defer b.Close()

	tx := b.BatchTx()
	tx.LockOutsideApply()
	tx.UnsafeCreateBucket(schema.Key)
	tx.UnsafePut(schema.Key, []byte("a"), []byte("1"))
	tx.UnsafePut(schema.Key, []byte("b"), []byte("2"))
	tx.Unlock()
	b.ForceCommit()

	snap := b.Snapshot()
	var out bytes.Buffer
	_, err = snap.WriteTo(&out)
	require.NoError(t, err)
	require.Equal(t, int64(out.Len()), snap.Size())
	require.NoError(t, snap.Close())
	require.NotEmpty(t, out.Bytes())

	tx.LockOutsideApply()
	tx.UnsafeDelete(schema.Key, []byte("a"))
	tx.Unlock()
	b.ForceCommit()
	read := b.ConcurrentReadTx()
	keys, _ := read.UnsafeRange(schema.Key, []byte("a"), []byte("c"), 0)
	read.RUnlock()
	require.Equal(t, [][]byte{[]byte("b")}, keys)
}

func TestDefragPreservesData(t *testing.T) {
	b, err := Open(Config{Path: t.TempDir() + "/db", BatchInterval: time.Hour, Logger: zap.NewNop()})
	require.NoError(t, err)
	defer b.Close()

	tx := b.BatchTx()
	tx.LockOutsideApply()
	tx.UnsafeCreateBucket(schema.Key)
	for i := 0; i < 32; i++ {
		tx.UnsafePut(schema.Key, []byte{byte(i)}, []byte("value"))
	}
	tx.Unlock()

	wantHash, err := b.Hash(nil)
	require.NoError(t, err)
	require.NoError(t, b.Defrag())

	gotHash, err := b.Hash(nil)
	require.NoError(t, err)
	require.Equal(t, wantHash, gotHash)
}

func TestSnapshotRestore(t *testing.T) {
	b, err := Open(Config{Path: t.TempDir() + "/db", BatchInterval: time.Hour, Logger: zap.NewNop()})
	require.NoError(t, err)
	defer b.Close()
	tx := b.BatchTx()
	tx.LockOutsideApply()
	tx.UnsafeCreateBucket(schema.Key)
	tx.UnsafePut(schema.Key, []byte("k"), []byte("v"))
	tx.Unlock()
	b.ForceCommit()
	s := b.Snapshot()
	defer s.Close()
	path := t.TempDir() + "/snapshot"
	f, err := os.Create(path)
	require.NoError(t, err)
	_, err = s.WriteTo(f)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	restorePath := t.TempDir() + "/restored"
	require.NoError(t, RestoreSnapshot(path, restorePath, true))
	restored, err := Open(Config{Path: restorePath, BatchInterval: time.Hour, Logger: zap.NewNop()})
	require.NoError(t, err)
	defer restored.Close()
	read := restored.ConcurrentReadTx()
	keys, values := read.UnsafeRange(schema.Key, []byte("k"), nil, 0)
	read.RUnlock()
	require.Equal(t, [][]byte{[]byte("k")}, keys)
	require.Equal(t, [][]byte{[]byte("v")}, values)
}

func TestLSMFlushIndexRoundTrip(t *testing.T) {
	path := t.TempDir() + "/db"
	open := func() backend.Backend {
		b, err := Open(Config{
			Path:          path,
			BatchInterval: time.Hour,
			DisableWAL:    true,
			Logger:        zap.NewNop(),
		})
		require.NoError(t, err)
		return b
	}

	b := open()
	tx := b.BatchTx()
	tx.LockOutsideApply()
	tx.UnsafeCreateBucket(schema.Key)
	tx.UnsafePut(schema.Key, []byte("k"), []byte("v"))
	tx.Unlock()

	flushable, ok := b.(interface {
		LSMFlushIndex() uint64
		FlushTo(uint64) error
	})
	require.True(t, ok)
	require.NoError(t, flushable.FlushTo(42))
	require.Equal(t, uint64(42), flushable.LSMFlushIndex())
	require.NoError(t, b.Close())

	b = open()
	defer b.Close()
	flushable, ok = b.(interface {
		LSMFlushIndex() uint64
		FlushTo(uint64) error
	})
	require.True(t, ok)
	require.Equal(t, uint64(42), flushable.LSMFlushIndex())
	read := b.ConcurrentReadTx()
	keys, values := read.UnsafeRange(schema.Key, []byte("k"), nil, 0)
	read.RUnlock()
	require.Equal(t, [][]byte{[]byte("k")}, keys)
	require.Equal(t, [][]byte{[]byte("v")}, values)
}
