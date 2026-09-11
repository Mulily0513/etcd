// Copyright 2017 The etcd Authors
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

package storage

import (
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"

	"go.etcd.io/etcd/server/v3/config"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/storage/backend"
	pebblebackend "go.etcd.io/etcd/server/v3/storage/backend/pebble"
	"go.etcd.io/etcd/server/v3/storage/schema"
	"go.etcd.io/raft/v3/raftpb"
)

// LSMFlushable describes an LSM backend that can establish a durable
// Raft-index boundary when its own WAL is disabled.
type LSMFlushable interface {
	LSMFlushIndex() uint64
	FlushTo(uint64) error
}

type backendOpener func(config.ServerConfig, backend.Hooks) backend.Backend

var backendOpeners = map[config.StorageBackend]backendOpener{
	config.StorageBackendBbolt:  openBboltBackend,
	config.StorageBackendPebble: openPebbleBackend,
}

func newBackend(cfg config.ServerConfig, hooks backend.Hooks) backend.Backend {
	opener, ok := backendOpeners[cfg.Backend]
	if !ok {
		panic(fmt.Sprintf("unsupported storage backend %q", cfg.Backend))
	}
	return opener(cfg, hooks)
}

func openBboltBackend(cfg config.ServerConfig, hooks backend.Hooks) backend.Backend {
	bcfg := backend.DefaultBackendConfig(cfg.Logger)
	bcfg.Path = cfg.BackendPath()
	bcfg.UnsafeNoFsync = cfg.UnsafeNoFsync
	if cfg.BackendBatchLimit != 0 {
		bcfg.BatchLimit = cfg.BackendBatchLimit
		if cfg.Logger != nil {
			cfg.Logger.Info("setting backend batch limit", zap.Int("batch limit", cfg.BackendBatchLimit))
		}
	}
	if cfg.BackendBatchInterval != 0 {
		bcfg.BatchInterval = cfg.BackendBatchInterval
		if cfg.Logger != nil {
			cfg.Logger.Info("setting backend batch interval", zap.Duration("batch interval", cfg.BackendBatchInterval))
		}
	}
	bcfg.BackendFreelistType = cfg.BackendFreelistType
	bcfg.Logger = cfg.Logger
	if cfg.QuotaBackendBytes > 0 && cfg.QuotaBackendBytes != DefaultQuotaBytes {
		// permit 10% excess over quota for disarm
		bcfg.MmapSize = uint64(cfg.QuotaBackendBytes + cfg.QuotaBackendBytes/10)
	}
	bcfg.Mlock = cfg.MemoryMlock
	bcfg.Hooks = hooks
	return backend.New(bcfg)
}

func openPebbleBackend(cfg config.ServerConfig, hooks backend.Hooks) backend.Backend {
	batchInterval := cfg.BackendBatchInterval
	if batchInterval == 0 {
		batchInterval = pebblebackend.DefaultBatchInterval
	}
	be, err := pebblebackend.Open(pebblebackend.Config{
		Path:             cfg.BackendPath(),
		BatchInterval:    batchInterval,
		BatchLimit:       cfg.BackendBatchLimit,
		DisableWAL:       true,
		UnsafeNoFsync:    cfg.UnsafeNoFsync,
		LazySnapshotSize: true,
		UseEstimatedSize: true,
		Logger:           cfg.Logger,
		Hooks:            hooks,
	})
	if err != nil {
		if cfg.Logger != nil {
			cfg.Logger.Panic("failed to open Pebble backend", zap.String("path", cfg.BackendPath()), zap.Error(err))
		}
		panic(err)
	}
	return be
}

// OpenSnapshotBackend replaces the current backend with the database from a
// Raft snapshot. The second return value is the previous backend when it can
// retain the original asynchronous close behavior; it is nil when the
// replacement path had to close it before opening the new backend.
func OpenSnapshotBackend(cfg config.ServerConfig, oldbe backend.Backend, ss *snap.Snapshotter, snapshot *raftpb.Snapshot, hooks *BackendHooks) (backend.Backend, backend.Backend, error) {
	snapPath, err := ss.DBFilePath(snapshot.Metadata.GetIndex())
	if err != nil {
		return nil, oldbe, fmt.Errorf("failed to find database snapshot file (%w)", err)
	}
	switch cfg.Backend {
	case config.StorageBackendBbolt:
		return openBboltSnapshotBackend(cfg, oldbe, snapPath, hooks)
	case config.StorageBackendPebble:
		return openPebbleSnapshotBackend(cfg, oldbe, snapPath, hooks)
	default:
		return nil, oldbe, fmt.Errorf("unsupported storage backend %q", cfg.Backend)
	}
}

func openBboltSnapshotBackend(cfg config.ServerConfig, oldbe backend.Backend, snapPath string, hooks *BackendHooks) (backend.Backend, backend.Backend, error) {
	if err := os.Rename(snapPath, cfg.BackendPath()); err != nil {
		return nil, oldbe, fmt.Errorf("failed to rename database snapshot file (%w)", err)
	}
	return OpenBackend(cfg, hooks), oldbe, nil
}

func openPebbleSnapshotBackend(cfg config.ServerConfig, oldbe backend.Backend, snapPath string, hooks *BackendHooks) (backend.Backend, backend.Backend, error) {
	if oldbe != nil {
		// Keep the live Pebble instance. Replacing its contents in one native
		// batch preserves existing read snapshots and avoids closing the DB
		// while an in-flight request still owns a read transaction.
		// The live etcd Pebble backend has WAL disabled. The replacement
		// helper performs an explicit Flush after the batch commit, so the
		// commit itself must use Pebble's NoSync option.
		if err := pebblebackend.RestoreSnapshotIntoBackend(oldbe, snapPath, false); err != nil {
			return nil, oldbe, fmt.Errorf("failed to restore Pebble database snapshot (%w)", err)
		}
		if err := os.Remove(snapPath); err != nil && !os.IsNotExist(err) {
			return nil, oldbe, fmt.Errorf("failed to remove Pebble database snapshot (%w)", err)
		}
		return oldbe, nil, nil
	}
	// Startup recovery has no live backend to preserve. Restore the generic
	// snapshot stream into the configured Pebble directory.
	oldPath := cfg.BackendPath()
	oldPathBackup := oldPath + ".old"
	if _, statErr := os.Stat(oldPath); statErr == nil {
		_ = os.RemoveAll(oldPathBackup)
		if err := os.Rename(oldPath, oldPathBackup); err != nil {
			return nil, oldbe, fmt.Errorf("failed to move old Pebble backend (%w)", err)
		}
	}
	if err := pebblebackend.RestoreSnapshot(snapPath, oldPath, true); err != nil {
		return nil, oldbe, fmt.Errorf("failed to restore Pebble database snapshot (%w)", err)
	}
	if err := os.Remove(snapPath); err != nil && !os.IsNotExist(err) {
		return nil, oldbe, fmt.Errorf("failed to remove Pebble database snapshot (%w)", err)
	}
	return OpenBackend(cfg, hooks), nil, nil
}

// OpenBackend returns a backend using the current etcd db.
func OpenBackend(cfg config.ServerConfig, hooks backend.Hooks) backend.Backend {
	fn := cfg.BackendPath()

	now, beOpened := time.Now(), make(chan backend.Backend)
	go func() {
		beOpened <- newBackend(cfg, hooks)
	}()

	defer func() {
		cfg.Logger.Info("opened backend db", zap.String("path", fn), zap.Duration("took", time.Since(now)))
	}()

	select {
	case be := <-beOpened:
		return be

	case <-time.After(10 * time.Second):
		cfg.Logger.Info(
			"db file is flocked by another process, or taking too long",
			zap.String("path", fn),
			zap.Duration("took", time.Since(now)),
		)
	}

	return <-beOpened
}

// RecoverSnapshotBackend recovers the DB from a snapshot in case etcd crashes
// before updating the backend db after persisting raft snapshot to disk,
// violating the invariant snapshot.Metadata.Index < db.consistentIndex. In this
// case, replace the db with the snapshot db sent by the leader.
func RecoverSnapshotBackend(cfg config.ServerConfig, oldbe backend.Backend, snapshot *raftpb.Snapshot, beExist bool, hooks *BackendHooks) (backend.Backend, error) {
	consistentIndex := uint64(0)
	if beExist {
		consistentIndex, _ = schema.ReadConsistentIndex(oldbe.ReadTx())
	}
	if snapshot.Metadata.GetIndex() <= consistentIndex {
		cfg.Logger.Info("Skipping snapshot backend", zap.Uint64("consistent-index", consistentIndex), zap.Uint64("snapshot-index", snapshot.Metadata.GetIndex()))
		return oldbe, nil
	}
	cfg.Logger.Info("Recovering from snapshot backend", zap.Uint64("consistent-index", consistentIndex), zap.Uint64("snapshot-index", snapshot.Metadata.GetIndex()))
	oldbe.Close()
	newbe, _, err := OpenSnapshotBackend(cfg, nil, snap.New(cfg.Logger, cfg.SnapDir()), snapshot, hooks)
	return newbe, err
}
