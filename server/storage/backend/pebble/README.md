# Pebble Backend

This package adapts Pebble to etcd's existing backend contracts.

The V1 design has two non-negotiable constraints:

1. The existing etcd `Backend`, `BatchTx`, and `ReadTx` interfaces are not
   changed.
2. Pebble's WAL-free recovery contract must remain valid: the persisted
   consistent index must never advance beyond the Pebble state that can be
   recovered after a crash. State that remains only in a lost Pebble memtable
   must be replayable from the Raft WAL.

The adapter implements etcd's storage semantics while using Pebble's native
batches, snapshots, memtables, and compaction. It does not reproduce bbolt's
page allocator, freelist, mmap, or periodic write-transaction machinery.

## V1 transaction model

One etcd backend write transaction maps to one Pebble indexed batch:

```text
LockInsideApply / LockOutsideApply
        |
        +-- create one IndexedBatch
        |
        +-- Put / Delete / Range / ForEach
        |       (reads include this batch's mutations)
        |
        +-- Unlock
                |
                +-- OnPreCommitUnsafe
                |
                +-- IndexedBatch.Commit(NoSync)
                |
                +-- Pebble memtable
                |
                +-- close the batch and release the writer lock
```

`IndexedBatch` is required because etcd can read through `BatchTx` before the
transaction is committed. It provides read-your-own-write behavior without a
second Go-side pending-write overlay.

The important visibility relationship is:

```text
storeTxnWrite.End()
    -> current revision is advanced while the MVCC lock is held
    -> BatchTx.Unlock() publishes the Pebble batch
    -> the MVCC lock is released
    -> later readers can observe the new revision and backend state together
```

The backend transaction is therefore an atomic publish boundary. It is not a
long-lived Pebble batch shared by multiple etcd transactions.

## Key layout

The adapter stores logical etcd buckets in one Pebble keyspace.

```text
bucket marker:
    bucketPrefix + bucket name

data key:
    dataPrefix + uint32(bucket-name-length) + bucket name + 0x00 + logical key
```

The bucket marker is the persistent existence record. Data reads are bounded
by the encoded bucket prefix, so a read never crosses into another logical
bucket. Hashing and snapshot streams operate on logical bucket names and keys,
not on Pebble's physical internal keys.

## Backend interface contract

The following table defines the V1 behavior. A method may be a lightweight
adapter or an intentional no-op only when that does not change etcd-visible
semantics.

| etcd API | Pebble V1 behavior | Apply / Flush | Notes |
| --- | --- | --- | --- |
| `Backend.BatchTx()` | Return the shared writer controller | Neither | Does not begin a transaction by itself |
| `BatchTx.LockInsideApply()` | Lock the writer, create an `IndexedBatch`, then call the apply hook | Neither | Used by the Raft apply path |
| `BatchTx.LockOutsideApply()` | Lock the writer and create an `IndexedBatch` | Neither | Does not call the apply hook |
| `BatchTx.Lock()` | Same storage behavior as `LockOutsideApply` | Neither | Kept for unit-test compatibility |
| `UnsafePut` | Set the encoded Pebble key in the current batch | No | Mutation remains local to this transaction |
| `UnsafeSeqPut` | Same as `UnsafePut` | No | bbolt's `FillPercent` has no Pebble equivalent |
| `UnsafeDelete` | Delete the encoded Pebble key in the current batch | No | No special commit path |
| `UnsafeRange` | Read through the current indexed batch | No | Includes committed DB state and pending mutations |
| `UnsafeForEach` | Iterate only the current logical bucket | No | Includes pending mutations |
| `UnsafeCreateBucket` | Set the persistent bucket marker | No | Marker is part of the same atomic batch |
| `UnsafeDeleteBucket` | Remove the marker and delete the bucket's data keys | No | V1 prioritizes correctness over a bbolt-style fast path |
| `BatchTx.Unlock()` | Run the pre-commit hook, commit the batch with `NoSync`, close it, and release the writer | **Apply** | The normal and only write publish point |
| `BatchTx.Commit()` | Ensure the current writer is complete | No extra flush | Does not create a bbolt-style physical transaction |
| `BatchTx.CommitAndStop()` | Complete the current writer lifecycle | No extra flush | There is no periodic Pebble writer to stop |
| `Backend.ForceCommit()` | No-op when no active batch remains | No | Must not turn bbolt commit cadence into Pebble flush cadence |
| `Backend.ReadTx()` | Return the blocking read wrapper | No | A read lock protects the lifetime of its stable view |
| `Backend.ConcurrentReadTx()` | Create an independent Pebble snapshot | No | Normal MVCC reads do not block the writer |
| `Backend.Hash()` | Snapshot, iterate logical buckets in stable order, and hash logical key/value data | No | Must not hash physical Pebble prefixes |
| `Backend.Snapshot()` | Publish the current state, flush when required by the recovery contract, and create the snapshot stream/checkpoint | **Flush** | Snapshot durability is separate from ordinary writes |
| `Backend.Defrag()` | Use Pebble compaction for space reclamation | No implicit bbolt rewrite | Do not emulate bbolt's stop-the-world file rewrite |
| `Backend.Size()` | Report the physical Pebble directory footprint | No | Includes engine files present on disk |
| `Backend.SizeInUse()` | Report the best available live-state estimate | No | It is backend-specific and is not a bbolt page count |
| `Backend.OpenReadTxN()` | Maintain an atomic count of active readers/snapshots | No | Used by backend metrics |
| `Backend.Close()` | Stop new work, wait for active readers/writer, and close Pebble | Recovery-dependent | Do not add an unrelated bbolt flush policy |
| `SetTxPostLockInsideApplyHook` | Store the hook and invoke it only after apply locking and batch creation | Neither | Preserves etcd's happens-before relationship |

The deliberately simplified operations are:

- `Lock`, `LockOutsideApply`, and the storage portion of
  `LockInsideApply` share the same writer-lock implementation.
- `UnsafeSeqPut` is intentionally identical to `UnsafePut`.
- `CommitAndStop` has no separate stop action when there is no background
  writer.
- `ForceCommit` must not be used as a synonym for `db.Flush()`.

These are implementation simplifications, not missing functionality.

## Read transactions

### Blocking `ReadTx`

`ReadTx` preserves the existing etcd reader contract. `RLock` starts a stable
view and `RUnlock` releases it. The writer publish path and this blocking view
must use the same view coordination so a caller never observes a partially
published backend transaction.

This path is used by initialization, schema validation, consistency checking,
and other code that follows the ordinary etcd `ReadTx` convention.

### `ConcurrentReadTx`

`ConcurrentReadTx` creates a Pebble snapshot and returns immediately. The
caller owns the snapshot until `RUnlock`/close. This is the normal MVCC read
path and must not wait for the backend writer.

The snapshot owns the read view; callers must not retain slices returned by a
transaction after releasing that transaction unless they have copied them.

## Hooks and atomic metadata

`OnPreCommitUnsafe` runs before the current Pebble batch is committed. It must
write metadata such as:

- the consistent index and term;
- a dirty Raft `ConfState`, when present.

Those writes must enter the same Pebble batch as the MVCC mutations:

```text
MVCC mutations
consistent index / term
ConfState, if dirty
        |
        +-- one atomic Pebble commit
```

The post-lock apply hook is different. It updates etcd's in-memory consistent
index state after the writer lock has been acquired and before apply mutations
are issued. It is not a storage commit hook and must not be moved to
`Unlock`.

## WAL-free recovery boundary

The server opens the Pebble backend with its own WAL disabled because Raft's
WAL is the recovery log. A successful Pebble memtable commit alone is not the
same as a flushed, durable LSM state.

The recovery invariant is:

```text
persisted consistent index <= durable Pebble state
```

Before etcd releases Raft WAL entries that are needed to recover the backend,
the Pebble-specific durability contract must:

1. commit the backend state and the flush-index marker;
2. call Pebble `Flush` so the relevant memtable state reaches durable LSM
   files;
3. publish the completed flush index only after the flush succeeds.

`LSMFlushIndex` and `FlushTo` are therefore intentionally outside the ordinary
`Backend` interface. They are not aliases for `ForceCommit`, and they must not
be silently removed as a backend-specific recovery capability.

On a crash, any state that existed only in an unflushed memtable remains
recoverable from Raft replay because the durable consistent index was never
advanced beyond the flushed Pebble boundary.

## Snapshots and restore

The normal backend snapshot path must produce a self-contained snapshot view.
It should not expose Pebble's internal WAL or require the receiver to have the
sender's directory layout.

The snapshot lifecycle is:

```text
finish the current backend publish
        |
        +-- establish the required durable boundary
        |
        +-- create a stable Pebble snapshot/checkpoint
        |
        +-- stream logical key/value frames
```

Restore writes the stream into a Pebble database, rebuilds the bucket marker
index, reloads the LSM flush marker, and only then exposes the backend to the
server. A live restore must serialize with backend lifecycle operations so no
new write can race with replacement of the logical contents.

## Size, compaction, and defrag

Pebble is an LSM engine, so its space model is different from bbolt:

- physical size includes WAL/manifest/table/metadata files that currently
  exist on disk;
- live size is affected by obsolete versions and compaction;
- `Defrag` is represented by Pebble flush/compaction, not by copying the
  database into a new bbolt file;
- a byte-for-byte equivalence between `SizeInUse` and bbolt's free-page
  accounting must not be assumed.

The backend must not add a fake freelist or page allocator merely to make the
two engines look identical.

## Non-goals for V1

V1 does not attempt to:

- change etcd's public backend interfaces;
- preserve bbolt's periodic transaction batching internally;
- emulate bbolt page allocation, freelists, mmap behavior, or fill percent;
- expose Pebble-specific types above the backend adapter;
- treat `ForceCommit` as an LSM durability barrier;
- add custom fault-injection logic to the performance harness;
- claim that performance results prove crash recovery correctness.

Correctness and recovery are validated by the official etcd unit,
integration, e2e, robustness, and proxy test suites. The performance harness
is responsible for comparing backend behavior under the same official
workloads and explicitly selected backend-specific scenarios.

## Implementation checklist

When changing this package, verify the following invariants:

- etcd's `Backend`, `BatchTx`, and `ReadTx` interfaces remain unchanged;
- every write transaction has one atomic Pebble batch;
- `UnsafeRange` and `UnsafeForEach` observe the current transaction's writes;
- hooks write into the same batch as the user mutation;
- `ConcurrentReadTx` owns an independent stable snapshot;
- `FlushTo` advances the LSM boundary only after a successful flush;
- snapshot restore rebuilds bucket metadata and the flush marker;
- `Close` cannot race with an active writer, reader, snapshot, or restore;
- tests do not confuse `Apply`, `Flush`, and `Compact`.
