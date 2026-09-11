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

	pebbledb "github.com/cockroachdb/pebble"
)

func dataKey(bucket, key []byte) []byte {
	result := make([]byte, 0, len(dataPrefix)+4+len(bucket)+1+len(key))
	result = append(result, dataPrefix...)
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(bucket)))
	result = append(result, length[:]...)
	result = append(result, bucket...)
	result = append(result, 0)
	result = append(result, key...)
	return result
}

// setData and deleteData encode the adapter key directly into Pebble's batch
// arena. Besides avoiding a temporary dataKey allocation, the deferred APIs
// avoid copying that temporary key into the batch a second time.
func setData(batch *pebbledb.Batch, bucket, key, value []byte) error {
	op := batch.SetDeferred(dataKeyLen(bucket, key), len(value))
	encodeDataKey(op.Key, bucket, key)
	copy(op.Value, value)
	return op.Finish()
}

func deleteData(batch *pebbledb.Batch, bucket, key []byte) error {
	op := batch.DeleteDeferred(dataKeyLen(bucket, key))
	encodeDataKey(op.Key, bucket, key)
	return op.Finish()
}

func dataKeyLen(bucket, key []byte) int {
	return len(dataPrefix) + 4 + len(bucket) + 1 + len(key)
}

func encodeDataKey(dst, bucket, key []byte) {
	n := copy(dst, dataPrefix)
	binary.BigEndian.PutUint32(dst[n:n+4], uint32(len(bucket)))
	n += 4
	n += copy(dst[n:], bucket)
	dst[n] = 0
	n++
	copy(dst[n:], key)
}

func lowerPrefix(bucket []byte) []byte   { return dataKey(bucket, nil) }
func bucketMetaKey(bucket []byte) []byte { return append(clone(bucketPrefix), bucket...) }

func prefixEnd(prefix []byte) []byte {
	end := clone(prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

func clone(v []byte) []byte { return append([]byte(nil), v...) }

func cloneBuckets(src map[string]struct{}) map[string]struct{} {
	dst := make(map[string]struct{}, len(src))
	for key := range src {
		dst[key] = struct{}{}
	}
	return dst
}
