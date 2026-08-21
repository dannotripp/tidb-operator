// Copyright 2024 PingCAP, Inc.
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

package backupschedule

import "sync"

// TargetLocker serializes Backup admission for one TiDB target inside one
// controller process. It is not a distributed fence.
type TargetLocker interface {
	Acquire(Target) func()
}

type targetLockEntry struct {
	mu   sync.Mutex
	refs int
}

// TargetLocks is a reference-counted keyed mutex registry. Empty entries are
// removed so schedules and clusters that no longer exist do not leak memory.
type TargetLocks struct {
	mu      sync.Mutex
	entries map[Target]*targetLockEntry
}

func NewTargetLocks() *TargetLocks {
	return &TargetLocks{entries: make(map[Target]*targetLockEntry)}
}

// Acquire locks target and returns an idempotent release function.
func (locks *TargetLocks) Acquire(target Target) func() {
	locks.mu.Lock()
	entry := locks.entries[target]
	if entry == nil {
		entry = &targetLockEntry{}
		locks.entries[target] = entry
	}
	entry.refs++
	locks.mu.Unlock()

	entry.mu.Lock()
	var once sync.Once
	return func() {
		once.Do(func() {
			entry.mu.Unlock()
			locks.mu.Lock()
			defer locks.mu.Unlock()
			entry.refs--
			if entry.refs == 0 {
				delete(locks.entries, target)
			}
		})
	}
}
