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

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScanOccurrencesBoundaries(t *testing.T) {
	parsed, err := ParseSchedule("@every 1m")
	require.NoError(t, err)
	cursor := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name       string
		now        time.Time
		wantCount  int
		wantLatest time.Time
		wantMore   bool
	}{
		{name: "none", now: cursor.Add(30 * time.Second), wantCount: 0},
		{name: "one", now: cursor.Add(time.Minute), wantCount: 1, wantLatest: cursor.Add(time.Minute)},
		{name: "newest of several", now: cursor.Add(3*time.Minute + 30*time.Second), wantCount: 3, wantLatest: cursor.Add(3 * time.Minute)},
		{name: "exactly limit", now: cursor.Add(1000 * time.Minute), wantCount: 1000, wantLatest: cursor.Add(1000 * time.Minute)},
		{name: "one beyond limit", now: cursor.Add(1001 * time.Minute), wantCount: 1000, wantLatest: cursor.Add(1000 * time.Minute), wantMore: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scan, err := ScanOccurrences(parsed, cursor, tt.now, 1000)
			require.NoError(t, err)
			assert.Equal(t, tt.wantCount, scan.Count)
			assert.Equal(t, tt.wantMore, scan.More)
			assert.Equal(t, tt.wantLatest, scan.LatestDue)
			if tt.wantCount == 0 {
				assert.Equal(t, cursor, scan.BatchEnd)
			} else {
				assert.Equal(t, tt.wantLatest, scan.BatchEnd)
			}
		})
	}
}

func TestScanOccurrencesRejectsNonAdvancingSchedule(t *testing.T) {
	cursor := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	_, err := ScanOccurrences(fixedSchedule{next: cursor}, cursor, cursor.Add(time.Hour), 1000)
	require.ErrorContains(t, err, "did not advance")
	_, err = ScanOccurrences(nil, cursor, cursor, 1000)
	require.Error(t, err)
	_, err = ScanOccurrences(fixedSchedule{next: cursor.Add(time.Minute)}, cursor, cursor, 0)
	require.Error(t, err)
}

func TestNextWakeTimeUsesNonPersistedFallbackWhenOccurrenceIsOverdue(t *testing.T) {
	parsed, err := ParseSchedule("@every 1m")
	require.NoError(t, err)
	cursor := time.Date(2026, 8, 20, 0, 0, 30, 0, time.UTC)
	now := time.Date(2026, 8, 20, 0, 2, 10, 0, time.UTC)

	scan, err := ScanOccurrences(parsed, cursor, now, 1000)
	require.NoError(t, err)
	assert.Equal(t, time.Date(2026, 8, 20, 0, 1, 30, 0, time.UTC), scan.LatestDue)

	wake, err := NextWakeTime(parsed, cursor, now)
	require.NoError(t, err)
	assert.Equal(t, time.Date(2026, 8, 20, 0, 3, 10, 0, time.UTC), wake)
	assert.NotEqual(t, wake, scan.LatestDue, "wake-only calculation must not become the logical occurrence")
}

func TestNextWakeTimePreservesEveryCursorAnchor(t *testing.T) {
	parsed, err := ParseSchedule("@every 10m")
	require.NoError(t, err)
	cursor := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	now := cursor.Add(5 * time.Minute)

	wake, err := NextWakeTime(parsed, cursor, now)
	require.NoError(t, err)
	assert.Equal(t, cursor.Add(10*time.Minute), wake)
}

func TestNextWakeTimeUsesFutureCursor(t *testing.T) {
	parsed, err := ParseSchedule("@every 1m")
	require.NoError(t, err)
	now := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	cursor := now.Add(5 * time.Minute)
	wake, err := NextWakeTime(parsed, cursor, now)
	require.NoError(t, err)
	assert.Equal(t, cursor.Add(time.Minute), wake)
}

func TestTargetLocksSerializeAndReleaseEntries(t *testing.T) {
	locks := NewTargetLocks()
	target := Target{Namespace: "backups", Cluster: "example"}
	releaseFirst := locks.Acquire(target)

	acquiredSecond := make(chan struct{})
	releaseSecond := make(chan func(), 1)
	go func() {
		release := locks.Acquire(target)
		close(acquiredSecond)
		releaseSecond <- release
	}()

	select {
	case <-acquiredSecond:
		t.Fatal("second target lock acquired before first released")
	case <-time.After(20 * time.Millisecond):
	}
	releaseFirst()
	select {
	case <-acquiredSecond:
	case <-time.After(time.Second):
		t.Fatal("second target lock did not acquire")
	}
	(<-releaseSecond)()

	locks.mu.Lock()
	assert.Empty(t, locks.entries)
	locks.mu.Unlock()
}

func TestTargetLocksDoNotSerializeDifferentTargets(t *testing.T) {
	locks := NewTargetLocks()
	releaseFirst := locks.Acquire(Target{Namespace: "backups", Cluster: "one"})
	defer releaseFirst()

	done := make(chan struct{})
	go func() {
		release := locks.Acquire(Target{Namespace: "backups", Cluster: "two"})
		release()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("different targets were serialized")
	}
}

type fixedSchedule struct {
	next time.Time
}

func (schedule fixedSchedule) Next(time.Time) time.Time {
	return schedule.next
}
