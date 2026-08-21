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
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

const maxScheduledTimesPerReconcile = 1000

// Clock is the scheduling controller's time source. The repository real and
// fake clocks both satisfy this intentionally narrow interface.
type Clock interface {
	Now() time.Time
}

// OccurrenceScan is the bounded result of walking a schedule from a durable
// cursor. BatchEnd is the latest occurrence in this batch and More reports
// that at least one additional occurrence is already due.
type OccurrenceScan struct {
	Count     int
	LatestDue time.Time
	BatchEnd  time.Time
	More      bool
}

// ScanOccurrences walks occurrences strictly after cursor and no later than
// now. It examines at most limit due occurrences plus one lookahead. It never
// derives occurrences from reconciliation time, which preserves @every's
// durable cursor anchor.
func ScanOccurrences(schedule cron.Schedule, cursor, now time.Time, limit int) (OccurrenceScan, error) {
	if schedule == nil {
		return OccurrenceScan{}, fmt.Errorf("cron schedule is required")
	}
	if limit <= 0 {
		return OccurrenceScan{}, fmt.Errorf("occurrence scan limit must be positive")
	}

	cursor = cursor.UTC()
	now = now.UTC()
	result := OccurrenceScan{BatchEnd: cursor}
	current := cursor
	for result.Count < limit {
		next := schedule.Next(current)
		if next.IsZero() {
			return OccurrenceScan{}, fmt.Errorf("cron schedule has no future occurrence")
		}
		next = next.UTC()
		if !next.After(current) {
			return OccurrenceScan{}, fmt.Errorf("cron schedule did not advance after %s", current.Format(time.RFC3339Nano))
		}
		if next.After(now) {
			return result, nil
		}

		result.Count++
		result.LatestDue = next
		result.BatchEnd = next
		current = next
	}

	lookahead := schedule.Next(current)
	if lookahead.IsZero() {
		return OccurrenceScan{}, fmt.Errorf("cron schedule has no future occurrence")
	}
	lookahead = lookahead.UTC()
	if !lookahead.After(current) {
		return OccurrenceScan{}, fmt.Errorf("cron schedule did not advance after %s", current.Format(time.RFC3339Nano))
	}
	result.More = !lookahead.After(now)
	return result, nil
}

// NextWakeTime calculates only the controller wake deadline. The first future
// occurrence remains anchored to the durable cursor, which is significant for
// @every schedules. If that occurrence is already due, the non-persisted now
// anchor supplies a future safety wake while overlap or another blocker keeps
// the cursor unchanged.
func NextWakeTime(schedule cron.Schedule, cursor, now time.Time) (time.Time, error) {
	if schedule == nil {
		return time.Time{}, fmt.Errorf("cron schedule is required")
	}
	cursor = cursor.UTC()
	now = now.UTC()
	next := schedule.Next(cursor).UTC()
	if next.IsZero() {
		return time.Time{}, fmt.Errorf("cron schedule has no future occurrence")
	}
	if !next.After(cursor) {
		return time.Time{}, fmt.Errorf("cron schedule did not advance after %s", cursor.Format(time.RFC3339Nano))
	}
	if next.After(now) {
		return next, nil
	}

	fallback := schedule.Next(now).UTC()
	if fallback.IsZero() {
		return time.Time{}, fmt.Errorf("cron schedule has no future occurrence")
	}
	if !fallback.After(now) {
		return time.Time{}, fmt.Errorf("cron schedule did not advance after %s", now.Format(time.RFC3339Nano))
	}
	return fallback, nil
}
