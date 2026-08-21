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
	"sort"
	"time"

	brv1alpha1 "github.com/pingcap/tidb-operator/api/v2/br/v1alpha1"
)

const maxRetainedUnsuccessfulBackups = 5

// RetentionCandidate is an immutable snapshot of one Backup selected for
// deletion. UID and resourceVersion are revalidated immediately before the
// delete request.
type RetentionCandidate struct {
	Backup        *brv1alpha1.Backup
	ScheduledTime time.Time
	Target        Target
	State         BackupTerminalState
}

// RetentionPlan contains terminal Backups that exceed the independent success
// and unsuccessful-history limits. Candidates are ordered oldest first.
type RetentionPlan struct {
	DeletionCandidates []RetentionCandidate
}

// PlanRetention validates every current-UID Backup before calculating any
// destructive action. Backups without the current UID, from an older schedule
// incarnation, or from another namespace are outside this schedule's scope.
func PlanRetention(
	schedule *brv1alpha1.BackupSchedule,
	backups []brv1alpha1.Backup,
) (RetentionPlan, error) {
	if schedule == nil {
		return RetentionPlan{}, fmt.Errorf("backup schedule is nil")
	}
	if schedule.Spec.MaxBackups == nil || *schedule.Spec.MaxBackups == 0 {
		return RetentionPlan{}, nil
	}
	if *schedule.Spec.MaxBackups < 0 {
		return RetentionPlan{}, fmt.Errorf("spec.maxBackups must be nonnegative")
	}

	successful := make([]RetentionCandidate, 0, len(backups))
	unsuccessful := make([]RetentionCandidate, 0, len(backups))
	for i := range backups {
		backup := &backups[i]
		if backup.Namespace != schedule.Namespace {
			continue
		}
		if backup.Labels[ScheduleUIDLabel] != string(schedule.UID) {
			continue
		}

		scheduledTime, err := ValidateManagedBackup(schedule, backup)
		if err != nil {
			return RetentionPlan{}, fmt.Errorf(
				"validate current-schedule Backup %s/%s: %w",
				backup.Namespace,
				backup.Name,
				err,
			)
		}

		state := ClassifyBackup(backup)
		if state == BackupAmbiguous {
			return RetentionPlan{}, fmt.Errorf(
				"classify current-schedule Backup %s/%s: terminal conditions are ambiguous",
				backup.Namespace,
				backup.Name,
			)
		}
		if backup.DeletionTimestamp != nil || state == BackupActive {
			continue
		}
		target, err := ResolveBackupTarget(backup)
		if err != nil {
			return RetentionPlan{}, fmt.Errorf(
				"resolve current-schedule Backup %s/%s target: %w",
				backup.Namespace,
				backup.Name,
				err,
			)
		}

		candidate := RetentionCandidate{
			Backup:        backup.DeepCopy(),
			ScheduledTime: scheduledTime,
			Target:        target,
			State:         state,
		}
		switch state {
		case BackupSucceeded:
			successful = append(successful, candidate)
		case BackupFailed, BackupInvalid:
			unsuccessful = append(unsuccessful, candidate)
		default:
			return RetentionPlan{}, fmt.Errorf(
				"classify current-schedule Backup %s/%s: unsupported state %q",
				backup.Namespace,
				backup.Name,
				state,
			)
		}
	}

	sortRetentionCandidatesNewestFirst(successful)
	sortRetentionCandidatesNewestFirst(unsuccessful)

	maxSuccessful := int(*schedule.Spec.MaxBackups)
	deletions := make([]RetentionCandidate, 0, len(successful)+len(unsuccessful))
	if len(successful) > maxSuccessful {
		deletions = append(deletions, successful[maxSuccessful:]...)
	}
	if len(unsuccessful) > maxRetainedUnsuccessfulBackups {
		deletions = append(deletions, unsuccessful[maxRetainedUnsuccessfulBackups:]...)
	}
	sortRetentionCandidatesOldestFirst(deletions)

	return RetentionPlan{DeletionCandidates: deletions}, nil
}

func sortRetentionCandidatesNewestFirst(candidates []RetentionCandidate) {
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].ScheduledTime.Equal(candidates[j].ScheduledTime) {
			return candidates[i].Backup.Name > candidates[j].Backup.Name
		}
		return candidates[i].ScheduledTime.After(candidates[j].ScheduledTime)
	})
}

func sortRetentionCandidatesOldestFirst(candidates []RetentionCandidate) {
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].ScheduledTime.Equal(candidates[j].ScheduledTime) {
			return candidates[i].Backup.Name < candidates[j].Backup.Name
		}
		return candidates[i].ScheduledTime.Before(candidates[j].ScheduledTime)
	})
}
