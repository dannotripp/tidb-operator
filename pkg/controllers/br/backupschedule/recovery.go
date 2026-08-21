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
	"context"
	"fmt"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	brv1alpha1 "github.com/pingcap/tidb-operator/api/v2/br/v1alpha1"
)

// ManagedBackup is a generated Backup together with its validated logical
// occurrence.
type ManagedBackup struct {
	Backup        *brv1alpha1.Backup
	ScheduledTime time.Time
}

// FindNewestManagedBackup finds the newest safe recovery candidate for the
// current BackupSchedule incarnation. Each Backup is validated against its own
// durable identity and canonical destination, not the schedule's current
// template, so a template destination edit cannot erase recovery evidence.
func FindNewestManagedBackup(
	ctx context.Context,
	reader client.Reader,
	schedule *brv1alpha1.BackupSchedule,
) (*ManagedBackup, error) {
	if reader == nil {
		return nil, fmt.Errorf("backup reader is required")
	}
	if schedule == nil {
		return nil, fmt.Errorf("backup schedule is required")
	}

	backups := &brv1alpha1.BackupList{}
	if err := reader.List(
		ctx,
		backups,
		client.InNamespace(schedule.Namespace),
		client.MatchingLabels{ScheduleUIDLabel: string(schedule.UID)},
	); err != nil {
		return nil, fmt.Errorf("list generated Backups for recovery: %w", err)
	}

	var newest *ManagedBackup
	for i := range backups.Items {
		backup := &backups.Items[i]
		scheduledTime, err := ValidateManagedBackup(schedule, backup)
		if err != nil {
			continue
		}
		if newest == nil || scheduledTime.After(newest.ScheduledTime) ||
			(scheduledTime.Equal(newest.ScheduledTime) && backup.Name > newest.Backup.Name) {
			newest = &ManagedBackup{
				Backup:        backup.DeepCopy(),
				ScheduledTime: scheduledTime.UTC(),
			}
		}
	}
	return newest, nil
}
