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
	"errors"
	"fmt"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	brv1alpha1 "github.com/pingcap/tidb-operator/api/v2/br/v1alpha1"
)

// ErrRetentionPlanStale means no delete was sent because the schedule or
// candidate changed after the retention plan was calculated.
var ErrRetentionPlanStale = errors.New("retention plan is stale")

// DeleteRetentionCandidate performs the final uncached schedule and Backup
// checks and sends one UID- and resourceVersion-preconditioned delete. It never
// patches cleanPolicy or removes finalizers.
//
//nolint:gocyclo // Every branch is a distinct fail-closed destructive-action guard.
func DeleteRetentionCandidate(
	ctx context.Context,
	reader client.Reader,
	writer client.Writer,
	schedule *brv1alpha1.BackupSchedule,
	candidate *RetentionCandidate,
) error {
	if reader == nil || writer == nil {
		return fmt.Errorf("retention reader and writer are required")
	}
	if schedule == nil || candidate == nil || candidate.Backup == nil {
		return fmt.Errorf("backup schedule and retention candidate are required")
	}

	liveBackup := &brv1alpha1.Backup{}
	if err := reader.Get(ctx, client.ObjectKeyFromObject(candidate.Backup), liveBackup); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("%w: Backup disappeared", ErrRetentionPlanStale)
		}
		return fmt.Errorf("get Backup %s before retention delete: %w", client.ObjectKeyFromObject(candidate.Backup), err)
	}
	if liveBackup.UID != candidate.Backup.UID ||
		liveBackup.ResourceVersion != candidate.Backup.ResourceVersion {
		return fmt.Errorf("%w: Backup UID or resourceVersion changed", ErrRetentionPlanStale)
	}
	if liveBackup.DeletionTimestamp != nil {
		return fmt.Errorf("%w: Backup is already terminating", ErrRetentionPlanStale)
	}
	if !apiequality.Semantic.DeepEqual(liveBackup.Spec, candidate.Backup.Spec) {
		return fmt.Errorf("%w: Backup specification changed", ErrRetentionPlanStale)
	}

	liveScheduledTime, err := ValidateManagedBackup(schedule, liveBackup)
	if err != nil {
		return fmt.Errorf("%w: Backup identity is no longer valid: %w", ErrRetentionPlanStale, err)
	}
	if !liveScheduledTime.Equal(candidate.ScheduledTime) {
		return fmt.Errorf("%w: Backup scheduled time changed", ErrRetentionPlanStale)
	}
	liveTarget, err := ResolveBackupTarget(liveBackup)
	if err != nil {
		return fmt.Errorf("%w: Backup target is no longer valid: %w", ErrRetentionPlanStale, err)
	}
	if liveTarget != candidate.Target {
		return fmt.Errorf("%w: Backup target changed from %s/%s to %s/%s", ErrRetentionPlanStale,
			candidate.Target.Namespace,
			candidate.Target.Cluster,
			liveTarget.Namespace,
			liveTarget.Cluster,
		)
	}
	liveState := ClassifyBackup(liveBackup)
	if liveState == BackupActive || liveState == BackupAmbiguous || liveState != candidate.State {
		return fmt.Errorf("%w: Backup terminal state changed from %q to %q", ErrRetentionPlanStale, candidate.State, liveState)
	}

	// Read the schedule last to make the pause, generation, and deletion checks
	// as close as possible to the destructive request.
	liveSchedule := &brv1alpha1.BackupSchedule{}
	if err := reader.Get(ctx, client.ObjectKeyFromObject(schedule), liveSchedule); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("%w: BackupSchedule disappeared", ErrRetentionPlanStale)
		}
		return fmt.Errorf("get BackupSchedule %s before retention delete: %w", client.ObjectKeyFromObject(schedule), err)
	}
	if liveSchedule.UID != schedule.UID || liveSchedule.Generation != schedule.Generation {
		return fmt.Errorf("%w: BackupSchedule UID or generation changed", ErrRetentionPlanStale)
	}
	if liveSchedule.DeletionTimestamp != nil || liveSchedule.Spec.Pause {
		return fmt.Errorf("%w: BackupSchedule is deleting or paused", ErrRetentionPlanStale)
	}
	if liveSchedule.Spec.MaxBackups == nil || *liveSchedule.Spec.MaxBackups <= 0 {
		return fmt.Errorf("%w: BackupSchedule retention is disabled", ErrRetentionPlanStale)
	}
	if err := ValidateBackupSchedule(liveSchedule); err != nil {
		return fmt.Errorf("%w: BackupSchedule is no longer valid: %w", ErrRetentionPlanStale, err)
	}
	if _, err := ValidateManagedBackup(liveSchedule, liveBackup); err != nil {
		return fmt.Errorf("%w: Backup no longer matches BackupSchedule: %w", ErrRetentionPlanStale, err)
	}

	uid := liveBackup.UID
	resourceVersion := liveBackup.ResourceVersion
	preconditions := metav1.Preconditions{
		UID:             &uid,
		ResourceVersion: &resourceVersion,
	}
	if err := writer.Delete(ctx, liveBackup, client.Preconditions(preconditions)); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("%w: Backup disappeared before delete", ErrRetentionPlanStale)
		}
		return fmt.Errorf("delete Backup %s with preconditions: %w", client.ObjectKeyFromObject(liveBackup), err)
	}
	return nil
}
