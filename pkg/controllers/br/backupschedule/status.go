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
	"reflect"

	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	brv1alpha1 "github.com/pingcap/tidb-operator/api/v2/br/v1alpha1"
)

const (
	ConditionSchedulingReady = "SchedulingReady"
	ConditionRetentionReady  = "RetentionReady"

	ReasonReconciled      = "Reconciled"
	ReasonInvalidSpec     = "InvalidSpec"
	ReasonReconcilerError = "ReconcilerError"
)

// StatusMutation applies an outcome to the direct-read BackupSchedule. The
// helper invokes it at most once. Any conflict requires a full reconciliation
// of schedule and Backup state before another status attempt.
type StatusMutation func(*brv1alpha1.BackupSchedule) (bool, error)

// StaleScheduleError reports that the object used to calculate an outcome has
// been recreated or its specification has changed.
type StaleScheduleError struct {
	ExpectedUID        types.UID
	ActualUID          types.UID
	ExpectedGeneration int64
	ActualGeneration   int64
}

func (err *StaleScheduleError) Error() string {
	return fmt.Sprintf(
		"backup schedule changed while reconciling: expected UID %q generation %d, got UID %q generation %d",
		err.ExpectedUID,
		err.ExpectedGeneration,
		err.ActualUID,
		err.ActualGeneration,
	)
}

// StatusUpdater performs optimistic status patches while preserving fields
// owned by the other BackupSchedule reconciliation loop.
type StatusUpdater struct {
	Reader client.Reader
	Writer client.SubResourceWriter
}

// NewStatusUpdater builds a status updater from an uncached API reader and the
// manager client used for status writes.
func NewStatusUpdater(reader client.Reader, managerClient client.Client) *StatusUpdater {
	return &StatusUpdater{Reader: reader, Writer: managerClient.Status()}
}

// Update direct-reads once, verifies the UID and generation used to calculate
// the outcome, and issues one optimistic status patch. A conflict or stale
// identity is returned so the controller can fully reconcile again.
func (updater *StatusUpdater) Update(
	ctx context.Context,
	key client.ObjectKey,
	expectedUID types.UID,
	expectedGeneration int64,
	mutate StatusMutation,
) error {
	if updater == nil || updater.Reader == nil || updater.Writer == nil {
		return fmt.Errorf("status reader and writer are required")
	}
	if mutate == nil {
		return fmt.Errorf("status mutation is required")
	}

	latest := &brv1alpha1.BackupSchedule{}
	if err := updater.Reader.Get(ctx, key, latest); err != nil {
		return err
	}
	if latest.UID != expectedUID || latest.Generation != expectedGeneration {
		return &StaleScheduleError{
			ExpectedUID:        expectedUID,
			ActualUID:          latest.UID,
			ExpectedGeneration: expectedGeneration,
			ActualGeneration:   latest.Generation,
		}
	}

	before := latest.DeepCopy()
	changed, err := mutate(latest)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	patch := client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})
	return updater.Writer.Patch(ctx, latest, patch)
}

// SchedulingStatus contains only fields owned by the scheduling loop.
type SchedulingStatus struct {
	LastScheduleTime *metav1.Time
	LastBackup       string
	LastBackupTime   *metav1.Time
	Condition        metav1.Condition
}

// ApplySchedulingStatus mutates only scheduling-owned status fields and
// preserves the retention condition and all legacy status.
func ApplySchedulingStatus(schedule *brv1alpha1.BackupSchedule, desired *SchedulingStatus) bool {
	before := schedule.Status.DeepCopy()
	schedule.Status.LastScheduleTime = copyTime(desired.LastScheduleTime)
	schedule.Status.LastBackup = desired.LastBackup
	schedule.Status.LastBackupTime = copyTime(desired.LastBackupTime)
	condition := desired.Condition.DeepCopy()
	condition.Type = ConditionSchedulingReady
	condition.ObservedGeneration = schedule.Generation
	apiMeta.SetStatusCondition(&schedule.Status.Conditions, *condition)
	return !reflect.DeepEqual(before, &schedule.Status)
}

// ApplyRetentionStatus mutates only the retention-owned condition.
func ApplyRetentionStatus(schedule *brv1alpha1.BackupSchedule, desired *metav1.Condition) bool {
	before := schedule.Status.DeepCopy()
	condition := desired.DeepCopy()
	condition.Type = ConditionRetentionReady
	condition.ObservedGeneration = schedule.Generation
	apiMeta.SetStatusCondition(&schedule.Status.Conditions, *condition)
	return !reflect.DeepEqual(before, &schedule.Status)
}

func copyTime(value *metav1.Time) *metav1.Time {
	if value == nil {
		return nil
	}
	return value.DeepCopy()
}
