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
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	brv1alpha1 "github.com/pingcap/tidb-operator/api/v2/br/v1alpha1"
)

type retentionStatusUpdater interface {
	Update(
		context.Context,
		client.ObjectKey,
		types.UID,
		int64,
		StatusMutation,
	) error
}

// RetentionReconciler calculates and performs count-based Backup retention.
// All destructive reads use Reader, which must be the manager API reader.
type RetentionReconciler struct {
	Reader client.Reader
	Writer client.Writer
	Status retentionStatusUpdater
	Now    func() time.Time
}

// NewRetentionReconciler builds the retention reconciler from the uncached API
// reader and the manager client used for writes.
func NewRetentionReconciler(reader client.Reader, managerClient client.Client) *RetentionReconciler {
	return &RetentionReconciler{
		Reader: reader,
		Writer: managerClient,
		Status: NewStatusUpdater(reader, managerClient),
		Now:    time.Now,
	}
}

// Reconcile validates all current-UID Backups and sends at most one deletion
// request from each fresh plan.
func (reconciler *RetentionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if reconciler == nil || reconciler.Reader == nil || reconciler.Writer == nil || reconciler.Status == nil {
		return ctrl.Result{}, fmt.Errorf("retention reader, writer, and status updater are required")
	}

	schedule := &brv1alpha1.BackupSchedule{}
	if err := reconciler.Reader.Get(ctx, req.NamespacedName, schedule); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("get BackupSchedule %s for retention: %w", req.NamespacedName, err)
	}
	if schedule.DeletionTimestamp != nil {
		return ctrl.Result{}, nil
	}

	if err := ValidateBackupSchedule(schedule); err != nil {
		statusErr := reconciler.updateCondition(
			ctx,
			schedule,
			metav1.ConditionFalse,
			ReasonInvalidSpec,
			err.Error(),
		)
		return ctrl.Result{}, statusErr
	}
	if schedule.Spec.Pause {
		return ctrl.Result{}, reconciler.updateCondition(
			ctx,
			schedule,
			metav1.ConditionTrue,
			ReasonReconciled,
			"retention is paused; no deletion requests were sent",
		)
	}
	if schedule.Spec.MaxBackups == nil || *schedule.Spec.MaxBackups == 0 {
		return ctrl.Result{}, reconciler.updateCondition(
			ctx,
			schedule,
			metav1.ConditionTrue,
			ReasonReconciled,
			"retention is disabled because maxBackups is nil or zero",
		)
	}

	backups := &brv1alpha1.BackupList{}
	if err := reconciler.Reader.List(
		ctx,
		backups,
		client.InNamespace(schedule.Namespace),
		client.MatchingLabels{ScheduleUIDLabel: string(schedule.UID)},
	); err != nil {
		return ctrl.Result{}, reconciler.fail(
			ctx,
			schedule,
			fmt.Errorf("list current-schedule Backups: %w", err),
		)
	}

	plan, err := PlanRetention(schedule, backups.Items)
	if err != nil {
		// A malformed current-UID Backup requires an explicit object update to
		// repair. Report it without creating an API-error hot loop; Backup and
		// schedule watches will enqueue the next attempt.
		return ctrl.Result{}, reconciler.updateCondition(
			ctx,
			schedule,
			metav1.ConditionFalse,
			ReasonReconcilerError,
			err.Error(),
		)
	}
	if len(plan.DeletionCandidates) == 0 {
		return ctrl.Result{}, reconciler.updateCondition(
			ctx,
			schedule,
			metav1.ConditionTrue,
			ReasonReconciled,
			"retention reconciled; no expired Backups require deletion",
		)
	}

	candidate := &plan.DeletionCandidates[0]
	if err := DeleteRetentionCandidate(ctx, reconciler.Reader, reconciler.Writer, schedule, candidate); err != nil {
		if errors.Is(err, ErrRetentionPlanStale) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, reconciler.fail(ctx, schedule, err)
	}

	message := fmt.Sprintf(
		"retention reconciled; accepted deletion request for Backup %s/%s",
		candidate.Backup.Namespace,
		candidate.Backup.Name,
	)
	if err := reconciler.updateCondition(
		ctx,
		schedule,
		metav1.ConditionTrue,
		ReasonReconciled,
		message,
	); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{Requeue: true}, nil
}

func (reconciler *RetentionReconciler) fail(
	ctx context.Context,
	schedule *brv1alpha1.BackupSchedule,
	reconcileErr error,
) error {
	statusErr := reconciler.updateCondition(
		ctx,
		schedule,
		metav1.ConditionFalse,
		ReasonReconcilerError,
		reconcileErr.Error(),
	)
	return errors.Join(reconcileErr, statusErr)
}

func (reconciler *RetentionReconciler) updateCondition(
	ctx context.Context,
	schedule *brv1alpha1.BackupSchedule,
	status metav1.ConditionStatus,
	reason string,
	message string,
) error {
	now := time.Now().UTC()
	if reconciler.Now != nil {
		now = reconciler.Now().UTC()
	}
	condition := NewCondition(
		ConditionRetentionReady,
		status,
		reason,
		message,
		schedule.Generation,
		now,
	)
	return reconciler.Status.Update(
		ctx,
		client.ObjectKeyFromObject(schedule),
		schedule.UID,
		schedule.Generation,
		func(latest *brv1alpha1.BackupSchedule) (bool, error) {
			return ApplyRetentionStatus(latest, &condition), nil
		},
	)
}
