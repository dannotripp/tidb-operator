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
	"reflect"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	brv1alpha1 "github.com/pingcap/tidb-operator/api/v2/br/v1alpha1"
)

const schedulingReconciledMessage = "BackupSchedule scheduling reconciled"

// ScheduleStatusUpdater is the optimistic status contract consumed by the
// scheduling engine. StatusUpdater implements it.
type ScheduleStatusUpdater interface {
	Update(
		ctx context.Context,
		key client.ObjectKey,
		expectedUID types.UID,
		expectedGeneration int64,
		mutate StatusMutation,
	) error
}

// SchedulingReconciler owns only snapshot occurrence creation and the
// scheduling-owned BackupSchedule status fields. Controller registration and
// event mapping are intentionally kept outside this file.
type SchedulingReconciler struct {
	Client        client.Client
	APIReader     client.Reader
	Clock         Clock
	Locks         TargetLocker
	StatusUpdater ScheduleStatusUpdater
}

func NewSchedulingReconciler(
	managerClient client.Client,
	apiReader client.Reader,
	clock Clock,
	locks TargetLocker,
) *SchedulingReconciler {
	return &SchedulingReconciler{
		Client:        managerClient,
		APIReader:     apiReader,
		Clock:         clock,
		Locks:         locks,
		StatusUpdater: NewStatusUpdater(apiReader, managerClient),
	}
}

//nolint:gocyclo // The branches mirror the documented scheduling state machine.
func (r *SchedulingReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if err := r.validateDependencies(); err != nil {
		return ctrl.Result{}, err
	}

	schedule := &brv1alpha1.BackupSchedule{}
	if err := r.APIReader.Get(ctx, req.NamespacedName, schedule); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !schedule.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// BackupSchedule status and managed identity are second-precision API
	// values. Truncate once so decisions and the persisted cursor agree after
	// an API round trip.
	now := r.Clock.Now().UTC().Truncate(time.Second)
	if err := ValidateBackupSchedule(schedule); err != nil {
		statusErr := r.writeSchedulingStatus(
			ctx,
			schedule,
			now,
			nil,
			nil,
			metav1.ConditionFalse,
			ReasonInvalidSpec,
			err.Error(),
		)
		return ctrl.Result{}, statusErr
	}
	parsed, err := ParseSchedule(schedule.Spec.Schedule)
	if err != nil {
		return r.reconcilerError(ctx, schedule, now, err)
	}
	target, err := ResolveScheduleTarget(schedule)
	if err != nil {
		return r.reconcilerError(ctx, schedule, now, err)
	}

	if schedule.Status.LastScheduleTime == nil {
		return r.observeFirstSchedule(ctx, schedule, parsed, now)
	}

	cursor := schedule.Status.LastScheduleTime.UTC()
	scan, err := ScanOccurrences(parsed, cursor, now, maxScheduledTimesPerReconcile)
	if err != nil {
		return r.reconcilerError(ctx, schedule, now, err)
	}

	if scan.More {
		if schedule.Spec.Pause {
			return r.advanceCursorAndContinue(ctx, schedule, now, scan.BatchEnd, "paused schedule consumed a bounded occurrence batch")
		}

		blocker, err := r.findLiveBlockerWithLock(ctx, target)
		if err != nil {
			return r.reconcilerError(ctx, schedule, now, err)
		}
		if blocker != nil {
			if err := r.writeSchedulingStatus(
				ctx,
				schedule,
				now,
				nil,
				nil,
				metav1.ConditionTrue,
				ReasonReconciled,
				fmt.Sprintf("waiting for active Backup %s/%s", blocker.Namespace, blocker.Name),
			); err != nil {
				return ctrl.Result{}, err
			}
			return r.wakeResult(parsed, cursor, now)
		}
		return r.advanceCursorAndContinue(ctx, schedule, now, scan.BatchEnd, "advanced a bounded occurrence batch")
	}

	if scan.Count == 0 {
		if err := r.writeSchedulingStatus(
			ctx,
			schedule,
			now,
			nil,
			nil,
			metav1.ConditionTrue,
			ReasonReconciled,
			schedulingReconciledMessage,
		); err != nil {
			return ctrl.Result{}, err
		}
		return r.wakeResult(parsed, cursor, now)
	}

	due := scan.LatestDue.UTC()
	if schedule.Spec.Pause {
		if err := r.writeSchedulingStatus(
			ctx,
			schedule,
			now,
			&due,
			nil,
			metav1.ConditionTrue,
			ReasonReconciled,
			"paused schedule consumed due occurrences without creating a Backup",
		); err != nil {
			return ctrl.Result{}, err
		}
		return r.wakeResult(parsed, due, now)
	}

	desired, err := RenderBackup(schedule, due)
	if err != nil {
		return r.reconcilerError(ctx, schedule, now, err)
	}
	observed, blocker, err := r.createOrRecover(ctx, schedule, target, desired)
	if err != nil {
		return r.reconcilerError(ctx, schedule, now, err)
	}
	if blocker != nil {
		if err := r.writeSchedulingStatus(
			ctx,
			schedule,
			now,
			nil,
			nil,
			metav1.ConditionTrue,
			ReasonReconciled,
			fmt.Sprintf("waiting for active Backup %s/%s", blocker.Namespace, blocker.Name),
		); err != nil {
			return ctrl.Result{}, err
		}
		return r.wakeResult(parsed, cursor, now)
	}

	if err := r.writeSchedulingStatus(
		ctx,
		schedule,
		now,
		&due,
		observed,
		metav1.ConditionTrue,
		ReasonReconciled,
		schedulingReconciledMessage,
	); err != nil {
		return ctrl.Result{}, err
	}
	return r.wakeResult(parsed, due, now)
}

func (r *SchedulingReconciler) validateDependencies() error {
	if r.Client == nil {
		return fmt.Errorf("scheduling client is required")
	}
	if r.APIReader == nil {
		return fmt.Errorf("scheduling API reader is required")
	}
	if r.Clock == nil {
		return fmt.Errorf("scheduling clock is required")
	}
	if r.Locks == nil {
		return fmt.Errorf("scheduling target locks are required")
	}
	if r.StatusUpdater == nil {
		return fmt.Errorf("scheduling status updater is required")
	}
	return nil
}

func (r *SchedulingReconciler) observeFirstSchedule(
	ctx context.Context,
	schedule *brv1alpha1.BackupSchedule,
	parsed interface{ Next(time.Time) time.Time },
	now time.Time,
) (ctrl.Result, error) {
	newest, err := FindNewestManagedBackup(ctx, r.APIReader, schedule)
	if err != nil {
		return r.reconcilerError(ctx, schedule, now, err)
	}

	cursor := now
	var observed *brv1alpha1.Backup
	message := "initialized scheduling cursor without creating a Backup"
	if newest != nil {
		cursor = newest.ScheduledTime.UTC()
		observed = newest.Backup
		message = fmt.Sprintf("recovered scheduling cursor from Backup %s/%s", observed.Namespace, observed.Name)
	}
	if err := r.writeSchedulingStatus(
		ctx,
		schedule,
		now,
		&cursor,
		observed,
		metav1.ConditionTrue,
		ReasonReconciled,
		message,
	); err != nil {
		return ctrl.Result{}, err
	}
	return r.wakeResult(parsed, cursor, now)
}

func (r *SchedulingReconciler) advanceCursorAndContinue(
	ctx context.Context,
	schedule *brv1alpha1.BackupSchedule,
	now time.Time,
	cursor time.Time,
	message string,
) (ctrl.Result, error) {
	if err := r.writeSchedulingStatus(
		ctx,
		schedule,
		now,
		&cursor,
		nil,
		metav1.ConditionTrue,
		ReasonReconciled,
		message,
	); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{Requeue: true}, nil
}

func (r *SchedulingReconciler) findLiveBlockerWithLock(ctx context.Context, target Target) (*brv1alpha1.Backup, error) {
	release := r.Locks.Acquire(target)
	defer release()
	return FindBlockingBackup(ctx, r.APIReader, target, client.ObjectKey{})
}

//nolint:gocritic // Separate observed and blocker results make the state transition explicit.
func (r *SchedulingReconciler) createOrRecover(
	ctx context.Context,
	schedule *brv1alpha1.BackupSchedule,
	target Target,
	desired *brv1alpha1.Backup,
) (*brv1alpha1.Backup, *brv1alpha1.Backup, error) {
	release := r.Locks.Acquire(target)
	defer release()

	key := client.ObjectKeyFromObject(desired)
	existing := &brv1alpha1.Backup{}
	err := r.APIReader.Get(ctx, key, existing)
	switch {
	case err == nil:
		if matchErr := MatchesRenderedBackup(schedule, desired, existing); matchErr != nil {
			return nil, nil, fmt.Errorf("backup %s already exists with conflicting identity: %w", key, matchErr)
		}
		return existing, nil, nil
	case !apierrors.IsNotFound(err):
		return nil, nil, fmt.Errorf("get desired Backup %s: %w", key, err)
	}

	blocker, err := FindBlockingBackup(ctx, r.Client, target, key)
	if err != nil {
		return nil, nil, err
	}
	if blocker != nil {
		return nil, blocker, nil
	}
	blocker, err = FindBlockingBackup(ctx, r.APIReader, target, key)
	if err != nil {
		return nil, nil, err
	}
	if blocker != nil {
		return nil, blocker, nil
	}

	if err := r.Client.Create(ctx, desired); err == nil {
		return desired.DeepCopy(), nil, nil
	} else if !apierrors.IsAlreadyExists(err) {
		return nil, nil, fmt.Errorf("create Backup %s: %w", key, err)
	}

	existing = &brv1alpha1.Backup{}
	if err := r.APIReader.Get(ctx, key, existing); err != nil {
		return nil, nil, fmt.Errorf("get concurrently created Backup %s: %w", key, err)
	}
	if err := MatchesRenderedBackup(schedule, desired, existing); err != nil {
		return nil, nil, fmt.Errorf("backup %s was created concurrently with conflicting identity: %w", key, err)
	}
	return existing, nil, nil
}

func (r *SchedulingReconciler) reconcilerError(
	ctx context.Context,
	schedule *brv1alpha1.BackupSchedule,
	now time.Time,
	cause error,
) (ctrl.Result, error) {
	statusErr := r.writeSchedulingStatus(
		ctx,
		schedule,
		now,
		nil,
		nil,
		metav1.ConditionFalse,
		ReasonReconcilerError,
		cause.Error(),
	)
	if statusErr != nil {
		return ctrl.Result{}, errors.Join(cause, statusErr)
	}
	return ctrl.Result{}, cause
}

func (r *SchedulingReconciler) writeSchedulingStatus(
	ctx context.Context,
	schedule *brv1alpha1.BackupSchedule,
	now time.Time,
	cursor *time.Time,
	observed *brv1alpha1.Backup,
	conditionStatus metav1.ConditionStatus,
	reason string,
	message string,
) error {
	expected := schedulingOwnedStatus(schedule)
	return r.StatusUpdater.Update(
		ctx,
		client.ObjectKeyFromObject(schedule),
		schedule.UID,
		schedule.Generation,
		func(latest *brv1alpha1.BackupSchedule) (bool, error) {
			if !reflect.DeepEqual(expected, schedulingOwnedStatus(latest)) {
				return false, fmt.Errorf("scheduling-owned status changed while reconciling")
			}

			desired := &SchedulingStatus{
				LastScheduleTime: latest.Status.LastScheduleTime,
				LastBackup:       latest.Status.LastBackup,
				LastBackupTime:   latest.Status.LastBackupTime,
				Condition: NewCondition(
					ConditionSchedulingReady,
					conditionStatus,
					reason,
					message,
					latest.Generation,
					now,
				),
			}
			if cursor != nil {
				value := metav1.NewTime(cursor.UTC())
				desired.LastScheduleTime = &value
			}
			if observed != nil {
				desired.LastBackup = observed.Name
				created := observed.CreationTimestamp.DeepCopy()
				desired.LastBackupTime = created
			}
			return ApplySchedulingStatus(latest, desired), nil
		},
	)
}

type schedulingOwnedStatusSnapshot struct {
	LastScheduleTime *metav1.Time
	LastBackup       string
	LastBackupTime   *metav1.Time
	Condition        *metav1.Condition
}

func schedulingOwnedStatus(schedule *brv1alpha1.BackupSchedule) schedulingOwnedStatusSnapshot {
	var condition *metav1.Condition
	if found := apiMeta.FindStatusCondition(schedule.Status.Conditions, ConditionSchedulingReady); found != nil {
		condition = found.DeepCopy()
	}
	return schedulingOwnedStatusSnapshot{
		LastScheduleTime: copyTime(schedule.Status.LastScheduleTime),
		LastBackup:       schedule.Status.LastBackup,
		LastBackupTime:   copyTime(schedule.Status.LastBackupTime),
		Condition:        condition,
	}
}

func (r *SchedulingReconciler) wakeResult(
	schedule interface{ Next(time.Time) time.Time },
	cursor time.Time,
	now time.Time,
) (ctrl.Result, error) {
	next, err := NextWakeTime(schedule, cursor, now)
	if err != nil {
		return ctrl.Result{}, err
	}
	delay := next.Sub(now.UTC())
	if delay <= 0 {
		return ctrl.Result{Requeue: true}, nil
	}
	return ctrl.Result{RequeueAfter: delay}, nil
}
