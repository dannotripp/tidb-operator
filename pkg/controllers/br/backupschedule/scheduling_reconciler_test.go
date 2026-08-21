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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	brv1alpha1 "github.com/pingcap/tidb-operator/api/v2/br/v1alpha1"
)

func TestSchedulingFirstObservationInitializesWithoutCreating(t *testing.T) {
	now := time.Date(2026, 8, 20, 1, 23, 45, 987654321, time.UTC)
	schedule := validSchedule()
	priorBackupTime := metav1.NewTime(now.Add(-24 * time.Hour).Truncate(time.Second))
	schedule.Status.LastBackup = "prior"
	schedule.Status.LastBackupTime = &priorBackupTime
	base := newSchedulingFakeClient(t, schedule)
	reconciler := NewSchedulingReconciler(base, base, staticClock{now: now}, NewTargetLocks())

	result, err := reconciler.Reconcile(context.Background(), requestFor(schedule))
	require.NoError(t, err)
	assert.Equal(t, 37*time.Minute-45*time.Second, result.RequeueAfter)

	stored := getSchedule(t, base, schedule)
	require.NotNil(t, stored.Status.LastScheduleTime)
	assert.True(t, stored.Status.LastScheduleTime.Time.Equal(now.Truncate(time.Second)))
	assert.Equal(t, "prior", stored.Status.LastBackup)
	assertMetav1TimeEqual(t, &priorBackupTime, stored.Status.LastBackupTime)
	assert.Empty(t, listBackups(t, base).Items)
	assertSchedulingReady(t, stored, metav1.ConditionTrue, ReasonReconciled)
}

func TestSchedulingFirstObservationRecoversNewestCreationTimestamp(t *testing.T) {
	now := time.Date(2026, 8, 20, 3, 15, 0, 0, time.UTC)
	schedule := validSchedule()
	scheduled := time.Date(2026, 8, 20, 2, 0, 0, 0, time.UTC)
	backup, err := RenderBackup(schedule, scheduled)
	require.NoError(t, err)
	created := metav1.NewTime(scheduled.Add(17 * time.Second))
	backup.CreationTimestamp = created
	base := newSchedulingFakeClient(t, schedule, backup)
	reconciler := NewSchedulingReconciler(base, base, staticClock{now: now}, NewTargetLocks())

	_, err = reconciler.Reconcile(context.Background(), requestFor(schedule))
	require.NoError(t, err)
	stored := getSchedule(t, base, schedule)
	assert.True(t, stored.Status.LastScheduleTime.Time.Equal(scheduled))
	assert.Equal(t, backup.Name, stored.Status.LastBackup)
	assertMetav1TimeEqual(t, &created, stored.Status.LastBackupTime)
	assert.Len(t, listBackups(t, base).Items, 1)
}

func TestSchedulingFirstObservationRecoversAfterDestinationEdit(t *testing.T) {
	now := time.Date(2026, 8, 20, 3, 15, 0, 0, time.UTC)
	scheduled := time.Date(2026, 8, 20, 2, 0, 0, 0, time.UTC)
	schedule := validSchedule()
	historical, err := RenderBackup(schedule, scheduled)
	require.NoError(t, err)
	created := metav1.NewTime(scheduled.Add(time.Minute))
	historical.CreationTimestamp = created
	schedule.Spec.BackupTemplate.S3.Bucket = "replacement-bucket"
	schedule.Spec.BackupTemplate.S3.Prefix = "replacement-prefix"
	require.NoError(t, ValidateBackupSchedule(schedule))

	base := newSchedulingFakeClient(t, schedule, historical)
	reconciler := NewSchedulingReconciler(base, base, staticClock{now: now}, NewTargetLocks())

	_, err = reconciler.Reconcile(context.Background(), requestFor(schedule))
	require.NoError(t, err)
	stored := getSchedule(t, base, schedule)
	require.NotNil(t, stored.Status.LastScheduleTime)
	assert.True(t, stored.Status.LastScheduleTime.Time.Equal(scheduled))
	assert.Equal(t, historical.Name, stored.Status.LastBackup)
	assertMetav1TimeEqual(t, &created, stored.Status.LastBackupTime)
	assert.Len(t, listBackups(t, base).Items, 1)
}

func TestSchedulingCreatesNewestDueBackupAndUsesServerCreationTimestamp(t *testing.T) {
	cursor := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	now := cursor.Add(90 * time.Minute)
	created := metav1.NewTime(now.Add(5 * time.Second))
	schedule := validSchedule()
	schedule.Status.LastScheduleTime = timePointer(cursor)
	schedule.Status.LastBackup = "prior"
	schedule.Status.LastBackupTime = timePointer(cursor.Add(-time.Hour))
	base := newSchedulingFakeClient(t, schedule)
	managerClient := &timestampingClient{Client: base, creationTime: created}
	reconciler := NewSchedulingReconciler(managerClient, base, staticClock{now: now}, NewTargetLocks())

	result, err := reconciler.Reconcile(context.Background(), requestFor(schedule))
	require.NoError(t, err)
	assert.Equal(t, 30*time.Minute, result.RequeueAfter)

	backups := listBackups(t, base)
	require.Len(t, backups.Items, 1)
	backup := &backups.Items[0]
	assert.Equal(t, BackupName(schedule.Name, schedule.UID, cursor.Add(time.Hour)), backup.Name)
	stored := getSchedule(t, base, schedule)
	assert.True(t, stored.Status.LastScheduleTime.Time.Equal(cursor.Add(time.Hour)))
	assert.Equal(t, backup.Name, stored.Status.LastBackup)
	assertMetav1TimeEqual(t, &created, stored.Status.LastBackupTime)
}

func TestSchedulingCreatesOnlyNewestOfSeveralDueOccurrences(t *testing.T) {
	cursor := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	now := cursor.Add(3*time.Hour + 30*time.Minute)
	schedule := validSchedule()
	schedule.Status.LastScheduleTime = timePointer(cursor)
	base := newSchedulingFakeClient(t, schedule)
	managerClient := &timestampingClient{Client: base, creationTime: metav1.NewTime(now)}
	reconciler := NewSchedulingReconciler(managerClient, base, staticClock{now: now}, NewTargetLocks())

	_, err := reconciler.Reconcile(context.Background(), requestFor(schedule))
	require.NoError(t, err)
	backups := listBackups(t, base)
	require.Len(t, backups.Items, 1)
	latest := cursor.Add(3 * time.Hour)
	assert.Equal(t, BackupName(schedule.Name, schedule.UID, latest), backups.Items[0].Name)
	stored := getSchedule(t, base, schedule)
	assert.True(t, stored.Status.LastScheduleTime.Time.Equal(latest))
}

func TestSchedulingClockRollbackCreatesNothing(t *testing.T) {
	cursor := time.Date(2026, 8, 20, 2, 0, 0, 0, time.UTC)
	now := cursor.Add(-time.Hour)
	schedule := validSchedule()
	schedule.Status.LastScheduleTime = timePointer(cursor)
	schedule.Status.LastBackup = "prior"
	base := newSchedulingFakeClient(t, schedule)
	reconciler := NewSchedulingReconciler(base, base, staticClock{now: now}, NewTargetLocks())

	result, err := reconciler.Reconcile(context.Background(), requestFor(schedule))
	require.NoError(t, err)
	assert.Equal(t, 2*time.Hour, result.RequeueAfter)
	stored := getSchedule(t, base, schedule)
	assert.True(t, stored.Status.LastScheduleTime.Time.Equal(cursor))
	assert.Equal(t, "prior", stored.Status.LastBackup)
	assert.Empty(t, listBackups(t, base).Items)
}

func TestSchedulingEveryWakePreservesCursorAnchor(t *testing.T) {
	cursor := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	now := cursor.Add(5 * time.Minute)
	schedule := validSchedule()
	schedule.Spec.Schedule = "@every 10m"
	schedule.Status.LastScheduleTime = timePointer(cursor)
	base := newSchedulingFakeClient(t, schedule)
	reconciler := NewSchedulingReconciler(base, base, staticClock{now: now}, NewTargetLocks())

	result, err := reconciler.Reconcile(context.Background(), requestFor(schedule))
	require.NoError(t, err)
	assert.Equal(t, 5*time.Minute, result.RequeueAfter)
	stored := getSchedule(t, base, schedule)
	assert.True(t, stored.Status.LastScheduleTime.Time.Equal(cursor))
	assert.Empty(t, listBackups(t, base).Items)
}

func TestSchedulingScheduleEditKeepsCursorAndUsesNewExpression(t *testing.T) {
	cursor := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	now := cursor.Add(time.Hour + 45*time.Minute)
	schedule := validSchedule()
	schedule.Generation = 2
	schedule.Spec.Schedule = "30 * * * *"
	schedule.Status.LastScheduleTime = timePointer(cursor)
	base := newSchedulingFakeClient(t, schedule)
	managerClient := &timestampingClient{Client: base, creationTime: metav1.NewTime(now)}
	reconciler := NewSchedulingReconciler(managerClient, base, staticClock{now: now}, NewTargetLocks())

	_, err := reconciler.Reconcile(context.Background(), requestFor(schedule))
	require.NoError(t, err)
	latest := time.Date(2026, 8, 20, 1, 30, 0, 0, time.UTC)
	stored := getSchedule(t, base, schedule)
	assert.True(t, stored.Status.LastScheduleTime.Time.Equal(latest))
	backups := listBackups(t, base)
	require.Len(t, backups.Items, 1)
	assert.Equal(t, BackupName(schedule.Name, schedule.UID, latest), backups.Items[0].Name)
}

func TestSchedulingPauseConsumesDueAndPreservesLastBackup(t *testing.T) {
	cursor := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	now := cursor.Add(2*time.Hour + 30*time.Minute)
	schedule := validSchedule()
	schedule.Spec.Pause = true
	schedule.Status.LastScheduleTime = timePointer(cursor)
	schedule.Status.LastBackup = "prior"
	schedule.Status.LastBackupTime = timePointer(cursor.Add(-time.Hour))
	wantBackupTime := schedule.Status.LastBackupTime.DeepCopy()
	base := newSchedulingFakeClient(t, schedule)
	reconciler := NewSchedulingReconciler(base, base, staticClock{now: now}, NewTargetLocks())

	_, err := reconciler.Reconcile(context.Background(), requestFor(schedule))
	require.NoError(t, err)
	stored := getSchedule(t, base, schedule)
	assert.True(t, stored.Status.LastScheduleTime.Time.Equal(cursor.Add(2*time.Hour)))
	assert.Equal(t, "prior", stored.Status.LastBackup)
	assertMetav1TimeEqual(t, wantBackupTime, stored.Status.LastBackupTime)
	assert.Empty(t, listBackups(t, base).Items)
}

func TestSchedulingOverlapPreservesCursorAndLastBackup(t *testing.T) {
	cursor := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	now := cursor.Add(90 * time.Minute)
	schedule := validSchedule()
	schedule.Status.LastScheduleTime = timePointer(cursor)
	schedule.Status.LastBackup = "prior"
	schedule.Status.LastBackupTime = timePointer(cursor.Add(-time.Hour))
	wantBackupTime := schedule.Status.LastBackupTime.DeepCopy()
	manual := backupForTarget("manual", "backups", "", "example")
	base := newSchedulingFakeClient(t, schedule, manual)
	reconciler := NewSchedulingReconciler(base, base, staticClock{now: now}, NewTargetLocks())

	_, err := reconciler.Reconcile(context.Background(), requestFor(schedule))
	require.NoError(t, err)
	stored := getSchedule(t, base, schedule)
	assert.True(t, stored.Status.LastScheduleTime.Time.Equal(cursor))
	assert.Equal(t, "prior", stored.Status.LastBackup)
	assertMetav1TimeEqual(t, wantBackupTime, stored.Status.LastBackupTime)
	assert.Len(t, listBackups(t, base).Items, 1)
	assertSchedulingReady(t, stored, metav1.ConditionTrue, ReasonReconciled)
}

func TestSchedulingRecoversDesiredBeforeItChecksOverlap(t *testing.T) {
	cursor := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	due := cursor.Add(time.Hour)
	now := due.Add(30 * time.Minute)
	schedule := validSchedule()
	schedule.Status.LastScheduleTime = timePointer(cursor)
	desired, err := RenderBackup(schedule, due)
	require.NoError(t, err)
	created := metav1.NewTime(due.Add(9 * time.Second))
	desired.CreationTimestamp = created
	base := newSchedulingFakeClient(t, schedule, desired)
	reconciler := NewSchedulingReconciler(base, base, staticClock{now: now}, NewTargetLocks())

	_, err = reconciler.Reconcile(context.Background(), requestFor(schedule))
	require.NoError(t, err)
	stored := getSchedule(t, base, schedule)
	assert.True(t, stored.Status.LastScheduleTime.Time.Equal(due))
	assert.Equal(t, desired.Name, stored.Status.LastBackup)
	assertMetav1TimeEqual(t, &created, stored.Status.LastBackupTime)
	assert.Len(t, listBackups(t, base).Items, 1)
}

func TestSchedulingRejectsForeignDeterministicNameCollision(t *testing.T) {
	cursor := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	due := cursor.Add(time.Hour)
	schedule := validSchedule()
	schedule.Status.LastScheduleTime = timePointer(cursor)
	collision, err := RenderBackup(schedule, due)
	require.NoError(t, err)
	collision.Labels[ScheduleUIDLabel] = "foreign"
	base := newSchedulingFakeClient(t, schedule, collision)
	reconciler := NewSchedulingReconciler(base, base, staticClock{now: due.Add(30 * time.Minute)}, NewTargetLocks())

	_, err = reconciler.Reconcile(context.Background(), requestFor(schedule))
	require.ErrorContains(t, err, "conflicting identity")
	stored := getSchedule(t, base, schedule)
	assert.True(t, stored.Status.LastScheduleTime.Time.Equal(cursor))
	assertSchedulingReady(t, stored, metav1.ConditionFalse, ReasonReconcilerError)
	backups := listBackups(t, base)
	require.Len(t, backups.Items, 1)
	assert.Equal(t, "foreign", backups.Items[0].Labels[ScheduleUIDLabel])
}

func TestSchedulingRejectsCurrentRenderTargetMismatch(t *testing.T) {
	cursor := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	due := cursor.Add(time.Hour)
	schedule := validSchedule()
	schedule.Status.LastScheduleTime = timePointer(cursor)
	collision, err := RenderBackup(schedule, due)
	require.NoError(t, err)
	collision.Spec.BR.Cluster = "old-target"
	_, err = ValidateManagedBackup(schedule, collision)
	require.Error(t, err)
	base := newSchedulingFakeClient(t, schedule, collision)
	reconciler := NewSchedulingReconciler(base, base, staticClock{now: due.Add(30 * time.Minute)}, NewTargetLocks())

	_, err = reconciler.Reconcile(context.Background(), requestFor(schedule))
	require.ErrorContains(t, err, "does not match schedule target")
	stored := getSchedule(t, base, schedule)
	assert.True(t, stored.Status.LastScheduleTime.Time.Equal(cursor))
	assertSchedulingReady(t, stored, metav1.ConditionFalse, ReasonReconcilerError)
}

func TestSchedulingRecoversExactConcurrentAlreadyExists(t *testing.T) {
	cursor := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	due := cursor.Add(time.Hour)
	now := due.Add(30 * time.Minute)
	created := metav1.NewTime(due.Add(13 * time.Second))
	schedule := validSchedule()
	schedule.Status.LastScheduleTime = timePointer(cursor)
	base := newSchedulingFakeClient(t, schedule)
	managerClient := &alreadyExistsCreateClient{Client: base, creationTime: created}
	reconciler := NewSchedulingReconciler(managerClient, base, staticClock{now: now}, NewTargetLocks())

	_, err := reconciler.Reconcile(context.Background(), requestFor(schedule))
	require.NoError(t, err)
	stored := getSchedule(t, base, schedule)
	assert.True(t, stored.Status.LastScheduleTime.Time.Equal(due))
	assertMetav1TimeEqual(t, &created, stored.Status.LastBackupTime)
	assert.Len(t, listBackups(t, base).Items, 1)
}

func TestSchedulingFinalAPIReadCatchesCachedMiss(t *testing.T) {
	cursor := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	now := cursor.Add(90 * time.Minute)
	schedule := validSchedule()
	schedule.Status.LastScheduleTime = timePointer(cursor)
	manual := backupForTarget("manual", "backups", "", "example")
	live := newSchedulingFakeClient(t, schedule, manual)
	stale := newSchedulingFakeClient(t)
	managerClient := &splitListClient{Client: live, listReader: stale}
	reconciler := NewSchedulingReconciler(managerClient, live, staticClock{now: now}, NewTargetLocks())

	_, err := reconciler.Reconcile(context.Background(), requestFor(schedule))
	require.NoError(t, err)
	stored := getSchedule(t, live, schedule)
	assert.True(t, stored.Status.LastScheduleTime.Time.Equal(cursor))
	assert.Len(t, listBackups(t, live).Items, 1)
}

func TestSchedulingSharedTargetLockAllowsOnlyOneConcurrentSchedule(t *testing.T) {
	cursor := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	due := cursor.Add(time.Hour)
	now := due.Add(30 * time.Minute)
	first := validSchedule()
	first.Name = "first"
	first.Status.LastScheduleTime = timePointer(cursor)
	second := validSchedule()
	second.Name = "second"
	second.UID = "22222222-3333-4444-5555-666666666666"
	second.Status.LastScheduleTime = timePointer(cursor)
	live := newSchedulingFakeClient(t, first, second)
	managerClient := &timestampingClient{Client: live, creationTime: metav1.NewTime(now)}
	locks := NewTargetLocks()
	firstReconciler := NewSchedulingReconciler(managerClient, live, staticClock{now: now}, locks)
	secondReconciler := NewSchedulingReconciler(managerClient, live, staticClock{now: now}, locks)

	start := make(chan struct{})
	results := make(chan error, 2)
	var workers sync.WaitGroup
	for _, item := range []struct {
		reconciler *SchedulingReconciler
		schedule   *brv1alpha1.BackupSchedule
	}{{firstReconciler, first}, {secondReconciler, second}} {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			_, err := item.reconciler.Reconcile(context.Background(), requestFor(item.schedule))
			results <- err
		}()
	}
	close(start)
	workers.Wait()
	close(results)
	for err := range results {
		require.NoError(t, err)
	}

	assert.Len(t, listBackups(t, live).Items, 1)
	advanced := 0
	for _, schedule := range []*brv1alpha1.BackupSchedule{first, second} {
		stored := getSchedule(t, live, schedule)
		if stored.Status.LastScheduleTime.Time.Equal(due) {
			advanced++
		} else {
			assert.True(t, stored.Status.LastScheduleTime.Time.Equal(cursor))
		}
	}
	assert.Equal(t, 1, advanced)
}

func TestSchedulingStatusFailureLeavesCursorAndRetryRecovers(t *testing.T) {
	cursor := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	due := cursor.Add(time.Hour)
	now := due.Add(30 * time.Minute)
	created := metav1.NewTime(due.Add(11 * time.Second))
	schedule := validSchedule()
	schedule.Status.LastScheduleTime = timePointer(cursor)
	base := newSchedulingFakeClient(t, schedule)
	managerClient := &timestampingClient{Client: base, creationTime: created}
	reconciler := NewSchedulingReconciler(managerClient, base, staticClock{now: now}, NewTargetLocks())
	reconciler.StatusUpdater = &failOnceStatusUpdater{delegate: NewStatusUpdater(base, managerClient)}

	_, err := reconciler.Reconcile(context.Background(), requestFor(schedule))
	require.ErrorContains(t, err, "injected status failure")
	stored := getSchedule(t, base, schedule)
	assert.True(t, stored.Status.LastScheduleTime.Time.Equal(cursor))
	assert.Len(t, listBackups(t, base).Items, 1)
	stored.Spec.BackupTemplate.S3.Bucket = "replacement-bucket"
	stored.Spec.BackupTemplate.S3.Prefix = "replacement-prefix"
	stored.Generation++
	require.NoError(t, base.Update(context.Background(), stored))

	_, err = reconciler.Reconcile(context.Background(), requestFor(schedule))
	require.NoError(t, err)
	stored = getSchedule(t, base, schedule)
	assert.True(t, stored.Status.LastScheduleTime.Time.Equal(due))
	assertMetav1TimeEqual(t, &created, stored.Status.LastBackupTime)
	assert.Len(t, listBackups(t, base).Items, 1)

	backups := listBackups(t, base)
	first := &backups.Items[0]
	first.Status.Conditions = []metav1.Condition{{
		Type:   string(brv1alpha1.BackupComplete),
		Status: metav1.ConditionTrue,
	}}
	require.NoError(t, base.Update(context.Background(), first))

	nextDue := due.Add(time.Hour)
	reconciler = NewSchedulingReconciler(
		managerClient,
		base,
		staticClock{now: nextDue.Add(30 * time.Minute)},
		NewTargetLocks(),
	)
	_, err = reconciler.Reconcile(context.Background(), requestFor(schedule))
	require.NoError(t, err)
	backups = listBackups(t, base)
	require.Len(t, backups.Items, 2)
	for i := range backups.Items {
		backup := &backups.Items[i]
		if backup.Name != BackupName(schedule.Name, schedule.UID, nextDue) {
			continue
		}
		require.NotNil(t, backup.Spec.S3)
		assert.Equal(t, "replacement-bucket", backup.Spec.S3.Bucket)
		assert.Equal(t, "replacement-prefix/"+backup.Name, backup.Spec.S3.Prefix)
		return
	}
	t.Fatal("next occurrence Backup was not created")
}

func TestSchedulingBoundedBatchAdvancesWithoutCreating(t *testing.T) {
	now := time.Date(2026, 8, 20, 20, 0, 0, 0, time.UTC)
	cursor := now.Add(-1001 * time.Minute)
	schedule := validSchedule()
	schedule.Spec.Schedule = "@every 1m"
	schedule.Status.LastScheduleTime = timePointer(cursor)
	schedule.Status.LastBackup = "prior"
	schedule.Status.LastBackupTime = timePointer(cursor.Add(-time.Minute))
	wantBackupTime := schedule.Status.LastBackupTime.DeepCopy()
	base := newSchedulingFakeClient(t, schedule)
	reconciler := NewSchedulingReconciler(base, base, staticClock{now: now}, NewTargetLocks())

	result, err := reconciler.Reconcile(context.Background(), requestFor(schedule))
	require.NoError(t, err)
	assert.True(t, result.Requeue)
	stored := getSchedule(t, base, schedule)
	assert.True(t, stored.Status.LastScheduleTime.Time.Equal(cursor.Add(1000*time.Minute)))
	assert.Equal(t, "prior", stored.Status.LastBackup)
	assertMetav1TimeEqual(t, wantBackupTime, stored.Status.LastBackupTime)
	assert.Empty(t, listBackups(t, base).Items)
}

func TestSchedulingBoundedBatchAPIReaderBlockerPreservesCursor(t *testing.T) {
	now := time.Date(2026, 8, 20, 20, 0, 0, 0, time.UTC)
	cursor := now.Add(-1001 * time.Minute)
	schedule := validSchedule()
	schedule.Spec.Schedule = "@every 1m"
	schedule.Status.LastScheduleTime = timePointer(cursor)
	schedule.Status.LastBackup = "prior"
	schedule.Status.LastBackupTime = timePointer(cursor.Add(-time.Minute))
	wantBackupTime := schedule.Status.LastBackupTime.DeepCopy()
	manual := backupForTarget("manual", "backups", "", "example")
	live := newSchedulingFakeClient(t, schedule, manual)
	stale := newSchedulingFakeClient(t)
	managerClient := &splitListClient{Client: live, listReader: stale}
	reconciler := NewSchedulingReconciler(managerClient, live, staticClock{now: now}, NewTargetLocks())

	result, err := reconciler.Reconcile(context.Background(), requestFor(schedule))
	require.NoError(t, err)
	assert.False(t, result.Requeue)
	assert.Positive(t, result.RequeueAfter)
	stored := getSchedule(t, live, schedule)
	assert.True(t, stored.Status.LastScheduleTime.Time.Equal(cursor))
	assert.Equal(t, "prior", stored.Status.LastBackup)
	assertMetav1TimeEqual(t, wantBackupTime, stored.Status.LastBackupTime)
	assert.Len(t, listBackups(t, live).Items, 1)
}

func TestSchedulingInvalidSpecDoesNotAdvanceOrCreate(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*brv1alpha1.BackupSchedule)
	}{
		{name: "malformed cron", mutate: func(schedule *brv1alpha1.BackupSchedule) { schedule.Spec.Schedule = "invalid" }},
		{name: "impossible cron", mutate: func(schedule *brv1alpha1.BackupSchedule) { schedule.Spec.Schedule = "0 0 30 2 *" }},
		{name: "positive maxBackups", mutate: func(schedule *brv1alpha1.BackupSchedule) {
			maxBackups := int32(1)
			schedule.Spec.MaxBackups = &maxBackups
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursor := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
			schedule := validSchedule()
			tt.mutate(schedule)
			schedule.Status.LastScheduleTime = timePointer(cursor)
			base := newSchedulingFakeClient(t, schedule)
			reconciler := NewSchedulingReconciler(base, base, staticClock{now: cursor.Add(90 * time.Minute)}, NewTargetLocks())

			result, err := reconciler.Reconcile(context.Background(), requestFor(schedule))
			require.NoError(t, err)
			assert.True(t, result.IsZero())
			stored := getSchedule(t, base, schedule)
			assert.True(t, stored.Status.LastScheduleTime.Time.Equal(cursor))
			assert.Empty(t, listBackups(t, base).Items)
			assertSchedulingReady(t, stored, metav1.ConditionFalse, ReasonInvalidSpec)
		})
	}
}

func TestSchedulingStatusConflictDoesNotAdvanceCursor(t *testing.T) {
	cursor := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	schedule := validSchedule()
	schedule.Status.LastScheduleTime = timePointer(cursor)
	base := newSchedulingFakeClient(t, schedule)
	reconciler := NewSchedulingReconciler(base, base, staticClock{now: cursor.Add(90 * time.Minute)}, NewTargetLocks())
	reconciler.StatusUpdater = conflictStatusUpdater{}

	_, err := reconciler.Reconcile(context.Background(), requestFor(schedule))
	require.Error(t, err)
	assert.True(t, apierrors.IsConflict(err))
	stored := getSchedule(t, base, schedule)
	assert.True(t, stored.Status.LastScheduleTime.Time.Equal(cursor))
	assert.Len(t, listBackups(t, base).Items, 1, "deterministic Backup remains recoverable after status conflict")
}

type staticClock struct {
	now time.Time
}

func (clock staticClock) Now() time.Time {
	return clock.now
}

type timestampingClient struct {
	client.Client
	creationTime metav1.Time
}

type alreadyExistsCreateClient struct {
	client.Client
	creationTime metav1.Time
}

func (racing *alreadyExistsCreateClient) Create(
	ctx context.Context,
	obj client.Object,
	opts ...client.CreateOption,
) error {
	concurrent := obj.DeepCopyObject().(client.Object)
	concurrent.SetCreationTimestamp(racing.creationTime)
	if err := racing.Client.Create(ctx, concurrent, opts...); err != nil {
		return err
	}
	return apierrors.NewAlreadyExists(
		schema.GroupResource{Group: brv1alpha1.SchemeGroupVersion.Group, Resource: "backups"},
		obj.GetName(),
	)
}

func (timestamping *timestampingClient) Create(
	ctx context.Context,
	obj client.Object,
	opts ...client.CreateOption,
) error {
	obj.SetCreationTimestamp(timestamping.creationTime)
	return timestamping.Client.Create(ctx, obj, opts...)
}

type splitListClient struct {
	client.Client
	listReader client.Reader
}

func (split *splitListClient) List(
	ctx context.Context,
	list client.ObjectList,
	opts ...client.ListOption,
) error {
	return split.listReader.List(ctx, list, opts...)
}

type failOnceStatusUpdater struct {
	delegate ScheduleStatusUpdater
	failed   bool
}

func (updater *failOnceStatusUpdater) Update(
	ctx context.Context,
	key client.ObjectKey,
	uid types.UID,
	generation int64,
	mutate StatusMutation,
) error {
	if !updater.failed {
		updater.failed = true
		return errors.New("injected status failure")
	}
	return updater.delegate.Update(ctx, key, uid, generation, mutate)
}

type conflictStatusUpdater struct{}

func (conflictStatusUpdater) Update(
	context.Context,
	client.ObjectKey,
	types.UID,
	int64,
	StatusMutation,
) error {
	return apierrors.NewConflict(
		schema.GroupResource{Group: brv1alpha1.SchemeGroupVersion.Group, Resource: "backupschedules"},
		"schedule",
		errors.New("injected conflict"),
	)
}

func requestFor(schedule *brv1alpha1.BackupSchedule) ctrl.Request {
	return ctrl.Request{NamespacedName: client.ObjectKeyFromObject(schedule)}
}

func timePointer(value time.Time) *metav1.Time {
	result := metav1.NewTime(value)
	return &result
}

func getSchedule(t *testing.T, reader client.Reader, schedule *brv1alpha1.BackupSchedule) *brv1alpha1.BackupSchedule {
	t.Helper()
	stored := &brv1alpha1.BackupSchedule{}
	require.NoError(t, reader.Get(context.Background(), client.ObjectKeyFromObject(schedule), stored))
	return stored
}

func listBackups(t *testing.T, reader client.Reader) *brv1alpha1.BackupList {
	t.Helper()
	backups := &brv1alpha1.BackupList{}
	require.NoError(t, reader.List(context.Background(), backups))
	return backups
}

func assertSchedulingReady(
	t *testing.T,
	schedule *brv1alpha1.BackupSchedule,
	status metav1.ConditionStatus,
	reason string,
) {
	t.Helper()
	condition := apiMeta.FindStatusCondition(schedule.Status.Conditions, ConditionSchedulingReady)
	require.NotNil(t, condition)
	assert.Equal(t, status, condition.Status)
	assert.Equal(t, reason, condition.Reason)
	assert.Equal(t, schedule.Generation, condition.ObservedGeneration)
}

func assertMetav1TimeEqual(t *testing.T, expected, actual *metav1.Time) {
	t.Helper()
	require.NotNil(t, expected)
	require.NotNil(t, actual)
	assert.True(t, actual.Time.Equal(expected.Time), "expected %s, got %s", expected.Time, actual.Time)
}
