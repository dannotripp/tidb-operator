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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	brv1alpha1 "github.com/pingcap/tidb-operator/api/v2/br/v1alpha1"
	metav1alpha1 "github.com/pingcap/tidb-operator/api/v2/meta/v1alpha1"
)

func TestRetentionReconcilerDeletesOnlyOldestFromFreshPlan(t *testing.T) {
	schedule := validSchedule()
	schedule.Generation = 4
	maxBackups := int32(1)
	schedule.Spec.MaxBackups = &maxBackups
	oldest := retentionTestBackup(t, schedule, 1, BackupSucceeded)
	middle := retentionTestBackup(t, schedule, 2, BackupSucceeded)
	newest := retentionTestBackup(t, schedule, 3, BackupSucceeded)
	backups := []brv1alpha1.Backup{newest, oldest, middle}

	listCalls := 0
	reader := retentionStateReader(t, schedule, backups, &listCalls)
	deleted := make([]string, 0, 1)
	writer := &retentionTestWriter{delete: func(_ context.Context, obj client.Object, _ ...client.DeleteOption) error {
		deleted = append(deleted, obj.GetName())
		return nil
	}}
	status := &retentionRecordingStatus{schedule: schedule}
	reconciler := &RetentionReconciler{
		Reader: reader,
		Writer: writer,
		Status: status,
		Now: func() time.Time {
			return time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
		},
	}

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKeyFromObject(schedule),
	})
	require.NoError(t, err)
	assert.True(t, result.Requeue)
	assert.Equal(t, 1, listCalls)
	assert.Equal(t, []string{oldest.Name}, deleted)
	require.NotNil(t, status.condition)
	assert.Equal(t, metav1.ConditionTrue, status.condition.Status)
	assert.Equal(t, ReasonReconciled, status.condition.Reason)
}

func TestRetentionReconcilerNoOpControls(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*brv1alpha1.BackupSchedule)
	}{
		{name: "paused", mutate: func(schedule *brv1alpha1.BackupSchedule) {
			value := int32(1)
			schedule.Spec.MaxBackups = &value
			schedule.Spec.Pause = true
		}},
		{name: "nil maxBackups", mutate: func(schedule *brv1alpha1.BackupSchedule) {
			schedule.Spec.MaxBackups = nil
		}},
		{name: "zero maxBackups", mutate: func(schedule *brv1alpha1.BackupSchedule) {
			value := int32(0)
			schedule.Spec.MaxBackups = &value
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schedule := validSchedule()
			schedule.Generation = 2
			tt.mutate(schedule)
			listCalls := 0
			reader := &retentionTestReader{
				get: func(_ context.Context, _ client.ObjectKey, obj client.Object) error {
					*obj.(*brv1alpha1.BackupSchedule) = *schedule.DeepCopy()
					return nil
				},
				list: func(context.Context, client.ObjectList, ...client.ListOption) error {
					listCalls++
					return nil
				},
			}
			deletes := 0
			status := &retentionRecordingStatus{schedule: schedule}
			reconciler := &RetentionReconciler{
				Reader: reader,
				Writer: &retentionTestWriter{delete: func(context.Context, client.Object, ...client.DeleteOption) error {
					deletes++
					return nil
				}},
				Status: status,
			}

			result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
				NamespacedName: client.ObjectKeyFromObject(schedule),
			})
			require.NoError(t, err)
			assert.False(t, result.Requeue)
			assert.Equal(t, 0, listCalls)
			assert.Equal(t, 0, deletes)
			require.NotNil(t, status.condition)
			assert.Equal(t, metav1.ConditionTrue, status.condition.Status)
			assert.Equal(t, ReasonReconciled, status.condition.Reason)
		})
	}
}

func TestRetentionReconcilerInvalidSpecHasNoSideEffects(t *testing.T) {
	schedule := validSchedule()
	schedule.Generation = 2
	schedule.Spec.BackupTemplate.BR.ClusterNamespace = "other"
	reader := &retentionTestReader{get: func(_ context.Context, _ client.ObjectKey, obj client.Object) error {
		*obj.(*brv1alpha1.BackupSchedule) = *schedule.DeepCopy()
		return nil
	}}
	status := &retentionRecordingStatus{schedule: schedule}
	reconciler := &RetentionReconciler{
		Reader: reader,
		Writer: &retentionTestWriter{},
		Status: status,
	}

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKeyFromObject(schedule),
	})
	require.NoError(t, err)
	assert.False(t, result.Requeue)
	require.NotNil(t, status.condition)
	assert.Equal(t, metav1.ConditionFalse, status.condition.Status)
	assert.Equal(t, ReasonInvalidSpec, status.condition.Reason)
}

func TestRetentionReconcilerMalformedManagedBackupFailsClosed(t *testing.T) {
	schedule := validSchedule()
	schedule.Generation = 2
	maxBackups := int32(1)
	schedule.Spec.MaxBackups = &maxBackups
	malformed := retentionTestBackup(t, schedule, 1, BackupSucceeded)
	delete(malformed.Annotations, ScheduledTimeAnnotation)
	listCalls := 0
	reader := retentionStateReader(t, schedule, []brv1alpha1.Backup{malformed}, &listCalls)
	deletes := 0
	status := &retentionRecordingStatus{schedule: schedule}
	reconciler := &RetentionReconciler{
		Reader: reader,
		Writer: &retentionTestWriter{delete: func(context.Context, client.Object, ...client.DeleteOption) error {
			deletes++
			return nil
		}},
		Status: status,
	}

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKeyFromObject(schedule),
	})
	require.NoError(t, err)
	assert.False(t, result.Requeue)
	assert.Equal(t, 1, listCalls)
	assert.Equal(t, 0, deletes)
	require.NotNil(t, status.condition)
	assert.Equal(t, metav1.ConditionFalse, status.condition.Status)
	assert.Equal(t, ReasonReconcilerError, status.condition.Reason)
}

func TestRetentionReconcilerStaleCandidateRequeuesWithoutDelete(t *testing.T) {
	schedule := validSchedule()
	schedule.Generation = 2
	maxBackups := int32(1)
	schedule.Spec.MaxBackups = &maxBackups
	oldest := retentionTestBackup(t, schedule, 1, BackupSucceeded)
	newest := retentionTestBackup(t, schedule, 2, BackupSucceeded)

	reader := &retentionTestReader{
		get: func(_ context.Context, key client.ObjectKey, obj client.Object) error {
			switch typed := obj.(type) {
			case *brv1alpha1.BackupSchedule:
				*typed = *schedule.DeepCopy()
			case *brv1alpha1.Backup:
				if key.Name != oldest.Name {
					return errors.New("unexpected Backup get")
				}
				live := oldest.DeepCopy()
				live.ResourceVersion = "new-version"
				*typed = *live
			}
			return nil
		},
		list: func(_ context.Context, list client.ObjectList, _ ...client.ListOption) error {
			list.(*brv1alpha1.BackupList).Items = []brv1alpha1.Backup{oldest, newest}
			return nil
		},
	}
	deletes := 0
	status := &retentionRecordingStatus{schedule: schedule}
	reconciler := &RetentionReconciler{
		Reader: reader,
		Writer: &retentionTestWriter{delete: func(context.Context, client.Object, ...client.DeleteOption) error {
			deletes++
			return nil
		}},
		Status: status,
	}

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKeyFromObject(schedule),
	})
	require.NoError(t, err)
	assert.True(t, result.Requeue)
	assert.Equal(t, 0, deletes)
	assert.Equal(t, 0, status.calls)
}

func TestRetentionReconcilerNeverTouchesTerminatingInvalidFinalizer(t *testing.T) {
	schedule := validSchedule()
	schedule.Generation = 2
	maxBackups := int32(1)
	schedule.Spec.MaxBackups = &maxBackups

	backups := make([]brv1alpha1.Backup, 0, maxRetainedUnsuccessfulBackups+1)
	for hour := 1; hour <= maxRetainedUnsuccessfulBackups+1; hour++ {
		backup := retentionTestBackup(t, schedule, hour, BackupInvalid)
		backup.Spec.CleanPolicy = brv1alpha1.CleanPolicyTypeDelete
		backups = append(backups, backup)
	}
	terminating := &backups[0]
	deletionTime := metav1.NewTime(time.Date(2026, 8, 20, 8, 0, 0, 0, time.UTC))
	terminating.DeletionTimestamp = &deletionTime
	terminating.Finalizers = []string{metav1alpha1.Finalizer}
	original := terminating.DeepCopy()

	listCalls := 0
	reader := retentionStateReader(t, schedule, backups, &listCalls)
	deletes := 0
	updates := 0
	patches := 0
	status := &retentionRecordingStatus{schedule: schedule}
	reconciler := &RetentionReconciler{
		Reader: reader,
		Writer: &retentionTestWriter{
			delete: func(context.Context, client.Object, ...client.DeleteOption) error {
				deletes++
				return nil
			},
			update: func(context.Context, client.Object, ...client.UpdateOption) error {
				updates++
				return nil
			},
			patch: func(context.Context, client.Object, client.Patch, ...client.PatchOption) error {
				patches++
				return nil
			},
		},
		Status: status,
	}

	result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKeyFromObject(schedule),
	})
	require.NoError(t, err)
	assert.False(t, result.Requeue)
	assert.Equal(t, 1, listCalls)
	assert.Equal(t, 0, deletes)
	assert.Equal(t, 0, updates)
	assert.Equal(t, 0, patches)
	assert.Equal(t, original.Finalizers, terminating.Finalizers)
	assert.Equal(t, original.Spec.CleanPolicy, terminating.Spec.CleanPolicy)
	require.NotNil(t, status.condition)
	assert.Equal(t, metav1.ConditionTrue, status.condition.Status)
}

func TestRetentionReconcilerReplansWhenKeeperTerminatesBetweenPasses(t *testing.T) {
	schedule := validSchedule()
	schedule.Generation = 2
	maxBackups := int32(1)
	schedule.Spec.MaxBackups = &maxBackups
	oldest := retentionTestBackup(t, schedule, 1, BackupSucceeded)
	middle := retentionTestBackup(t, schedule, 2, BackupSucceeded)
	keeper := retentionTestBackup(t, schedule, 3, BackupSucceeded)
	backups := []brv1alpha1.Backup{oldest, middle, keeper}

	listCalls := 0
	reader := &retentionTestReader{
		get: func(_ context.Context, key client.ObjectKey, obj client.Object) error {
			switch typed := obj.(type) {
			case *brv1alpha1.BackupSchedule:
				*typed = *schedule.DeepCopy()
				return nil
			case *brv1alpha1.Backup:
				for i := range backups {
					if client.ObjectKeyFromObject(&backups[i]) == key {
						*typed = *backups[i].DeepCopy()
						return nil
					}
				}
				return errors.New("Backup not found")
			default:
				return errors.New("unexpected object type")
			}
		},
		list: func(_ context.Context, list client.ObjectList, _ ...client.ListOption) error {
			listCalls++
			list.(*brv1alpha1.BackupList).Items = append([]brv1alpha1.Backup(nil), backups...)
			return nil
		},
	}
	deleted := make([]string, 0, 2)
	writer := &retentionTestWriter{delete: func(_ context.Context, obj client.Object, _ ...client.DeleteOption) error {
		deleted = append(deleted, obj.GetName())
		for i := range backups {
			if backups[i].Name == obj.GetName() {
				backups = append(backups[:i], backups[i+1:]...)
				break
			}
		}
		return nil
	}}
	status := &retentionRecordingStatus{schedule: schedule}
	reconciler := &RetentionReconciler{Reader: reader, Writer: writer, Status: status}
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(schedule)}

	first, err := reconciler.Reconcile(context.Background(), request)
	require.NoError(t, err)
	assert.True(t, first.Requeue)
	assert.Equal(t, []string{oldest.Name}, deleted)

	for i := range backups {
		if backups[i].Name == keeper.Name {
			deletionTime := metav1.NewTime(time.Date(2026, 8, 20, 9, 0, 0, 0, time.UTC))
			backups[i].DeletionTimestamp = &deletionTime
		}
	}
	second, err := reconciler.Reconcile(context.Background(), request)
	require.NoError(t, err)
	assert.False(t, second.Requeue)
	assert.Equal(t, 2, listCalls)
	assert.Equal(t, []string{oldest.Name}, deleted)
}

func TestRetentionReconcilerAPIFailureStopsAndReports(t *testing.T) {
	schedule := validSchedule()
	schedule.Generation = 2
	maxBackups := int32(1)
	schedule.Spec.MaxBackups = &maxBackups
	wantErr := errors.New("list unavailable")
	reader := &retentionTestReader{
		get: func(_ context.Context, _ client.ObjectKey, obj client.Object) error {
			*obj.(*brv1alpha1.BackupSchedule) = *schedule.DeepCopy()
			return nil
		},
		list: func(context.Context, client.ObjectList, ...client.ListOption) error {
			return wantErr
		},
	}
	status := &retentionRecordingStatus{schedule: schedule}
	reconciler := &RetentionReconciler{
		Reader: reader,
		Writer: &retentionTestWriter{},
		Status: status,
	}

	_, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKeyFromObject(schedule),
	})
	require.Error(t, err)
	require.ErrorIs(t, err, wantErr)
	require.NotNil(t, status.condition)
	assert.Equal(t, metav1.ConditionFalse, status.condition.Status)
	assert.Equal(t, ReasonReconcilerError, status.condition.Reason)
}

func retentionStateReader(
	t *testing.T,
	schedule *brv1alpha1.BackupSchedule,
	backups []brv1alpha1.Backup,
	listCalls *int,
) *retentionTestReader {
	t.Helper()
	return &retentionTestReader{
		get: func(_ context.Context, key client.ObjectKey, obj client.Object) error {
			switch typed := obj.(type) {
			case *brv1alpha1.BackupSchedule:
				*typed = *schedule.DeepCopy()
				return nil
			case *brv1alpha1.Backup:
				for i := range backups {
					if client.ObjectKeyFromObject(&backups[i]) == key {
						*typed = *backups[i].DeepCopy()
						return nil
					}
				}
				return errors.New("Backup not found")
			default:
				return errors.New("unexpected object type")
			}
		},
		list: func(_ context.Context, list client.ObjectList, opts ...client.ListOption) error {
			*listCalls++
			options := &client.ListOptions{}
			options.ApplyOptions(opts)
			assert.Equal(t, schedule.Namespace, options.Namespace)
			require.NotNil(t, options.LabelSelector)
			assert.True(t, options.LabelSelector.Matches(mapLabels{
				ScheduleUIDLabel: string(schedule.UID),
			}))
			list.(*brv1alpha1.BackupList).Items = append([]brv1alpha1.Backup(nil), backups...)
			return nil
		},
	}
}

// mapLabels is the smallest labels.Label implementation needed by the selector
// assertion above.
type mapLabels map[string]string

func (labels mapLabels) Has(label string) bool {
	_, ok := labels[label]
	return ok
}

func (labels mapLabels) Get(label string) string {
	return labels[label]
}

type retentionRecordingStatus struct {
	schedule  *brv1alpha1.BackupSchedule
	condition *metav1.Condition
	calls     int
}

func (status *retentionRecordingStatus) Update(
	_ context.Context,
	_ client.ObjectKey,
	expectedUID types.UID,
	expectedGeneration int64,
	mutate StatusMutation,
) error {
	status.calls++
	if status.schedule.UID != expectedUID || status.schedule.Generation != expectedGeneration {
		return errors.New("unexpected status identity")
	}
	latest := status.schedule.DeepCopy()
	if _, err := mutate(latest); err != nil {
		return err
	}
	condition := apiMeta.FindStatusCondition(latest.Status.Conditions, ConditionRetentionReady)
	if condition != nil {
		status.condition = condition.DeepCopy()
	}
	return nil
}
