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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	brv1alpha1 "github.com/pingcap/tidb-operator/api/v2/br/v1alpha1"
)

func TestDeleteRetentionCandidateUsesUIDAndResourceVersionPreconditions(t *testing.T) {
	schedule, candidate := retentionDeleteFixture(t)
	candidate.Backup.Spec.CleanPolicy = brv1alpha1.CleanPolicyTypeDelete
	candidate.Backup.Finalizers = []string{"example.com/protect"}
	live := candidate.Backup.DeepCopy()
	operations := make([]string, 0, 3)

	reader := &retentionTestReader{get: func(_ context.Context, _ client.ObjectKey, obj client.Object) error {
		switch typed := obj.(type) {
		case *brv1alpha1.Backup:
			operations = append(operations, "get-backup")
			*typed = *live.DeepCopy()
		case *brv1alpha1.BackupSchedule:
			operations = append(operations, "get-schedule")
			*typed = *schedule.DeepCopy()
		default:
			return errors.New("unexpected object type")
		}
		return nil
	}}

	deletes := 0
	writer := &retentionTestWriter{delete: func(_ context.Context, obj client.Object, opts ...client.DeleteOption) error {
		deletes++
		operations = append(operations, "delete-backup")
		backup := obj.(*brv1alpha1.Backup)
		assert.Equal(t, brv1alpha1.CleanPolicyTypeDelete, backup.Spec.CleanPolicy)
		assert.Equal(t, []string{"example.com/protect"}, backup.Finalizers)

		options := &client.DeleteOptions{}
		options.ApplyOptions(opts)
		require.NotNil(t, options.Preconditions)
		require.NotNil(t, options.Preconditions.UID)
		require.NotNil(t, options.Preconditions.ResourceVersion)
		assert.Equal(t, live.UID, *options.Preconditions.UID)
		assert.Equal(t, live.ResourceVersion, *options.Preconditions.ResourceVersion)
		return nil
	}}

	err := DeleteRetentionCandidate(context.Background(), reader, writer, schedule, &candidate)
	require.NoError(t, err)
	assert.Equal(t, 1, deletes)
	assert.Equal(t, []string{"get-backup", "get-schedule", "delete-backup"}, operations)
	assert.Equal(t, brv1alpha1.CleanPolicyTypeDelete, candidate.Backup.Spec.CleanPolicy)
	assert.Equal(t, []string{"example.com/protect"}, candidate.Backup.Finalizers)
}

func TestDeleteRetentionCandidateRejectsStaleBackup(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*brv1alpha1.Backup)
	}{
		{
			name: "UID changed",
			mutate: func(backup *brv1alpha1.Backup) {
				backup.UID = "replacement-uid"
			},
		},
		{
			name: "resource version changed",
			mutate: func(backup *brv1alpha1.Backup) {
				backup.ResourceVersion = "new-version"
			},
		},
		{
			name: "scheduled time changed",
			mutate: func(backup *brv1alpha1.Backup) {
				backup.Annotations[ScheduledTimeAnnotation] = "2026-08-20T00:00:00Z"
			},
		},
		{
			name: "already terminating",
			mutate: func(backup *brv1alpha1.Backup) {
				now := metav1.NewTime(time.Date(2026, 8, 20, 2, 0, 0, 0, time.UTC))
				backup.DeletionTimestamp = &now
			},
		},
		{
			name: "terminal state changed",
			mutate: func(backup *brv1alpha1.Backup) {
				backup.Status.Conditions = nil
			},
		},
		{
			name: "target changed",
			mutate: func(backup *brv1alpha1.Backup) {
				backup.Spec.BR.Cluster = "other-cluster"
			},
		},
		{
			name: "target namespace made explicit",
			mutate: func(backup *brv1alpha1.Backup) {
				backup.Spec.BR.ClusterNamespace = backup.Namespace
			},
		},
		{
			name: "target moved cross namespace",
			mutate: func(backup *brv1alpha1.Backup) {
				backup.Spec.BR.ClusterNamespace = "other-namespace"
			},
		},
		{
			name: "target removed",
			mutate: func(backup *brv1alpha1.Backup) {
				backup.Spec.BR = nil
			},
		},
		{
			name: "destination changed",
			mutate: func(backup *brv1alpha1.Backup) {
				backup.Spec.S3.Prefix = "other/" + backup.Name
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schedule, candidate := retentionDeleteFixture(t)
			live := candidate.Backup.DeepCopy()
			tt.mutate(live)

			reader := &retentionTestReader{get: func(_ context.Context, _ client.ObjectKey, obj client.Object) error {
				switch typed := obj.(type) {
				case *brv1alpha1.Backup:
					*typed = *live.DeepCopy()
				case *brv1alpha1.BackupSchedule:
					*typed = *schedule.DeepCopy()
				}
				return nil
			}}
			deletes := 0
			writer := &retentionTestWriter{delete: func(context.Context, client.Object, ...client.DeleteOption) error {
				deletes++
				return nil
			}}

			err := DeleteRetentionCandidate(context.Background(), reader, writer, schedule, &candidate)
			require.Error(t, err)
			require.ErrorIs(t, err, ErrRetentionPlanStale)
			assert.Equal(t, 0, deletes)
		})
	}
}

func TestDeleteRetentionCandidateUsesHistoricalCanonicalDestination(t *testing.T) {
	schedule := validSchedule()
	maxBackups := int32(1)
	schedule.Spec.MaxBackups = &maxBackups
	oldest := retentionTestBackup(t, schedule, 1, BackupSucceeded)
	newest := retentionTestBackup(t, schedule, 2, BackupSucceeded)

	schedule.Generation = 4
	schedule.Spec.BackupTemplate.S3.Bucket = "replacement-bucket"
	schedule.Spec.BackupTemplate.S3.Prefix = "replacement-prefix"
	plan, err := PlanRetention(schedule, []brv1alpha1.Backup{newest, oldest})
	require.NoError(t, err)
	require.Len(t, plan.DeletionCandidates, 1)
	candidate := plan.DeletionCandidates[0]
	assert.NotEqual(t, schedule.Spec.BackupTemplate.S3.Bucket, candidate.Backup.Spec.S3.Bucket)

	reader := &retentionTestReader{get: func(_ context.Context, _ client.ObjectKey, obj client.Object) error {
		switch typed := obj.(type) {
		case *brv1alpha1.Backup:
			*typed = *candidate.Backup.DeepCopy()
		case *brv1alpha1.BackupSchedule:
			*typed = *schedule.DeepCopy()
		default:
			return errors.New("unexpected object type")
		}
		return nil
	}}
	deletes := 0
	writer := &retentionTestWriter{delete: func(_ context.Context, obj client.Object, _ ...client.DeleteOption) error {
		deletes++
		assert.Equal(t, candidate.Backup.Spec.StorageProvider, obj.(*brv1alpha1.Backup).Spec.StorageProvider)
		return nil
	}}

	require.NoError(t, DeleteRetentionCandidate(context.Background(), reader, writer, schedule, &candidate))
	assert.Equal(t, 1, deletes)
}

func TestDeleteRetentionCandidateRechecksScheduleSafety(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*brv1alpha1.BackupSchedule)
	}{
		{name: "paused", mutate: func(schedule *brv1alpha1.BackupSchedule) { schedule.Spec.Pause = true }},
		{name: "UID changed", mutate: func(schedule *brv1alpha1.BackupSchedule) { schedule.UID = "replacement-uid" }},
		{name: "generation changed", mutate: func(schedule *brv1alpha1.BackupSchedule) { schedule.Generation++ }},
		{name: "target changed", mutate: func(schedule *brv1alpha1.BackupSchedule) {
			schedule.Spec.Cluster.Name = "replacement-cluster"
			schedule.Spec.BackupTemplate.BR.Cluster = "replacement-cluster"
		}},
		{name: "retention disabled", mutate: func(schedule *brv1alpha1.BackupSchedule) { schedule.Spec.MaxBackups = nil }},
		{name: "retention set to zero", mutate: func(schedule *brv1alpha1.BackupSchedule) {
			value := int32(0)
			schedule.Spec.MaxBackups = &value
		}},
		{name: "invalid specification", mutate: func(schedule *brv1alpha1.BackupSchedule) {
			schedule.Spec.BackupTemplate.BR.ClusterNamespace = schedule.Namespace
		}},
		{name: "schedule deleting", mutate: func(schedule *brv1alpha1.BackupSchedule) {
			now := metav1.NewTime(time.Date(2026, 8, 20, 2, 0, 0, 0, time.UTC))
			schedule.DeletionTimestamp = &now
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schedule, candidate := retentionDeleteFixture(t)
			liveSchedule := schedule.DeepCopy()
			tt.mutate(liveSchedule)

			reader := &retentionTestReader{get: func(_ context.Context, _ client.ObjectKey, obj client.Object) error {
				switch typed := obj.(type) {
				case *brv1alpha1.Backup:
					*typed = *candidate.Backup.DeepCopy()
				case *brv1alpha1.BackupSchedule:
					*typed = *liveSchedule.DeepCopy()
				}
				return nil
			}}
			deletes := 0
			writer := &retentionTestWriter{delete: func(context.Context, client.Object, ...client.DeleteOption) error {
				deletes++
				return nil
			}}

			err := DeleteRetentionCandidate(context.Background(), reader, writer, schedule, &candidate)
			require.Error(t, err)
			require.ErrorIs(t, err, ErrRetentionPlanStale)
			assert.Equal(t, 0, deletes)
		})
	}
}

func retentionDeleteFixture(t *testing.T) (*brv1alpha1.BackupSchedule, RetentionCandidate) {
	t.Helper()
	schedule := validSchedule()
	schedule.Generation = 3
	maxBackups := int32(1)
	schedule.Spec.MaxBackups = &maxBackups
	backup := retentionTestBackup(t, schedule, 1, BackupSucceeded)
	scheduledTime, err := ParseScheduledTime(&backup)
	require.NoError(t, err)
	target, err := ResolveBackupTarget(&backup)
	require.NoError(t, err)
	return schedule, RetentionCandidate{
		Backup:        backup.DeepCopy(),
		ScheduledTime: scheduledTime,
		Target:        target,
		State:         BackupSucceeded,
	}
}

type retentionTestReader struct {
	get  func(context.Context, client.ObjectKey, client.Object) error
	list func(context.Context, client.ObjectList, ...client.ListOption) error
}

func (reader *retentionTestReader) Get(
	ctx context.Context,
	key client.ObjectKey,
	obj client.Object,
	_ ...client.GetOption,
) error {
	if reader.get == nil {
		return errors.New("unexpected get")
	}
	return reader.get(ctx, key, obj)
}

func (reader *retentionTestReader) List(
	ctx context.Context,
	list client.ObjectList,
	opts ...client.ListOption,
) error {
	if reader.list == nil {
		return errors.New("unexpected list")
	}
	return reader.list(ctx, list, opts...)
}

type retentionTestWriter struct {
	create      func(context.Context, client.Object, ...client.CreateOption) error
	delete      func(context.Context, client.Object, ...client.DeleteOption) error
	deleteAllOf func(context.Context, client.Object, ...client.DeleteAllOfOption) error
	update      func(context.Context, client.Object, ...client.UpdateOption) error
	patch       func(context.Context, client.Object, client.Patch, ...client.PatchOption) error
}

func (writer *retentionTestWriter) Create(
	ctx context.Context,
	obj client.Object,
	opts ...client.CreateOption,
) error {
	if writer.create == nil {
		return errors.New("unexpected create")
	}
	return writer.create(ctx, obj, opts...)
}

func (writer *retentionTestWriter) Delete(
	ctx context.Context,
	obj client.Object,
	opts ...client.DeleteOption,
) error {
	if writer.delete == nil {
		return errors.New("unexpected delete")
	}
	return writer.delete(ctx, obj, opts...)
}

func (writer *retentionTestWriter) DeleteAllOf(
	ctx context.Context,
	obj client.Object,
	opts ...client.DeleteAllOfOption,
) error {
	if writer.deleteAllOf == nil {
		return errors.New("unexpected delete all of")
	}
	return writer.deleteAllOf(ctx, obj, opts...)
}

func (writer *retentionTestWriter) Update(
	ctx context.Context,
	obj client.Object,
	opts ...client.UpdateOption,
) error {
	if writer.update == nil {
		return errors.New("unexpected update")
	}
	return writer.update(ctx, obj, opts...)
}

func (writer *retentionTestWriter) Patch(
	ctx context.Context,
	obj client.Object,
	patch client.Patch,
	opts ...client.PatchOption,
) error {
	if writer.patch == nil {
		return errors.New("unexpected patch")
	}
	return writer.patch(ctx, obj, patch, opts...)
}
