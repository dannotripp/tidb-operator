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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	brv1alpha1 "github.com/pingcap/tidb-operator/api/v2/br/v1alpha1"
	corev1alpha1 "github.com/pingcap/tidb-operator/api/v2/core/v1alpha1"
)

func TestBackupScheduleIndexFunctions(t *testing.T) {
	schedule := indexedSchedule("backups", "hourly", "uid-hourly", "example")
	assert.Equal(t, []string{"backups/example"}, indexBackupScheduleByTarget(schedule))
	schedule.Spec.BackupTemplate.BR = nil
	assert.Equal(t, []string{"backups/example"}, indexBackupScheduleByTarget(schedule))

	schedule.Spec.BackupTemplate.BR = &brv1alpha1.BRConfig{Cluster: "example"}
	schedule.Spec.BackupTemplate.BR.ClusterNamespace = "other"
	assert.Nil(t, indexBackupScheduleByTarget(schedule))
	schedule.Spec.BackupTemplate.BR.ClusterNamespace = ""
	schedule.Spec.Cluster.Name = "INVALID"
	assert.Nil(t, indexBackupScheduleByTarget(schedule))
	assert.Nil(t, indexBackupScheduleByTarget(&brv1alpha1.Backup{}))
}

func TestBackupEventMapper(t *testing.T) {
	first := indexedSchedule("backups", "first", "uid-first", "example")
	second := indexedSchedule("backups", "second", "uid-second", "example")
	third := indexedSchedule("other", "third", "uid-third", "other-example")
	mapper := BackupEventMapper{Reader: indexedFakeClient(t, first, second, third)}

	manual := &brv1alpha1.Backup{
		ObjectMeta: v1.ObjectMeta{Namespace: "jobs", Name: "manual"},
		Spec: brv1alpha1.BackupSpec{
			BR: &brv1alpha1.BRConfig{Cluster: "example", ClusterNamespace: "backups"},
		},
	}
	assert.ElementsMatch(t, []reconcile.Request{
		{NamespacedName: types.NamespacedName{Namespace: "backups", Name: "first"}},
		{NamespacedName: types.NamespacedName{Namespace: "backups", Name: "second"}},
	}, mapper.SchedulingRequests(context.Background(), manual))

	manual.Spec.BR = nil
	assert.Nil(t, mapper.SchedulingRequests(context.Background(), manual))
}

func TestBackupUpdateEventsMapOldAndNewAssociations(t *testing.T) {
	first := indexedSchedule("backups", "first", "uid-first", "example")
	second := indexedSchedule("other", "second", "uid-second", "other-example")
	mapper := BackupEventMapper{Reader: indexedFakeClient(t, first, second)}

	oldBackup := &brv1alpha1.Backup{
		ObjectMeta: v1.ObjectMeta{
			Namespace: "backups",
			Name:      "changing",
			Labels:    map[string]string{ScheduleUIDLabel: "uid-first"},
		},
		Spec: brv1alpha1.BackupSpec{BR: &brv1alpha1.BRConfig{Cluster: "example"}},
	}
	newBackup := oldBackup.DeepCopy()
	newBackup.Labels[ScheduleUIDLabel] = "uid-second"
	newBackup.Spec.BR.Cluster = "other-example"
	newBackup.Spec.BR.ClusterNamespace = "other"

	t.Run("scheduling target", func(t *testing.T) {
		eventHandler := handler.EnqueueRequestsFromMapFunc(mapper.SchedulingRequests)
		queue := workqueue.NewTypedRateLimitingQueue(
			workqueue.DefaultTypedItemBasedRateLimiter[reconcile.Request](),
		)
		t.Cleanup(queue.ShutDown)
		eventHandler.Update(
			context.Background(),
			event.TypedUpdateEvent[client.Object]{ObjectOld: oldBackup, ObjectNew: newBackup},
			queue,
		)
		assert.Equal(t, requestSet(first, second), drainRequests(t, queue))
	})
}

func TestBackupCreateDeleteAndGenericEventsMapAssociations(t *testing.T) {
	schedule := indexedSchedule("backups", "hourly", "uid-hourly", "example")
	mapper := BackupEventMapper{Reader: indexedFakeClient(t, schedule)}
	backup := &brv1alpha1.Backup{
		ObjectMeta: v1.ObjectMeta{
			Namespace: "backups",
			Name:      "managed",
			Labels:    map[string]string{ScheduleUIDLabel: "uid-hourly"},
		},
		Spec: brv1alpha1.BackupSpec{BR: &brv1alpha1.BRConfig{Cluster: "example"}},
	}
	want := requestSet(schedule)

	eventHandler := handler.EnqueueRequestsFromMapFunc(mapper.SchedulingRequests)

	t.Run("create", func(t *testing.T) {
		queue := newRequestQueue(t)
		eventHandler.Create(
			context.Background(),
			event.TypedCreateEvent[client.Object]{Object: backup},
			queue,
		)
		assert.Equal(t, want, drainRequests(t, queue))
	})

	t.Run("delete", func(t *testing.T) {
		queue := newRequestQueue(t)
		eventHandler.Delete(
			context.Background(),
			event.TypedDeleteEvent[client.Object]{Object: backup},
			queue,
		)
		assert.Equal(t, want, drainRequests(t, queue))
	})

	t.Run("generic", func(t *testing.T) {
		queue := newRequestQueue(t)
		eventHandler.Generic(
			context.Background(),
			event.TypedGenericEvent[client.Object]{Object: backup},
			queue,
		)
		assert.Equal(t, want, drainRequests(t, queue))
	})
}

func indexedFakeClient(t *testing.T, schedules ...*brv1alpha1.BackupSchedule) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, brv1alpha1.AddToScheme(scheme))
	objects := make([]client.Object, 0, len(schedules))
	for _, schedule := range schedules {
		objects = append(objects, schedule)
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).
		WithIndex(&brv1alpha1.BackupSchedule{}, backupScheduleTargetIndex, indexBackupScheduleByTarget).
		Build()
}

func indexedSchedule(namespace, name string, uid types.UID, cluster string) *brv1alpha1.BackupSchedule {
	return &brv1alpha1.BackupSchedule{
		ObjectMeta: v1.ObjectMeta{Namespace: namespace, Name: name, UID: uid},
		Spec: brv1alpha1.BackupScheduleSpec{
			Cluster:  corev1alpha1.ClusterReference{Name: cluster},
			Schedule: "0 * * * *",
			BackupTemplate: brv1alpha1.BackupSpec{
				BR: &brv1alpha1.BRConfig{Cluster: cluster},
			},
		},
	}
}

func requestSet(schedules ...*brv1alpha1.BackupSchedule) map[reconcile.Request]struct{} {
	requests := make(map[reconcile.Request]struct{}, len(schedules))
	for _, schedule := range schedules {
		requests[reconcile.Request{NamespacedName: client.ObjectKeyFromObject(schedule)}] = struct{}{}
	}
	return requests
}

func newRequestQueue(t *testing.T) workqueue.TypedRateLimitingInterface[reconcile.Request] {
	t.Helper()
	queue := workqueue.NewTypedRateLimitingQueue(
		workqueue.DefaultTypedItemBasedRateLimiter[reconcile.Request](),
	)
	t.Cleanup(queue.ShutDown)
	return queue
}

func drainRequests(
	t *testing.T,
	queue workqueue.TypedRateLimitingInterface[reconcile.Request],
) map[reconcile.Request]struct{} {
	t.Helper()
	requests := make(map[reconcile.Request]struct{}, queue.Len())
	for queue.Len() > 0 {
		request, shutdown := queue.Get()
		require.False(t, shutdown)
		requests[request] = struct{}{}
		queue.Done(request)
	}
	return requests
}
