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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	brv1alpha1 "github.com/pingcap/tidb-operator/api/v2/br/v1alpha1"
)

func TestFindNewestManagedBackup(t *testing.T) {
	schedule := validSchedule()
	firstTime := time.Date(2026, 8, 20, 1, 0, 0, 0, time.UTC)
	secondTime := firstTime.Add(time.Hour)
	first, err := RenderBackup(schedule, firstTime)
	require.NoError(t, err)
	first.CreationTimestamp = metav1.NewTime(firstTime.Add(time.Minute))
	second, err := RenderBackup(schedule, secondTime)
	require.NoError(t, err)
	second.CreationTimestamp = metav1.NewTime(secondTime.Add(time.Minute))

	malformed := second.DeepCopy()
	malformed.Name = "malformed"
	malformed.Annotations[ScheduledTimeAnnotation] = "not-a-time"
	foreign := second.DeepCopy()
	foreign.Name = "foreign"
	foreign.Labels[ScheduleUIDLabel] = "different-uid"

	reader := newSchedulingFakeClient(t, first, second, malformed, foreign)
	newest, err := FindNewestManagedBackup(context.Background(), reader, schedule)
	require.NoError(t, err)
	require.NotNil(t, newest)
	assert.Equal(t, second.Name, newest.Backup.Name)
	assert.Equal(t, secondTime, newest.ScheduledTime)
}

func TestFindNewestManagedBackupReturnsNilWithoutCandidate(t *testing.T) {
	schedule := validSchedule()
	reader := newSchedulingFakeClient(t)
	newest, err := FindNewestManagedBackup(context.Background(), reader, schedule)
	require.NoError(t, err)
	assert.Nil(t, newest)
}

func TestFindNewestManagedBackupSurvivesDestinationEdit(t *testing.T) {
	scheduledTime := time.Date(2026, 8, 20, 2, 0, 0, 0, time.UTC)
	previous := validSchedule()
	historical, err := RenderBackup(previous, scheduledTime)
	require.NoError(t, err)

	current := previous.DeepCopy()
	current.Spec.BackupTemplate.S3.Bucket = "replacement-bucket"
	current.Spec.BackupTemplate.S3.Prefix = "replacement-prefix"
	require.NoError(t, ValidateBackupSchedule(current))
	require.NoError(t, ValidateManagedDestination(historical))

	reader := newSchedulingFakeClient(t, historical)
	newest, err := FindNewestManagedBackup(context.Background(), reader, current)
	require.NoError(t, err)
	require.NotNil(t, newest)
	assert.Equal(t, historical.Name, newest.Backup.Name)
	assert.Equal(t, scheduledTime, newest.ScheduledTime)
}

func TestFindBlockingBackupUsesEffectiveTargetAndConditions(t *testing.T) {
	target := Target{Namespace: "clusters", Cluster: "example"}
	active := backupForTarget("z-active", "backups", "clusters", "example")
	manual := backupForTarget("a-manual", "other", "clusters", "example")
	terminal := backupForTarget("terminal", "backups", "clusters", "example")
	terminal.Status.Conditions = []metav1.Condition{{Type: string(brv1alpha1.BackupComplete), Status: metav1.ConditionTrue}}
	logBackup := backupForTarget("log", "backups", "clusters", "example")
	logBackup.Spec.Mode = brv1alpha1.BackupModeLog
	other := backupForTarget("other", "backups", "clusters", "different")

	reader := newSchedulingFakeClient(t, active, manual, terminal, logBackup, other)
	blocker, err := FindBlockingBackup(context.Background(), reader, target, client.ObjectKey{})
	require.NoError(t, err)
	require.NotNil(t, blocker)
	assert.Equal(t, "z-active", blocker.Name)

	blocker, err = FindBlockingBackup(context.Background(), reader, target, client.ObjectKeyFromObject(active))
	require.NoError(t, err)
	require.NotNil(t, blocker)
	assert.Equal(t, "a-manual", blocker.Name)
}

func TestFindBlockingBackupTreatsAmbiguousAsBlocking(t *testing.T) {
	target := Target{Namespace: "backups", Cluster: "example"}
	ambiguous := backupForTarget("ambiguous", "backups", "", "example")
	ambiguous.Status.Conditions = []metav1.Condition{
		{Type: string(brv1alpha1.BackupComplete), Status: metav1.ConditionTrue},
		{Type: string(brv1alpha1.BackupFailed), Status: metav1.ConditionTrue},
	}
	reader := newSchedulingFakeClient(t, ambiguous)
	blocker, err := FindBlockingBackup(context.Background(), reader, target, client.ObjectKey{})
	require.NoError(t, err)
	require.NotNil(t, blocker)
	assert.Equal(t, ambiguous.Name, blocker.Name)
}

func TestFindBlockingBackupTreatsUnknownModeAsSnapshot(t *testing.T) {
	target := Target{Namespace: "backups", Cluster: "example"}
	unknown := backupForTarget("unknown-mode", "backups", "", "example")
	unknown.Spec.Mode = brv1alpha1.BackupMode("typo")

	reader := newSchedulingFakeClient(t, unknown)
	blocker, err := FindBlockingBackup(context.Background(), reader, target, client.ObjectKey{})
	require.NoError(t, err)
	require.NotNil(t, blocker)
	assert.Equal(t, unknown.Name, blocker.Name)
}

func newSchedulingFakeClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, brv1alpha1.AddToScheme(scheme))
	return fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&brv1alpha1.BackupSchedule{}).WithObjects(objects...).Build()
}

func backupForTarget(name, namespace, clusterNamespace, cluster string) *brv1alpha1.Backup {
	return &brv1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: brv1alpha1.BackupSpec{
			BR: &brv1alpha1.BRConfig{Cluster: cluster, ClusterNamespace: clusterNamespace},
		},
	}
}
