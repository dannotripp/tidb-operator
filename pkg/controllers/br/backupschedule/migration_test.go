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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	brv1alpha1 "github.com/pingcap/tidb-operator/api/v2/br/v1alpha1"
	corev1alpha1 "github.com/pingcap/tidb-operator/api/v2/core/v1alpha1"
)

func TestSchedulingStoredResourceMigrationBlockersFailClosed(t *testing.T) {
	stringValue := "value"
	storageClass := "standard"
	tests := []struct {
		name   string
		mutate func(*brv1alpha1.BackupSchedule)
	}{
		{name: "missing cluster", mutate: func(schedule *brv1alpha1.BackupSchedule) {
			schedule.Spec.Cluster = corev1alpha1.ClusterReference{}
		}},
		{name: "overlong cluster", mutate: func(schedule *brv1alpha1.BackupSchedule) {
			schedule.Spec.Cluster.Name = strings.Repeat("a", 254)
			schedule.Spec.BackupTemplate.BR.Cluster = schedule.Spec.Cluster.Name
		}},
		{name: "negative max backups", mutate: func(schedule *brv1alpha1.BackupSchedule) {
			value := int32(-1)
			schedule.Spec.MaxBackups = &value
		}},
		{name: "time based retention", mutate: func(schedule *brv1alpha1.BackupSchedule) {
			schedule.Spec.MaxReservedTime = &stringValue
		}},
		{name: "log backup template", mutate: func(schedule *brv1alpha1.BackupSchedule) {
			schedule.Spec.LogBackupTemplate = &brv1alpha1.BackupSpec{}
		}},
		{name: "compact span", mutate: func(schedule *brv1alpha1.BackupSchedule) {
			schedule.Spec.CompactSpan = &stringValue
		}},
		{name: "compact template", mutate: func(schedule *brv1alpha1.BackupSchedule) {
			schedule.Spec.CompactBackupTemplate = &brv1alpha1.CompactSpec{}
		}},
		{name: "schedule BR inheritance", mutate: func(schedule *brv1alpha1.BackupSchedule) {
			schedule.Spec.BR = &brv1alpha1.BRConfig{Cluster: schedule.Spec.Cluster.Name}
		}},
		{name: "schedule S3 inheritance", mutate: func(schedule *brv1alpha1.BackupSchedule) {
			schedule.Spec.S3 = &brv1alpha1.S3StorageProvider{}
		}},
		{name: "schedule GCS inheritance", mutate: func(schedule *brv1alpha1.BackupSchedule) {
			schedule.Spec.Gcs = &brv1alpha1.GcsStorageProvider{}
		}},
		{name: "schedule Azure Blob inheritance", mutate: func(schedule *brv1alpha1.BackupSchedule) {
			schedule.Spec.Azblob = &brv1alpha1.AzblobStorageProvider{}
		}},
		{name: "schedule local inheritance", mutate: func(schedule *brv1alpha1.BackupSchedule) {
			schedule.Spec.Local = &brv1alpha1.LocalStorageProvider{}
		}},
		{name: "schedule storage class", mutate: func(schedule *brv1alpha1.BackupSchedule) {
			schedule.Spec.StorageClassName = &storageClass
		}},
		{name: "schedule storage size", mutate: func(schedule *brv1alpha1.BackupSchedule) {
			schedule.Spec.StorageSize = "1Gi"
		}},
		{name: "schedule image pull secrets", mutate: func(schedule *brv1alpha1.BackupSchedule) {
			schedule.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "pull-secret"}}
		}},
		{name: "template log mode", mutate: func(schedule *brv1alpha1.BackupSchedule) {
			schedule.Spec.BackupTemplate.Mode = brv1alpha1.BackupModeLog
		}},
		{name: "template log subcommand", mutate: func(schedule *brv1alpha1.BackupSchedule) {
			schedule.Spec.BackupTemplate.LogSubcommand = brv1alpha1.LogStopCommand
		}},
		{name: "template log truncate", mutate: func(schedule *brv1alpha1.BackupSchedule) {
			schedule.Spec.BackupTemplate.LogTruncateUntil = "123"
		}},
		{name: "template log stop", mutate: func(schedule *brv1alpha1.BackupSchedule) {
			schedule.Spec.BackupTemplate.LogStop = true
		}},
		{name: "nested cluster conflict", mutate: func(schedule *brv1alpha1.BackupSchedule) {
			schedule.Spec.BackupTemplate.BR.Cluster = "other"
		}},
		{name: "nested cluster namespace", mutate: func(schedule *brv1alpha1.BackupSchedule) {
			schedule.Spec.BackupTemplate.BR.ClusterNamespace = schedule.Namespace
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursor := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
			schedule := validSchedule()
			schedule.Status.LastScheduleTime = timePointer(cursor)
			schedule.Status.LastBackup = "prior-backup"
			schedule.Status.LastBackupTime = timePointer(cursor.Add(-time.Hour))
			wantLastBackupTime := schedule.Status.LastBackupTime.DeepCopy()
			tt.mutate(schedule)

			base := newSchedulingFakeClient(t, schedule)
			reconciler := NewSchedulingReconciler(
				base,
				base,
				staticClock{now: cursor.Add(90 * time.Minute)},
				NewTargetLocks(),
			)

			result, err := reconciler.Reconcile(context.Background(), requestFor(schedule))
			require.NoError(t, err)
			assert.True(t, result.IsZero())

			stored := getSchedule(t, base, schedule)
			assert.True(t, stored.Status.LastScheduleTime.Time.Equal(cursor))
			assert.Equal(t, "prior-backup", stored.Status.LastBackup)
			assertMetav1TimeEqual(t, wantLastBackupTime, stored.Status.LastBackupTime)
			assert.Empty(t, listBackups(t, base).Items)
			assertSchedulingReady(t, stored, metav1.ConditionFalse, ReasonInvalidSpec)
		})
	}
}
