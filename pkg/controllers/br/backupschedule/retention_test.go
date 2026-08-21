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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	brv1alpha1 "github.com/pingcap/tidb-operator/api/v2/br/v1alpha1"
)

func TestPlanRetentionPartitionsQuotasAndOrdersOldestFirst(t *testing.T) {
	schedule := validSchedule()
	maxBackups := int32(2)
	schedule.Spec.MaxBackups = &maxBackups

	backups := make([]brv1alpha1.Backup, 0, 16)
	for hour := 1; hour <= 5; hour++ {
		backups = append(backups, retentionTestBackup(t, schedule, hour, BackupSucceeded))
	}
	for hour := 6; hour <= 12; hour++ {
		state := BackupFailed
		if hour%2 == 0 {
			state = BackupInvalid
		}
		backups = append(backups, retentionTestBackup(t, schedule, hour, state))
	}

	active := retentionTestBackup(t, schedule, 0, BackupActive)
	backups = append(backups, active)
	terminating := retentionTestBackup(t, schedule, 13, BackupSucceeded)
	deletionTime := metav1.NewTime(time.Date(2026, 8, 20, 14, 0, 0, 0, time.UTC))
	terminating.DeletionTimestamp = &deletionTime
	backups = append(backups, terminating)

	foreign := retentionTestBackup(t, schedule, 14, BackupSucceeded)
	foreign.Labels[ScheduleUIDLabel] = "old-schedule-uid"
	foreign.Annotations[ScheduledTimeAnnotation] = "malformed-but-ignored"
	backups = append(backups, foreign)
	foreignNamespace := retentionTestBackup(t, schedule, 15, BackupSucceeded)
	foreignNamespace.Namespace = "other-namespace"
	foreignNamespace.Annotations[ScheduledTimeAnnotation] = "malformed-but-ignored"
	backups = append(backups, foreignNamespace)

	plan, err := PlanRetention(schedule, backups)
	require.NoError(t, err)
	require.Len(t, plan.DeletionCandidates, 5)

	gotHours := make([]int, 0, len(plan.DeletionCandidates))
	for _, candidate := range plan.DeletionCandidates {
		gotHours = append(gotHours, candidate.ScheduledTime.Hour())
	}
	assert.Equal(t, []int{1, 2, 3, 6, 7}, gotHours)
}

func TestPlanRetentionFailsClosedForCurrentUIDObjects(t *testing.T) {
	schedule := validSchedule()
	maxBackups := int32(1)
	schedule.Spec.MaxBackups = &maxBackups

	tests := []struct {
		name   string
		mutate func(*brv1alpha1.Backup)
	}{
		{
			name: "missing scheduled time",
			mutate: func(backup *brv1alpha1.Backup) {
				delete(backup.Annotations, ScheduledTimeAnnotation)
			},
		},
		{
			name: "malformed scheduled time",
			mutate: func(backup *brv1alpha1.Backup) {
				backup.Annotations[ScheduledTimeAnnotation] = "not-a-time"
			},
		},
		{
			name: "wrong deterministic name",
			mutate: func(backup *brv1alpha1.Backup) {
				backup.Name = "foreign"
			},
		},
		{
			name: "log mode",
			mutate: func(backup *brv1alpha1.Backup) {
				backup.Spec.Mode = brv1alpha1.BackupModeLog
			},
		},
		{
			name: "different target cluster",
			mutate: func(backup *brv1alpha1.Backup) {
				backup.Spec.BR.Cluster = "other-cluster"
			},
		},
		{
			name: "explicit same namespace target",
			mutate: func(backup *brv1alpha1.Backup) {
				backup.Spec.BR.ClusterNamespace = backup.Namespace
			},
		},
		{
			name: "cross namespace target",
			mutate: func(backup *brv1alpha1.Backup) {
				backup.Spec.BR.ClusterNamespace = "other-namespace"
			},
		},
		{
			name: "missing target",
			mutate: func(backup *brv1alpha1.Backup) {
				backup.Spec.BR = nil
			},
		},
		{
			name: "noncanonical destination",
			mutate: func(backup *brv1alpha1.Backup) {
				backup.Spec.S3.Prefix = "base//" + backup.Name
			},
		},
		{
			name: "ambiguous terminal conditions",
			mutate: func(backup *brv1alpha1.Backup) {
				backup.Status.Conditions = []metav1.Condition{
					{Type: string(brv1alpha1.BackupComplete), Status: metav1.ConditionTrue},
					{Type: string(brv1alpha1.BackupFailed), Status: metav1.ConditionTrue},
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backup := retentionTestBackup(t, schedule, 1, BackupSucceeded)
			tt.mutate(&backup)
			_, err := PlanRetention(schedule, []brv1alpha1.Backup{backup})
			assert.Error(t, err)
		})
	}
}

func TestPlanRetentionDisabledDoesNotInspectBackups(t *testing.T) {
	schedule := validSchedule()
	malformed := retentionTestBackup(t, schedule, 1, BackupSucceeded)
	delete(malformed.Annotations, ScheduledTimeAnnotation)

	for _, maxBackups := range []*int32{nil, ptr(int32(0))} {
		schedule.Spec.MaxBackups = maxBackups
		plan, err := PlanRetention(schedule, []brv1alpha1.Backup{malformed})
		require.NoError(t, err)
		assert.Empty(t, plan.DeletionCandidates)
	}
}

func TestPlanRetentionPreservesCleanPolicy(t *testing.T) {
	schedule := validSchedule()
	maxBackups := int32(1)
	schedule.Spec.MaxBackups = &maxBackups

	policies := []brv1alpha1.CleanPolicyType{
		"",
		brv1alpha1.CleanPolicyTypeRetain,
		brv1alpha1.CleanPolicyTypeOnFailure,
		brv1alpha1.CleanPolicyTypeDelete,
	}
	for i, policy := range policies {
		backup := retentionTestBackup(t, schedule, i+1, BackupSucceeded)
		backup.Spec.CleanPolicy = policy
		newer := retentionTestBackup(t, schedule, i+10, BackupSucceeded)
		plan, err := PlanRetention(schedule, []brv1alpha1.Backup{backup, newer})
		require.NoError(t, err)
		require.Len(t, plan.DeletionCandidates, 1)
		assert.Equal(t, policy, plan.DeletionCandidates[0].Backup.Spec.CleanPolicy)
		assert.Equal(t, policy, backup.Spec.CleanPolicy)
	}
}

func TestPlanRetentionUsesEachCandidatesCanonicalDestination(t *testing.T) {
	schedule := validSchedule()
	maxBackups := int32(1)
	schedule.Spec.MaxBackups = &maxBackups
	oldest := retentionTestBackup(t, schedule, 1, BackupSucceeded)
	newest := retentionTestBackup(t, schedule, 2, BackupSucceeded)

	oldestBucket := oldest.Spec.S3.Bucket
	oldestPrefix := oldest.Spec.S3.Prefix
	schedule.Spec.BackupTemplate.S3.Bucket = "replacement-bucket"
	schedule.Spec.BackupTemplate.S3.Prefix = "replacement-prefix"

	plan, err := PlanRetention(schedule, []brv1alpha1.Backup{newest, oldest})
	require.NoError(t, err)
	require.Len(t, plan.DeletionCandidates, 1)
	assert.Equal(t, oldest.Name, plan.DeletionCandidates[0].Backup.Name)
	assert.Equal(t, oldestBucket, plan.DeletionCandidates[0].Backup.Spec.S3.Bucket)
	assert.Equal(t, oldestPrefix, plan.DeletionCandidates[0].Backup.Spec.S3.Prefix)
}

func TestRetentionDelegatesRemoteCleanupToBackupCleanPolicy(t *testing.T) {
	tests := []struct {
		name           string
		policy         brv1alpha1.CleanPolicyType
		state          BackupTerminalState
		cleanCandidate bool
		retainData     bool
	}{
		{
			name:  "empty policy follows API default and does not enter cleanup",
			state: BackupSucceeded,
		},
		{
			name:       "Retain keeps successful data",
			policy:     brv1alpha1.CleanPolicyTypeRetain,
			state:      BackupSucceeded,
			retainData: true,
		},
		{
			name:           "OnFailure keeps successful data",
			policy:         brv1alpha1.CleanPolicyTypeOnFailure,
			state:          BackupSucceeded,
			cleanCandidate: true,
			retainData:     true,
		},
		{
			name:           "OnFailure cleans failed data",
			policy:         brv1alpha1.CleanPolicyTypeOnFailure,
			state:          BackupFailed,
			cleanCandidate: true,
		},
		{
			name:           "OnFailure keeps invalid data",
			policy:         brv1alpha1.CleanPolicyTypeOnFailure,
			state:          BackupInvalid,
			cleanCandidate: true,
			retainData:     true,
		},
		{
			name:           "Delete cleans successful data",
			policy:         brv1alpha1.CleanPolicyTypeDelete,
			state:          BackupSucceeded,
			cleanCandidate: true,
		},
		{
			name:           "Delete cleans failed data",
			policy:         brv1alpha1.CleanPolicyTypeDelete,
			state:          BackupFailed,
			cleanCandidate: true,
		},
		{
			name:           "Delete requests invalid cleanup but finalizer stall is handled separately",
			policy:         brv1alpha1.CleanPolicyTypeDelete,
			state:          BackupInvalid,
			cleanCandidate: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schedule := validSchedule()
			backup := retentionTestBackup(t, schedule, 1, tt.state)
			backup.Spec.CleanPolicy = tt.policy

			assert.Equal(t, tt.cleanCandidate, brv1alpha1.IsCleanCandidate(&backup))
			assert.Equal(t, tt.retainData, brv1alpha1.NeedRetainData(&backup))
			assert.Equal(t, tt.policy, backup.Spec.CleanPolicy)
		})
	}
}

func TestRetentionCandidateTieBreakers(t *testing.T) {
	at := time.Date(2026, 8, 20, 1, 0, 0, 0, time.UTC)
	candidates := []RetentionCandidate{
		{Backup: &brv1alpha1.Backup{ObjectMeta: metav1.ObjectMeta{Name: "b"}}, ScheduledTime: at},
		{Backup: &brv1alpha1.Backup{ObjectMeta: metav1.ObjectMeta{Name: "a"}}, ScheduledTime: at},
	}

	sortRetentionCandidatesNewestFirst(candidates)
	assert.Equal(t, []string{"b", "a"}, []string{candidates[0].Backup.Name, candidates[1].Backup.Name})
	sortRetentionCandidatesOldestFirst(candidates)
	assert.Equal(t, []string{"a", "b"}, []string{candidates[0].Backup.Name, candidates[1].Backup.Name})
}

func retentionTestBackup(
	t *testing.T,
	schedule *brv1alpha1.BackupSchedule,
	hour int,
	state BackupTerminalState,
) brv1alpha1.Backup {
	t.Helper()
	scheduledTime := time.Date(2026, 8, 20, hour, 0, 0, 0, time.UTC)
	backup, err := RenderBackup(schedule, scheduledTime)
	require.NoError(t, err)
	backup.UID = types.UID(fmt.Sprintf("backup-uid-%02d", hour))
	backup.ResourceVersion = fmt.Sprintf("%d", hour+1)

	switch state {
	case BackupSucceeded:
		backup.Status.Conditions = []metav1.Condition{{
			Type: string(brv1alpha1.BackupComplete), Status: metav1.ConditionTrue,
		}}
	case BackupFailed:
		backup.Status.Conditions = []metav1.Condition{{
			Type: string(brv1alpha1.BackupFailed), Status: metav1.ConditionTrue,
		}}
	case BackupInvalid:
		backup.Status.Conditions = []metav1.Condition{{
			Type: string(brv1alpha1.BackupInvalid), Status: metav1.ConditionTrue,
		}}
	case BackupActive:
		backup.Status.Phase = brv1alpha1.BackupComplete
	default:
		t.Fatalf("unsupported test state %q", state)
	}
	return *backup
}
