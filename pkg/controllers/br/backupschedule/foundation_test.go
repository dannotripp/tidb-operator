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
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"

	brv1alpha1 "github.com/pingcap/tidb-operator/api/v2/br/v1alpha1"
	corev1alpha1 "github.com/pingcap/tidb-operator/api/v2/core/v1alpha1"
)

func TestParseSchedule(t *testing.T) {
	originalLocal := time.Local
	time.Local = time.FixedZone("test-local", -7*60*60)
	t.Cleanup(func() { time.Local = originalLocal })

	for _, expression := range []string{
		"0 2 * * *",
		"*/5 * * * *",
		"@yearly",
		"@annually",
		"@monthly",
		"@weekly",
		"@daily",
		"@midnight",
		"@hourly",
		"@every 1m",
		"@every 90s",
	} {
		t.Run("accept "+expression, func(t *testing.T) {
			parsed, err := ParseSchedule(expression)
			require.NoError(t, err)
			next := parsed.Next(time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC))
			assert.Equal(t, time.UTC, next.Location())
		})
	}

	parsed, err := ParseSchedule("0 2 * * *")
	require.NoError(t, err)
	assert.Equal(
		t,
		time.Date(2026, 8, 20, 2, 0, 0, 0, time.UTC),
		parsed.Next(time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)),
	)

	for _, expression := range []string{
		"",
		"   ",
		"0 0 0 * * *",
		"TZ=America/Los_Angeles 0 0 * * *",
		"CRON_TZ=UTC 0 0 * * *",
		"@reboot",
		"@every",
		"@every 30s",
		"@every -1m",
		"0 0 30 2 *",
		"not a cron expression",
	} {
		t.Run("reject "+expression, func(t *testing.T) {
			_, err := ParseSchedule(expression)
			assert.Error(t, err)
		})
	}
}

func TestValidateBackupSchedule(t *testing.T) {
	require.NoError(t, ValidateBackupSchedule(validSchedule()))

	stringValue := "value"
	storageClass := "standard"
	tests := []struct {
		name   string
		mutate func(*brv1alpha1.BackupSchedule)
	}{
		{name: "negative max backups", mutate: func(s *brv1alpha1.BackupSchedule) { value := int32(-1); s.Spec.MaxBackups = &value }},
		{name: "positive max backups", mutate: func(s *brv1alpha1.BackupSchedule) { value := int32(3); s.Spec.MaxBackups = &value }},
		{name: "reserved time", mutate: func(s *brv1alpha1.BackupSchedule) { s.Spec.MaxReservedTime = &stringValue }},
		{name: "log template", mutate: func(s *brv1alpha1.BackupSchedule) { s.Spec.LogBackupTemplate = &brv1alpha1.BackupSpec{} }},
		{name: "compact span", mutate: func(s *brv1alpha1.BackupSchedule) { s.Spec.CompactSpan = &stringValue }},
		{name: "compact template", mutate: func(s *brv1alpha1.BackupSchedule) { s.Spec.CompactBackupTemplate = &brv1alpha1.CompactSpec{} }},
		{name: "schedule storage class", mutate: func(s *brv1alpha1.BackupSchedule) { s.Spec.StorageClassName = &storageClass }},
		{name: "schedule storage size", mutate: func(s *brv1alpha1.BackupSchedule) { s.Spec.StorageSize = "1Gi" }},
		{name: "schedule pull secret", mutate: func(s *brv1alpha1.BackupSchedule) {
			s.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "pull"}}
		}},
		{name: "schedule br inheritance", mutate: func(s *brv1alpha1.BackupSchedule) { s.Spec.BR = &brv1alpha1.BRConfig{Cluster: "other"} }},
		{name: "schedule storage inheritance", mutate: func(s *brv1alpha1.BackupSchedule) { s.Spec.S3 = &brv1alpha1.S3StorageProvider{} }},
		{name: "log mode", mutate: func(s *brv1alpha1.BackupSchedule) { s.Spec.BackupTemplate.Mode = brv1alpha1.BackupModeLog }},
		{name: "log subcommand", mutate: func(s *brv1alpha1.BackupSchedule) { s.Spec.BackupTemplate.LogSubcommand = brv1alpha1.LogStopCommand }},
		{name: "log truncate", mutate: func(s *brv1alpha1.BackupSchedule) { s.Spec.BackupTemplate.LogTruncateUntil = "123" }},
		{name: "log stop", mutate: func(s *brv1alpha1.BackupSchedule) { s.Spec.BackupTemplate.LogStop = true }},
		{name: "no provider", mutate: func(s *brv1alpha1.BackupSchedule) {
			s.Spec.BackupTemplate.StorageProvider = brv1alpha1.StorageProvider{}
		}},
		{name: "multiple providers", mutate: func(s *brv1alpha1.BackupSchedule) { s.Spec.BackupTemplate.Gcs = &brv1alpha1.GcsStorageProvider{} }},
		{name: "nested namespace", mutate: func(s *brv1alpha1.BackupSchedule) { s.Spec.BackupTemplate.BR.ClusterNamespace = s.Namespace }},
		{name: "parent prefix", mutate: func(s *brv1alpha1.BackupSchedule) { s.Spec.BackupTemplate.S3.Prefix = "safe/../escape" }},
		{name: "missing cluster", mutate: func(s *brv1alpha1.BackupSchedule) { s.Spec.Cluster.Name = "" }},
		{name: "conflicting nested cluster", mutate: func(s *brv1alpha1.BackupSchedule) { s.Spec.BackupTemplate.BR.Cluster = "other" }},
		{name: "invalid static backup", mutate: func(s *brv1alpha1.BackupSchedule) { s.Spec.BackupTemplate.S3.Bucket = "" }},
		{name: "missing Azure Blob container", mutate: func(s *brv1alpha1.BackupSchedule) {
			s.Spec.BackupTemplate.StorageProvider = brv1alpha1.StorageProvider{Azblob: &brv1alpha1.AzblobStorageProvider{}}
		}},
		{name: "missing Azure Blob credentials", mutate: func(s *brv1alpha1.BackupSchedule) {
			s.Spec.BackupTemplate.StorageProvider = brv1alpha1.StorageProvider{Azblob: &brv1alpha1.AzblobStorageProvider{Container: "container"}}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schedule := validSchedule()
			tt.mutate(schedule)
			assert.Error(t, ValidateBackupSchedule(schedule))
		})
	}

	for _, value := range []*int32{nil, ptr(int32(0))} {
		schedule := validSchedule()
		schedule.Spec.MaxBackups = value
		require.NoError(t, ValidateBackupSchedule(schedule))
	}

	scheduleWithoutNestedTarget := validSchedule()
	scheduleWithoutNestedTarget.Spec.BackupTemplate.BR = nil
	require.NoError(t, ValidateBackupSchedule(scheduleWithoutNestedTarget))

	for name, azblob := range map[string]*brv1alpha1.AzblobStorageProvider{
		"account only": {Container: "container", StorageAccount: "account"},
		"token only":   {Container: "container", SasToken: "token"},
	} {
		t.Run("reject Azure Blob "+name, func(t *testing.T) {
			schedule := validSchedule()
			schedule.Spec.BackupTemplate.StorageProvider = brv1alpha1.StorageProvider{Azblob: azblob}
			require.Error(t, ValidateBackupSchedule(schedule))
		})
	}
	scheduleWithInlineAzureCredentials := validSchedule()
	scheduleWithInlineAzureCredentials.Spec.BackupTemplate.StorageProvider = brv1alpha1.StorageProvider{
		Azblob: &brv1alpha1.AzblobStorageProvider{
			Container:      "container",
			StorageAccount: "account",
			SasToken:       "token",
		},
	}
	require.NoError(t, ValidateBackupSchedule(scheduleWithInlineAzureCredentials))

	for _, cluster := range []string{"UPPERCASE", "bad_name", ".leading", "trailing-", strings.Repeat("a", 254)} {
		t.Run("invalid cluster "+cluster[:min(len(cluster), 16)], func(t *testing.T) {
			schedule := validSchedule()
			schedule.Spec.Cluster.Name = cluster
			schedule.Spec.BackupTemplate.BR.Cluster = cluster
			require.Error(t, ValidateBackupSchedule(schedule))
		})
	}
}

func TestResolveTargets(t *testing.T) {
	schedule := validSchedule()
	target, err := ResolveScheduleTarget(schedule)
	require.NoError(t, err)
	assert.Equal(t, Target{Namespace: "backups", Cluster: "example"}, target)

	schedule.Spec.BackupTemplate.BR = nil
	target, err = ResolveScheduleTarget(schedule)
	require.NoError(t, err)
	assert.Equal(t, Target{Namespace: "backups", Cluster: "example"}, target)

	schedule.Spec.BackupTemplate.BR = &brv1alpha1.BRConfig{Cluster: "example", ClusterNamespace: "backups"}
	_, err = ResolveScheduleTarget(schedule)
	require.Error(t, err)

	schedule.Spec.BackupTemplate.BR = &brv1alpha1.BRConfig{Cluster: "other"}
	_, err = ResolveScheduleTarget(schedule)
	require.Error(t, err)

	backup := &brv1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Namespace: "backups"},
		Spec:       brv1alpha1.BackupSpec{BR: &brv1alpha1.BRConfig{Cluster: "example", ClusterNamespace: "clusters"}},
	}
	target, err = ResolveBackupTarget(backup)
	require.NoError(t, err)
	assert.Equal(t, Target{Namespace: "clusters", Cluster: "example"}, target)
}

func TestBackupName(t *testing.T) {
	scheduled := time.Date(2026, 8, 20, 1, 2, 3, 999, time.FixedZone("offset", 5*60*60))
	name := BackupName(
		"daily.backup-with-long-name",
		types.UID("11111111-2222-3333-4444-555555555555"),
		scheduled,
	)
	assert.Equal(t, "daily-backup-wit-666ff6ccaa5b3c07-20260819200203", name)
	assert.Len(t, name, 48)

	other := BackupName(
		"daily.backup-with-long-name",
		types.UID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
		scheduled,
	)
	assert.NotEqual(t, name, other)

	trailing := BackupName("123456789012345-.", types.UID("uid"), scheduled)
	assert.False(t, strings.HasPrefix(trailing, "123456789012345--"))
}

func TestNormalizeStoragePrefix(t *testing.T) {
	for input, expected := range map[string]string{
		"":                "",
		"/":               "",
		"base":            "base",
		"/base/":          "base",
		"base//child":     "base/child",
		"./base/./child/": "base/child",
	} {
		t.Run(input, func(t *testing.T) {
			actual, err := NormalizeStoragePrefix(input)
			require.NoError(t, err)
			assert.Equal(t, expected, actual)
		})
	}
	for _, input := range []string{"..", "../base", "base/..", "base//../child", "/../"} {
		_, err := NormalizeStoragePrefix(input)
		assert.Error(t, err, input)
	}
}

func TestRenderBackupAllProviders(t *testing.T) {
	assert.Equal(t, "tidb.pingcap.com/backup-schedule-uid", ScheduleUIDLabel)
	assert.Equal(t, "tidb.pingcap.com/backup-schedule-name", ScheduleNameAnnotation)
	assert.Equal(t, "tidb.pingcap.com/scheduled-at", ScheduledTimeAnnotation)

	providers := map[string]brv1alpha1.StorageProvider{
		"s3": {
			S3: &brv1alpha1.S3StorageProvider{Provider: brv1alpha1.S3StorageProviderTypeAWS, Bucket: "bucket", Prefix: "/base//./snapshots/"},
		},
		"gcs": {
			Gcs: &brv1alpha1.GcsStorageProvider{ProjectId: "project", Bucket: "bucket", Prefix: "/base//./snapshots/"},
		},
		"azblob": {
			Azblob: &brv1alpha1.AzblobStorageProvider{Container: "container", SecretName: "azure", Prefix: "/base//./snapshots/"},
		},
		"local": {
			Local: &brv1alpha1.LocalStorageProvider{
				Volume:      corev1.Volume{Name: "data"},
				VolumeMount: corev1.VolumeMount{Name: "data", MountPath: "/data"},
				Prefix:      "/base//./snapshots/",
			},
		},
	}
	scheduled := time.Date(2026, 8, 20, 1, 2, 3, 999999999, time.UTC)
	for name, provider := range providers {
		t.Run(name, func(t *testing.T) {
			schedule := validSchedule()
			schedule.Spec.BackupTemplate.BR = nil
			schedule.Spec.BackupTemplate.StorageProvider = provider
			schedule.Spec.BackupTemplate.CleanPolicy = brv1alpha1.CleanPolicyTypeDelete
			schedule.Labels = map[string]string{
				ScheduleUIDLabel:   "conflicting-value",
				"example.com/user": "label",
			}
			schedule.Annotations = map[string]string{
				ScheduleNameAnnotation:  "conflicting-value",
				ScheduledTimeAnnotation: "conflicting-value",
				"example.com/user":      "annotation",
			}
			original := schedule.DeepCopy()

			backup, err := RenderBackup(schedule, scheduled)
			require.NoError(t, err)
			_, prefix, err := StoragePrefix(backup.Spec.StorageProvider)
			require.NoError(t, err)
			assert.Equal(t, "base/snapshots/"+backup.Name, prefix)
			assert.Equal(t, brv1alpha1.CleanPolicyTypeDelete, backup.Spec.CleanPolicy)
			require.NotNil(t, backup.Spec.BR)
			assert.Equal(t, schedule.Spec.Cluster.Name, backup.Spec.BR.Cluster)
			assert.Empty(t, backup.Spec.BR.ClusterNamespace)
			assert.Empty(t, backup.OwnerReferences)
			assert.Equal(t, string(schedule.UID), backup.Labels[ScheduleUIDLabel])
			assert.Equal(t, schedule.Name, backup.Annotations[ScheduleNameAnnotation])
			assert.Equal(t, "2026-08-20T01:02:03Z", backup.Annotations[ScheduledTimeAnnotation])
			assert.NotContains(t, backup.Labels, "example.com/user")
			assert.NotContains(t, backup.Annotations, "example.com/user")
			assert.Equal(t, original, schedule, "rendering mutated the schedule template")
			for _, jobName := range []string{
				backup.GetBackupJobName(),
				backup.GetCleanJobName(),
				backup.GetVolumeBackupInitializeJobName(),
			} {
				assert.LessOrEqual(t, len(jobName), 63)
				assert.Empty(t, utilvalidation.IsDNS1123Subdomain(jobName), jobName)
			}
		})
	}
}

func TestManagedBackupIdentity(t *testing.T) {
	schedule := validSchedule()
	scheduled := time.Date(2026, 8, 20, 1, 2, 3, 0, time.UTC)
	expected, err := RenderBackup(schedule, scheduled)
	require.NoError(t, err)

	actual := expected.DeepCopy()
	actual.Annotations[ScheduledTimeAnnotation] = "2026-08-20T01:02:03.000000000+00:00"
	parsed, err := ValidateManagedBackup(schedule, actual)
	require.NoError(t, err)
	assert.True(t, parsed.Equal(scheduled))
	require.NoError(t, MatchesRenderedBackup(schedule, expected, actual))
	explicitSnapshot := actual.DeepCopy()
	explicitSnapshot.Spec.Mode = brv1alpha1.BackupModeSnapshot
	require.NoError(t, MatchesRenderedBackup(schedule, expected, explicitSnapshot))
	expectedSnapshot := expected.DeepCopy()
	expectedSnapshot.Spec.Mode = brv1alpha1.BackupModeSnapshot
	require.NoError(t, MatchesRenderedBackup(schedule, expectedSnapshot, actual))

	offset := expected.DeepCopy()
	offset.Annotations[ScheduledTimeAnnotation] = "2026-08-19T18:02:03-07:00"
	_, err = ValidateManagedBackup(schedule, offset)
	require.Error(t, err)

	// The schedule-name annotation is emitted for diagnostics, but UID plus
	// deterministic identity is authoritative when reading existing Backups.
	withoutInformationalName := expected.DeepCopy()
	delete(withoutInformationalName.Annotations, ScheduleNameAnnotation)
	_, err = ValidateManagedBackup(schedule, withoutInformationalName)
	require.NoError(t, err)
	withoutInformationalName.Annotations[ScheduleNameAnnotation] = "old-schedule-name"
	_, err = ValidateManagedBackup(schedule, withoutInformationalName)
	require.NoError(t, err)

	oldTarget := expected.DeepCopy()
	oldTarget.Spec.BR.Cluster = "previous-cluster"
	_, err = ValidateManagedBackup(schedule, oldTarget)
	require.Error(t, err)

	tests := []struct {
		name   string
		mutate func(*brv1alpha1.Backup)
	}{
		{name: "UID", mutate: func(b *brv1alpha1.Backup) { b.Labels[ScheduleUIDLabel] = "foreign" }},
		{name: "deterministic name", mutate: func(b *brv1alpha1.Backup) { b.Name = "collision" }},
		{name: "target cluster", mutate: func(b *brv1alpha1.Backup) { b.Spec.BR.Cluster = "foreign" }},
		{name: "target namespace", mutate: func(b *brv1alpha1.Backup) { b.Spec.BR.ClusterNamespace = "foreign" }},
		{name: "explicit schedule namespace", mutate: func(b *brv1alpha1.Backup) { b.Spec.BR.ClusterNamespace = schedule.Namespace }},
		{name: "mode", mutate: func(b *brv1alpha1.Backup) { b.Spec.Mode = brv1alpha1.BackupModeLog }},
		{name: "noncanonical destination", mutate: func(b *brv1alpha1.Backup) { b.Spec.S3.Prefix = "base//" + b.Name }},
		{name: "multiple providers", mutate: func(b *brv1alpha1.Backup) { b.Spec.Gcs = &brv1alpha1.GcsStorageProvider{} }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backup := expected.DeepCopy()
			tt.mutate(backup)
			_, err := ValidateManagedBackup(schedule, backup)
			assert.Error(t, err)
		})
	}

	changedDestination := expected.DeepCopy()
	changedDestination.Spec.S3.Bucket = "new-bucket"
	changedDestination.Spec.S3.Prefix = "new-prefix/" + changedDestination.Name
	require.NoError(t, MatchesRenderedBackup(schedule, expected, changedDestination))

	invalidDestinations := map[string]brv1alpha1.StorageProvider{
		"S3 bucket": {
			S3: &brv1alpha1.S3StorageProvider{Prefix: "base/" + expected.Name},
		},
		"GCS project": {
			Gcs: &brv1alpha1.GcsStorageProvider{Bucket: "bucket", Prefix: "base/" + expected.Name},
		},
		"GCS bucket": {
			Gcs: &brv1alpha1.GcsStorageProvider{ProjectId: "project", Prefix: "base/" + expected.Name},
		},
		"Azure Blob container": {
			Azblob: &brv1alpha1.AzblobStorageProvider{Prefix: "base/" + expected.Name},
		},
		"Local mount": {
			Local: &brv1alpha1.LocalStorageProvider{
				Volume:      corev1.Volume{Name: "data"},
				VolumeMount: corev1.VolumeMount{Name: "other", MountPath: "/data"},
				Prefix:      "base/" + expected.Name,
			},
		},
	}
	for name, provider := range invalidDestinations {
		t.Run("invalid "+name, func(t *testing.T) {
			backup := expected.DeepCopy()
			backup.Spec.StorageProvider = provider
			_, err := ValidateManagedBackup(schedule, backup)
			assert.Error(t, err)
		})
	}
}

func TestClassifyBackup(t *testing.T) {
	condition := func(conditionType brv1alpha1.BackupConditionType, status metav1.ConditionStatus) metav1.Condition {
		return metav1.Condition{Type: string(conditionType), Status: status}
	}
	tests := []struct {
		name       string
		conditions []metav1.Condition
		phase      brv1alpha1.BackupConditionType
		want       BackupTerminalState
	}{
		{name: "empty is active", want: BackupActive},
		{name: "phase ignored", phase: brv1alpha1.BackupComplete, want: BackupActive},
		{name: "complete", conditions: []metav1.Condition{condition(brv1alpha1.BackupComplete, metav1.ConditionTrue)}, want: BackupSucceeded},
		{name: "failed", conditions: []metav1.Condition{condition(brv1alpha1.BackupFailed, metav1.ConditionTrue)}, want: BackupFailed},
		{name: "invalid", conditions: []metav1.Condition{condition(brv1alpha1.BackupInvalid, metav1.ConditionTrue)}, want: BackupInvalid},
		{name: "false terminal is active", conditions: []metav1.Condition{condition(brv1alpha1.BackupComplete, metav1.ConditionFalse)}, want: BackupActive},
		{name: "unknown is ambiguous", conditions: []metav1.Condition{condition(brv1alpha1.BackupComplete, metav1.ConditionUnknown)}, want: BackupAmbiguous},
		{name: "contradictory types", conditions: []metav1.Condition{condition(brv1alpha1.BackupComplete, metav1.ConditionTrue), condition(brv1alpha1.BackupFailed, metav1.ConditionTrue)}, want: BackupAmbiguous},
		{name: "contradictory duplicates", conditions: []metav1.Condition{condition(brv1alpha1.BackupComplete, metav1.ConditionTrue), condition(brv1alpha1.BackupComplete, metav1.ConditionFalse)}, want: BackupAmbiguous},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backup := &brv1alpha1.Backup{Status: brv1alpha1.BackupStatus{Phase: tt.phase, Conditions: tt.conditions}}
			assert.Equal(t, tt.want, ClassifyBackup(backup))
		})
	}
}

func TestStatusOwnership(t *testing.T) {
	now := metav1.NewTime(time.Date(2026, 8, 20, 1, 2, 3, 0, time.UTC))
	schedule := validSchedule()
	schedule.Generation = 7
	schedule.Status.LastCompact = "legacy"
	schedule.Status.Conditions = []metav1.Condition{{
		Type:               "LegacyReady",
		Status:             metav1.ConditionTrue,
		Reason:             ReasonReconciled,
		ObservedGeneration: 6,
		LastTransitionTime: now,
	}}

	changed := ApplySchedulingStatus(schedule, &SchedulingStatus{
		LastScheduleTime: &now,
		LastBackup:       "backup",
		LastBackupTime:   &now,
		Condition: metav1.Condition{
			Status:             metav1.ConditionTrue,
			Reason:             ReasonReconciled,
			Message:            "scheduled",
			LastTransitionTime: now,
		},
	})
	assert.True(t, changed)
	assert.Equal(t, "legacy", schedule.Status.LastCompact)
	legacy := findCondition(schedule.Status.Conditions, "LegacyReady")
	require.NotNil(t, legacy)
	assert.EqualValues(t, 6, legacy.ObservedGeneration)
	scheduling := findCondition(schedule.Status.Conditions, ConditionSchedulingReady)
	require.NotNil(t, scheduling)
	assert.EqualValues(t, 7, scheduling.ObservedGeneration)
}

func TestConditionMessageFitsAPISchema(t *testing.T) {
	message := strings.Repeat("界", conditionMessageLimit+1) + string([]byte{0xff})
	condition := NewCondition(
		ConditionSchedulingReady,
		metav1.ConditionFalse,
		ReasonInvalidSpec,
		message,
		1,
		time.Date(2026, 8, 20, 1, 2, 3, 0, time.UTC),
	)
	assert.True(t, utf8.ValidString(condition.Message))
	assert.Equal(t, conditionMessageLimit, utf8.RuneCountInString(condition.Message))
}

func TestStatusUpdaterReturnsConflictForFullReconcile(t *testing.T) {
	key := client.ObjectKey{Namespace: "backups", Name: "daily.backup-with-long-name"}
	stored := validSchedule()
	stored.ResourceVersion = "1"
	stored.Generation = 3

	reader := &memoryStatusReader{get: func(_ context.Context, _ client.ObjectKey, obj client.Object) error {
		out := obj.(*brv1alpha1.BackupSchedule)
		*out = *stored.DeepCopy()
		return nil
	}}
	patches := 0
	writer := &memoryStatusWriter{patch: func(_ context.Context, obj client.Object, patch client.Patch) error {
		patches++
		data, err := patch.Data(obj)
		require.NoError(t, err)
		assert.Contains(t, string(data), `"resourceVersion"`)
		incoming := obj.(*brv1alpha1.BackupSchedule)
		if patches == 1 {
			stored.ResourceVersion = "2"
			stored.Status.Conditions = append(stored.Status.Conditions, metav1.Condition{
				Type:               "LegacyReady",
				Status:             metav1.ConditionTrue,
				Reason:             ReasonReconciled,
				LastTransitionTime: metav1.NewTime(time.Date(2026, 8, 20, 1, 0, 0, 0, time.UTC)),
			})
			return apierrors.NewConflict(
				schema.GroupResource{Group: brv1alpha1.SchemeGroupVersion.Group, Resource: "backupschedules"},
				incoming.Name,
				errors.New("injected conflict"),
			)
		}
		*stored = *incoming.DeepCopy()
		return nil
	}}

	mutations := 0
	updater := &StatusUpdater{Reader: reader, Writer: writer}
	mutation := func(latest *brv1alpha1.BackupSchedule) (bool, error) {
		mutations++
		now := metav1.NewTime(time.Date(2026, 8, 20, 2, 0, 0, 0, time.UTC))
		return ApplySchedulingStatus(latest, &SchedulingStatus{
			LastScheduleTime: &now,
			Condition: metav1.Condition{
				Status:             metav1.ConditionTrue,
				Reason:             ReasonReconciled,
				LastTransitionTime: now,
			},
		}), nil
	}
	err := updater.Update(context.Background(), key, stored.UID, stored.Generation, mutation)
	require.Error(t, err)
	assert.True(t, apierrors.IsConflict(err))
	assert.Equal(t, 1, reader.calls)
	assert.Equal(t, 1, patches)
	assert.Equal(t, 1, mutations)

	// A later full reconciliation explicitly calls Update again. That call
	// direct-reads resourceVersion 2 and preserves the unrelated status write that won
	// the first race.
	err = updater.Update(context.Background(), key, stored.UID, stored.Generation, mutation)
	require.NoError(t, err)
	assert.Equal(t, 2, reader.calls)
	assert.Equal(t, 2, patches)
	assert.Equal(t, 2, mutations)
	assert.NotNil(t, findCondition(stored.Status.Conditions, "LegacyReady"))
	assert.NotNil(t, findCondition(stored.Status.Conditions, ConditionSchedulingReady))
}

func TestStatusUpdaterRejectsStaleUIDOrGeneration(t *testing.T) {
	stored := validSchedule()
	stored.Generation = 4
	reader := &memoryStatusReader{get: func(_ context.Context, _ client.ObjectKey, obj client.Object) error {
		out := obj.(*brv1alpha1.BackupSchedule)
		*out = *stored.DeepCopy()
		return nil
	}}
	patches := 0
	writer := &memoryStatusWriter{patch: func(context.Context, client.Object, client.Patch) error {
		patches++
		return nil
	}}
	mutations := 0
	updater := &StatusUpdater{Reader: reader, Writer: writer}
	err := updater.Update(
		context.Background(),
		client.ObjectKeyFromObject(stored),
		types.UID("old-incarnation"),
		3,
		func(*brv1alpha1.BackupSchedule) (bool, error) {
			mutations++
			return true, nil
		},
	)
	require.Error(t, err)
	var stale *StaleScheduleError
	require.ErrorAs(t, err, &stale)
	assert.Equal(t, 0, patches)
	assert.Equal(t, 0, mutations)
}

func validSchedule() *brv1alpha1.BackupSchedule {
	return &brv1alpha1.BackupSchedule{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "daily.backup-with-long-name",
			Namespace: "backups",
			UID:       types.UID("11111111-2222-3333-4444-555555555555"),
		},
		Spec: brv1alpha1.BackupScheduleSpec{
			Cluster:  corev1alpha1.ClusterReference{Name: "example"},
			Schedule: "0 * * * *",
			BackupTemplate: brv1alpha1.BackupSpec{
				BR: &brv1alpha1.BRConfig{Cluster: "example"},
				StorageProvider: brv1alpha1.StorageProvider{
					S3: &brv1alpha1.S3StorageProvider{
						Provider: brv1alpha1.S3StorageProviderTypeAWS,
						Bucket:   "bucket",
						Prefix:   "snapshots",
					},
				},
			},
		},
	}
}

func ptr[T any](value T) *T {
	return &value
}

func findCondition(conditions []metav1.Condition, conditionType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == conditionType {
			return &conditions[i]
		}
	}
	return nil
}

type memoryStatusReader struct {
	get   func(context.Context, client.ObjectKey, client.Object) error
	calls int
}

func (reader *memoryStatusReader) Get(
	ctx context.Context,
	key client.ObjectKey,
	obj client.Object,
	_ ...client.GetOption,
) error {
	reader.calls++
	return reader.get(ctx, key, obj)
}

func (reader *memoryStatusReader) List(
	context.Context,
	client.ObjectList,
	...client.ListOption,
) error {
	return errors.New("unexpected list")
}

type memoryStatusWriter struct {
	patch func(context.Context, client.Object, client.Patch) error
}

func (writer *memoryStatusWriter) Create(
	context.Context,
	client.Object,
	client.Object,
	...client.SubResourceCreateOption,
) error {
	return errors.New("unexpected create")
}

func (writer *memoryStatusWriter) Update(
	context.Context,
	client.Object,
	...client.SubResourceUpdateOption,
) error {
	return errors.New("unexpected update")
}

func (writer *memoryStatusWriter) Patch(
	ctx context.Context,
	obj client.Object,
	patch client.Patch,
	_ ...client.SubResourcePatchOption,
) error {
	return writer.patch(ctx, obj, patch)
}
