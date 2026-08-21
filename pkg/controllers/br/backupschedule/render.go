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
	"path"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	brv1alpha1 "github.com/pingcap/tidb-operator/api/v2/br/v1alpha1"
)

// StorageKind identifies one supported Backup storage provider.
type StorageKind string

const (
	StorageKindS3     StorageKind = "s3"
	StorageKindGCS    StorageKind = "gcs"
	StorageKindAzblob StorageKind = "azblob"
	StorageKindLocal  StorageKind = "local"
)

// NormalizeStoragePrefix implements the schedule path policy. The result is a
// relative, slash-separated prefix. Parent traversal is rejected before path
// joining, including traversal surrounded by repeated slashes.
func NormalizeStoragePrefix(prefix string) (string, error) {
	parts := strings.Split(strings.Trim(prefix, "/"), "/")
	normalized := make([]string, 0, len(parts))
	for _, part := range parts {
		switch part {
		case "", ".":
			continue
		case "..":
			return "", fmt.Errorf("storage prefix %q contains a parent path component", prefix)
		default:
			normalized = append(normalized, part)
		}
	}
	return strings.Join(normalized, "/"), nil
}

// StoragePrefix returns the configured provider kind and prefix. Exactly one
// provider must be configured.
func StoragePrefix(provider brv1alpha1.StorageProvider) (StorageKind, string, error) {
	kind, count := StorageKind(""), 0
	prefix := ""
	if provider.S3 != nil {
		kind, prefix, count = StorageKindS3, provider.S3.Prefix, count+1
	}
	if provider.Gcs != nil {
		kind, prefix, count = StorageKindGCS, provider.Gcs.Prefix, count+1
	}
	if provider.Azblob != nil {
		kind, prefix, count = StorageKindAzblob, provider.Azblob.Prefix, count+1
	}
	if provider.Local != nil {
		kind, prefix, count = StorageKindLocal, provider.Local.Prefix, count+1
	}
	if count != 1 {
		return "", "", fmt.Errorf("exactly one storage provider is required, got %d", count)
	}
	return kind, prefix, nil
}

func setStoragePrefix(provider *brv1alpha1.StorageProvider, kind StorageKind, prefix string) {
	switch kind {
	case StorageKindS3:
		provider.S3.Prefix = prefix
	case StorageKindGCS:
		provider.Gcs.Prefix = prefix
	case StorageKindAzblob:
		provider.Azblob.Prefix = prefix
	case StorageKindLocal:
		provider.Local.Prefix = prefix
	}
}

// AppendBackupNameToStorage normalizes the template prefix and appends the
// deterministic Backup name for the configured provider.
func AppendBackupNameToStorage(provider *brv1alpha1.StorageProvider, backupName string) error {
	if provider == nil {
		return fmt.Errorf("storage provider is nil")
	}
	kind, prefix, err := StoragePrefix(*provider)
	if err != nil {
		return err
	}
	normalized, err := NormalizeStoragePrefix(prefix)
	if err != nil {
		return err
	}
	setStoragePrefix(provider, kind, path.Join(normalized, backupName))
	return nil
}

// RenderBackup deep-copies the schedule template and renders the exact Backup
// for one scheduled occurrence.
func RenderBackup(schedule *brv1alpha1.BackupSchedule, scheduledTime time.Time) (*brv1alpha1.Backup, error) {
	if schedule == nil {
		return nil, fmt.Errorf("backup schedule is nil")
	}
	if schedule.UID == "" {
		return nil, fmt.Errorf("backup schedule UID is required")
	}
	target, err := ResolveScheduleTarget(schedule)
	if err != nil {
		return nil, err
	}

	name := BackupName(schedule.Name, schedule.UID, scheduledTime)
	backup := &brv1alpha1.Backup{
		TypeMeta: metav1.TypeMeta{
			APIVersion: brv1alpha1.SchemeGroupVersion.String(),
			Kind:       "Backup",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: schedule.Namespace,
		},
		Spec: *schedule.Spec.BackupTemplate.DeepCopy(),
	}
	projectScheduleTarget(backup, target.Cluster)
	if err := AppendBackupNameToStorage(&backup.Spec.StorageProvider, name); err != nil {
		return nil, fmt.Errorf("render backup storage: %w", err)
	}
	SetManagedMetadata(backup, schedule, scheduledTime)
	if err := validateBackupStatic(backup); err != nil {
		return nil, fmt.Errorf("validate rendered backup: %w", err)
	}
	return backup, nil
}

func projectScheduleTarget(backup *brv1alpha1.Backup, cluster string) {
	if backup.Spec.BR == nil {
		backup.Spec.BR = &brv1alpha1.BRConfig{}
	}
	backup.Spec.BR.Cluster = cluster
	backup.Spec.BR.ClusterNamespace = ""
}

// ValidateManagedDestination requires a canonical provider prefix whose final
// component is the deterministic Backup name.
func ValidateManagedDestination(backup *brv1alpha1.Backup) error {
	if err := validateStorageProviderStatic(
		backup.Namespace,
		backup.Name,
		backup.Spec.StorageProvider,
	); err != nil {
		return err
	}
	kind, prefix, err := StoragePrefix(backup.Spec.StorageProvider)
	if err != nil {
		return err
	}
	normalized, err := NormalizeStoragePrefix(prefix)
	if err != nil {
		return err
	}
	if prefix != normalized {
		return fmt.Errorf("%s storage prefix %q is not canonical", kind, prefix)
	}
	if normalized == "" || path.Base(normalized) != backup.Name {
		return fmt.Errorf("%s storage prefix %q does not end with backup name %q", kind, prefix, backup.Name)
	}
	return nil
}

// ValidateManagedBackup validates the immutable association and safety
// properties used before recovery. It intentionally does not
// compare the full current template, which may have changed since an older
// Backup was rendered.
func ValidateManagedBackup(
	schedule *brv1alpha1.BackupSchedule,
	backup *brv1alpha1.Backup,
) (time.Time, error) {
	if schedule == nil || backup == nil {
		return time.Time{}, fmt.Errorf("backup schedule and backup are required")
	}
	if backup.Namespace != schedule.Namespace {
		return time.Time{}, fmt.Errorf("backup namespace %q does not match schedule namespace %q", backup.Namespace, schedule.Namespace)
	}
	if backup.Labels[ScheduleUIDLabel] != string(schedule.UID) {
		return time.Time{}, fmt.Errorf("backup schedule UID label does not match")
	}
	scheduledTime, err := ParseScheduledTime(backup)
	if err != nil {
		return time.Time{}, err
	}
	if expected := BackupName(schedule.Name, schedule.UID, scheduledTime); backup.Name != expected {
		return time.Time{}, fmt.Errorf("backup name %q does not match deterministic name %q", backup.Name, expected)
	}

	scheduleTarget, err := ResolveScheduleTarget(schedule)
	if err != nil {
		return time.Time{}, err
	}
	if backup.Spec.BR == nil || backup.Spec.BR.ClusterNamespace != "" {
		return time.Time{}, fmt.Errorf("managed backup spec.br.clusterNamespace must be empty")
	}
	backupTarget, err := ResolveBackupTarget(backup)
	if err != nil {
		return time.Time{}, err
	}
	if backupTarget != scheduleTarget {
		return time.Time{}, fmt.Errorf(
			"backup target %s/%s does not match schedule target %s/%s",
			backupTarget.Namespace,
			backupTarget.Cluster,
			scheduleTarget.Namespace,
			scheduleTarget.Cluster,
		)
	}
	if backup.Spec.Mode != "" && backup.Spec.Mode != brv1alpha1.BackupModeSnapshot {
		return time.Time{}, fmt.Errorf("backup mode %q is not snapshot", backup.Spec.Mode)
	}
	if err := ValidateManagedDestination(backup); err != nil {
		return time.Time{}, err
	}
	return scheduledTime, nil
}

// MatchesRenderedBackup checks whether an existing Backup safely represents
// the rendered occurrence. The existing Backup's canonical destination is
// authoritative so a later template destination edit cannot erase recovery
// evidence after a successful create.
func MatchesRenderedBackup(
	schedule *brv1alpha1.BackupSchedule,
	expected *brv1alpha1.Backup,
	actual *brv1alpha1.Backup,
) error {
	actualTime, err := ValidateManagedBackup(schedule, actual)
	if err != nil {
		return err
	}
	expectedTime, err := ParseScheduledTime(expected)
	if err != nil {
		return fmt.Errorf("expected backup metadata: %w", err)
	}
	if !actualTime.Equal(expectedTime) {
		return fmt.Errorf("scheduled time does not match rendered backup")
	}
	if actual.Name != expected.Name || actual.Namespace != expected.Namespace {
		return fmt.Errorf("backup identity does not match rendered backup")
	}
	if expected.Spec.Mode != "" && expected.Spec.Mode != brv1alpha1.BackupModeSnapshot {
		return fmt.Errorf("rendered backup mode %q is not snapshot", expected.Spec.Mode)
	}
	actualTarget, err := ResolveBackupTarget(actual)
	if err != nil {
		return err
	}
	expectedTarget, err := ResolveBackupTarget(expected)
	if err != nil {
		return err
	}
	if actualTarget != expectedTarget {
		return fmt.Errorf("backup target does not match rendered backup")
	}
	return nil
}
