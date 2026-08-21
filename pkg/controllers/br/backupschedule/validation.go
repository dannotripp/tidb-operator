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
	"strings"
	"time"

	"github.com/robfig/cron/v3"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	brv1alpha1 "github.com/pingcap/tidb-operator/api/v2/br/v1alpha1"
	managerutil "github.com/pingcap/tidb-operator/v2/pkg/controllers/br/manager/util"
)

var supportedDescriptors = map[string]struct{}{
	"@yearly":   {},
	"@annually": {},
	"@monthly":  {},
	"@weekly":   {},
	"@daily":    {},
	"@midnight": {},
	"@hourly":   {},
}

const standardCronFieldCount = 5

// ParseSchedule parses the exact UTC cron grammar supported by BackupSchedule.
// The returned schedule is evaluated against the caller-provided UTC cursor.
func ParseSchedule(expression string) (cron.Schedule, error) {
	expression = strings.TrimSpace(expression)
	if expression == "" {
		return nil, fmt.Errorf("spec.schedule is required")
	}
	if strings.Contains(expression, "CRON_TZ=") || strings.Contains(expression, "TZ=") {
		return nil, fmt.Errorf("time-zone directives are not supported; schedules run in UTC")
	}

	if strings.HasPrefix(expression, "@") {
		if _, ok := supportedDescriptors[expression]; ok {
			parsed, err := parseUTCSchedule(expression)
			if err != nil {
				return nil, fmt.Errorf("parse spec.schedule %q: %w", expression, err)
			}
			return validateFutureOccurrence(expression, parsed)
		}
		fields := strings.Fields(expression)
		if len(fields) != 2 || fields[0] != "@every" {
			return nil, fmt.Errorf("unsupported cron descriptor %q", expression)
		}
		duration, err := time.ParseDuration(fields[1])
		if err != nil {
			return nil, fmt.Errorf("parse @every duration %q: %w", fields[1], err)
		}
		if duration < time.Minute {
			return nil, fmt.Errorf("@every duration must be at least 1m")
		}
		parsed, err := parseUTCSchedule(expression)
		if err != nil {
			return nil, fmt.Errorf("parse spec.schedule %q: %w", expression, err)
		}
		return validateFutureOccurrence(expression, parsed)
	}

	if fields := strings.Fields(expression); len(fields) != standardCronFieldCount {
		return nil, fmt.Errorf("spec.schedule must contain exactly five cron fields")
	}
	parsed, err := parseUTCSchedule(expression)
	if err != nil {
		return nil, fmt.Errorf("parse spec.schedule %q: %w", expression, err)
	}
	return validateFutureOccurrence(expression, parsed)
}

func parseUTCSchedule(expression string) (cron.Schedule, error) {
	// ParseStandard otherwise binds calendar schedules to time.Local. Prefixing
	// the already validated expression makes UTC part of the parsed schedule,
	// independent of the operator process's local time zone.
	return cron.ParseStandard("CRON_TZ=UTC " + expression)
}

func validateFutureOccurrence(expression string, schedule cron.Schedule) (cron.Schedule, error) {
	// Cron expressions have no year field. Probing from the start of a leap
	// year accepts February 29 while rejecting syntactically valid schedules,
	// such as February 30, that can never produce an occurrence.
	probe := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	if schedule.Next(probe).IsZero() {
		return nil, fmt.Errorf("spec.schedule %q has no future occurrence", expression)
	}
	return schedule, nil
}

// ValidateBackupSchedule validates the supported v2 subset before scheduling
// or retention performs side effects.
//
//nolint:gocyclo // Explicit field-by-field rejection keeps the supported subset auditable.
func ValidateBackupSchedule(schedule *brv1alpha1.BackupSchedule) error {
	if schedule == nil {
		return fmt.Errorf("backup schedule is nil")
	}
	if _, err := ParseSchedule(schedule.Spec.Schedule); err != nil {
		return err
	}
	if schedule.Spec.MaxBackups != nil {
		if *schedule.Spec.MaxBackups < 0 {
			return fmt.Errorf("spec.maxBackups must be nonnegative")
		}
	}
	if schedule.Spec.MaxReservedTime != nil {
		return fmt.Errorf("spec.maxReservedTime is not supported")
	}
	if schedule.Spec.LogBackupTemplate != nil {
		return fmt.Errorf("spec.logBackupTemplate is not supported")
	}
	if schedule.Spec.CompactSpan != nil {
		return fmt.Errorf("spec.compactSpan is not supported")
	}
	if schedule.Spec.CompactBackupTemplate != nil {
		return fmt.Errorf("spec.compactBackupTemplate is not supported")
	}
	if schedule.Spec.StorageClassName != nil {
		return fmt.Errorf("spec.storageClassName is not supported")
	}
	if schedule.Spec.StorageSize != "" {
		return fmt.Errorf("spec.storageSize is not supported")
	}
	if len(schedule.Spec.ImagePullSecrets) != 0 {
		return fmt.Errorf("spec.imagePullSecrets is not supported")
	}
	if schedule.Spec.BR != nil {
		return fmt.Errorf("schedule-level spec.br inheritance is not supported")
	}
	if providerCount(schedule.Spec.StorageProvider) != 0 {
		return fmt.Errorf("schedule-level storage provider inheritance is not supported")
	}

	template := &schedule.Spec.BackupTemplate
	if template.Mode != "" && template.Mode != brv1alpha1.BackupModeSnapshot {
		return fmt.Errorf("spec.backupTemplate.backupMode must be empty or snapshot")
	}
	if template.LogSubcommand != "" {
		return fmt.Errorf("spec.backupTemplate.logSubcommand is not supported")
	}
	if template.LogTruncateUntil != "" {
		return fmt.Errorf("spec.backupTemplate.logTruncateUntil is not supported")
	}
	if template.LogStop {
		return fmt.Errorf("spec.backupTemplate.logStop=true is not supported")
	}
	if _, err := ResolveScheduleTarget(schedule); err != nil {
		return err
	}
	if _, prefix, err := StoragePrefix(template.StorageProvider); err != nil {
		return fmt.Errorf("spec.backupTemplate: %w", err)
	} else if _, err := NormalizeStoragePrefix(prefix); err != nil {
		return fmt.Errorf("spec.backupTemplate storage prefix: %w", err)
	}

	candidate := &brv1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      schedule.Name + "-template",
			Namespace: schedule.Namespace,
		},
		Spec: *template.DeepCopy(),
	}
	projectScheduleTarget(candidate, schedule.Spec.Cluster.Name)
	if err := validateBackupStatic(candidate); err != nil {
		return fmt.Errorf("spec.backupTemplate: %w", err)
	}
	return nil
}

func validateBackupStatic(backup *brv1alpha1.Backup) error {
	if err := managerutil.ValidateBackup(backup, "", nil); err != nil {
		return err
	}
	if backup.Spec.Azblob == nil {
		return nil
	}

	azblob := backup.Spec.Azblob
	if azblob.Container == "" {
		return fmt.Errorf("container should be configured for BR in spec of %s/%s", backup.Namespace, backup.Name)
	}
	if azblob.SecretName == "" && (azblob.StorageAccount == "" || azblob.SasToken == "") {
		return fmt.Errorf(
			"azblob must configure secretName or both storageAccount and sasToken for BR in spec of %s/%s",
			backup.Namespace,
			backup.Name,
		)
	}
	return nil
}

func validateStorageProviderStatic(namespace, name string, provider brv1alpha1.StorageProvider) error {
	if _, _, err := StoragePrefix(provider); err != nil {
		return err
	}
	candidate := &brv1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: brv1alpha1.BackupSpec{
			BR:              &brv1alpha1.BRConfig{Cluster: "validation"},
			StorageProvider: *provider.DeepCopy(),
		},
	}
	return validateBackupStatic(candidate)
}

func providerCount(provider brv1alpha1.StorageProvider) int {
	count := 0
	if provider.S3 != nil {
		count++
	}
	if provider.Gcs != nil {
		count++
	}
	if provider.Azblob != nil {
		count++
	}
	if provider.Local != nil {
		count++
	}
	return count
}
