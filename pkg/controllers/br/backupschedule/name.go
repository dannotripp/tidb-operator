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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	brv1alpha1 "github.com/pingcap/tidb-operator/api/v2/br/v1alpha1"
)

const (
	// ScheduleUIDLabel associates a generated Backup with one immutable
	// BackupSchedule object incarnation.
	ScheduleUIDLabel = "tidb.pingcap.com/backup-schedule-uid"
	// ScheduleNameAnnotation records the human-readable schedule name. It is
	// informational and is not used instead of the UID for association.
	ScheduleNameAnnotation = "tidb.pingcap.com/backup-schedule-name"
	// ScheduledTimeAnnotation records the logical UTC occurrence rendered into
	// a generated Backup.
	ScheduledTimeAnnotation = "tidb.pingcap.com/scheduled-at"

	backupNamePrefixLength = 16
	backupUIDHashLength    = 16
	backupTimeFormat       = "20060102150405"
	conditionMessageLimit  = 32768
)

// BackupName returns the deterministic Backup name for an occurrence.
func BackupName(scheduleName string, scheduleUID types.UID, scheduledTime time.Time) string {
	prefix := strings.ReplaceAll(scheduleName, ".", "-")
	if len(prefix) > backupNamePrefixLength {
		prefix = prefix[:backupNamePrefixLength]
	}
	prefix = strings.TrimRight(prefix, "-")

	hash := sha256.Sum256([]byte(scheduleUID))
	uidHash := hex.EncodeToString(hash[:])[:backupUIDHashLength]
	timestamp := scheduledTime.UTC().Format(backupTimeFormat)
	return fmt.Sprintf("%s-%s-%s", prefix, uidHash, timestamp)
}

// CanonicalScheduledTime returns the controller's canonical annotation value.
func CanonicalScheduledTime(scheduledTime time.Time) string {
	return scheduledTime.UTC().Truncate(time.Second).Format(time.RFC3339)
}

// SetManagedMetadata writes the required controller metadata, overriding any
// conflicting values already present on the Backup.
func SetManagedMetadata(
	backup *brv1alpha1.Backup,
	schedule *brv1alpha1.BackupSchedule,
	scheduledTime time.Time,
) {
	if backup.Labels == nil {
		backup.Labels = make(map[string]string)
	}
	if backup.Annotations == nil {
		backup.Annotations = make(map[string]string)
	}

	backup.Labels[ScheduleUIDLabel] = string(schedule.UID)
	backup.Annotations[ScheduleNameAnnotation] = schedule.Name
	backup.Annotations[ScheduledTimeAnnotation] = CanonicalScheduledTime(scheduledTime)
}

// ParseScheduledTime parses a managed Backup's occurrence and requires a UTC
// representation. Equivalent RFC3339 zero-offset representations are accepted.
func ParseScheduledTime(backup *brv1alpha1.Backup) (time.Time, error) {
	if backup == nil {
		return time.Time{}, fmt.Errorf("backup is nil")
	}
	value, ok := backup.Annotations[ScheduledTimeAnnotation]
	if !ok || value == "" {
		return time.Time{}, fmt.Errorf("annotation %q is required", ScheduledTimeAnnotation)
	}

	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse annotation %q: %w", ScheduledTimeAnnotation, err)
	}
	_, offset := parsed.Zone()
	if offset != 0 {
		return time.Time{}, fmt.Errorf("annotation %q must use a zero UTC offset", ScheduledTimeAnnotation)
	}
	return parsed.UTC(), nil
}

// NewCondition builds a BackupSchedule readiness condition with the required
// generation and transition time fields.
func NewCondition(
	conditionType string,
	status metav1.ConditionStatus,
	reason string,
	message string,
	generation int64,
	transitionTime time.Time,
) metav1.Condition {
	return metav1.Condition{
		Type:               conditionType,
		Status:             status,
		Reason:             reason,
		Message:            truncateConditionMessage(message),
		ObservedGeneration: generation,
		LastTransitionTime: metav1.NewTime(transitionTime.UTC()),
	}
}

func truncateConditionMessage(message string) string {
	message = strings.ToValidUTF8(message, "\uFFFD")
	if utf8.RuneCountInString(message) <= conditionMessageLimit {
		return message
	}
	return string([]rune(message)[:conditionMessageLimit])
}
