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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	brv1alpha1 "github.com/pingcap/tidb-operator/api/v2/br/v1alpha1"
)

// BackupTerminalState is derived only from canonical terminal conditions. The
// user-facing Phase field is deliberately ignored.
type BackupTerminalState string

const (
	BackupActive    BackupTerminalState = "Active"
	BackupSucceeded BackupTerminalState = "Complete"
	BackupFailed    BackupTerminalState = "Failed"
	BackupInvalid   BackupTerminalState = "Invalid"
	BackupAmbiguous BackupTerminalState = "Ambiguous"
)

// ClassifyBackup returns a fail-closed terminal classification. Unknown or
// contradictory terminal conditions are ambiguous and must not be treated as
// safely terminal by scheduling.
func ClassifyBackup(backup *brv1alpha1.Backup) BackupTerminalState {
	if backup == nil {
		return BackupAmbiguous
	}

	trueStates := make(map[BackupTerminalState]struct{}, 3)
	seenTypes := make(map[string]metav1.ConditionStatus, 3)
	for _, condition := range backup.Status.Conditions {
		state, relevant := terminalConditionState(condition.Type)
		if !relevant {
			continue
		}
		if condition.Status == metav1.ConditionUnknown {
			return BackupAmbiguous
		}
		if prior, seen := seenTypes[condition.Type]; seen && prior != condition.Status {
			return BackupAmbiguous
		}
		seenTypes[condition.Type] = condition.Status
		if condition.Status == metav1.ConditionTrue {
			trueStates[state] = struct{}{}
		}
	}

	if len(trueStates) == 0 {
		return BackupActive
	}
	if len(trueStates) != 1 {
		return BackupAmbiguous
	}
	for state := range trueStates {
		return state
	}
	return BackupAmbiguous
}

func terminalConditionState(conditionType string) (BackupTerminalState, bool) {
	switch conditionType {
	case string(brv1alpha1.BackupComplete):
		return BackupSucceeded, true
	case string(brv1alpha1.BackupFailed):
		return BackupFailed, true
	case string(brv1alpha1.BackupInvalid):
		return BackupInvalid, true
	default:
		return "", false
	}
}
