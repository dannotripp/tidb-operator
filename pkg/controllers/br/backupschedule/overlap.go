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
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"

	brv1alpha1 "github.com/pingcap/tidb-operator/api/v2/br/v1alpha1"
)

// FindBlockingBackup returns a deterministic nonterminal snapshot Backup for
// target, including manual Backups and Backups created by another schedule.
// Log Backups and safely terminal snapshot Backups do not block.
func FindBlockingBackup(
	ctx context.Context,
	reader client.Reader,
	target Target,
	ignore client.ObjectKey,
) (*brv1alpha1.Backup, error) {
	if reader == nil {
		return nil, fmt.Errorf("backup reader is required")
	}

	backups := &brv1alpha1.BackupList{}
	// A Backup's effective cluster namespace can differ from the namespace of
	// the Backup object, so target matching is deliberately cluster-wide.
	if err := reader.List(ctx, backups); err != nil {
		return nil, fmt.Errorf("list Backups for target %s/%s: %w", target.Namespace, target.Cluster, err)
	}

	var blocker *brv1alpha1.Backup
	for i := range backups.Items {
		backup := &backups.Items[i]
		if client.ObjectKeyFromObject(backup) == ignore {
			continue
		}
		// The existing Backup manager treats every mode other than explicit log
		// as a snapshot. Unknown values must therefore block conservatively too.
		if backup.Spec.Mode == brv1alpha1.BackupModeLog {
			continue
		}
		backupTarget, err := ResolveBackupTarget(backup)
		if err != nil || backupTarget != target {
			continue
		}
		switch ClassifyBackup(backup) {
		case BackupSucceeded, BackupFailed, BackupInvalid:
			continue
		case BackupActive, BackupAmbiguous:
		}
		if blocker == nil || objectKeyLess(client.ObjectKeyFromObject(backup), client.ObjectKeyFromObject(blocker)) {
			blocker = backup.DeepCopy()
		}
	}
	return blocker, nil
}

func objectKeyLess(left, right client.ObjectKey) bool {
	if left.Namespace != right.Namespace {
		return left.Namespace < right.Namespace
	}
	return left.Name < right.Name
}
