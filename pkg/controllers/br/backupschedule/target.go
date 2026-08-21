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

	utilvalidation "k8s.io/apimachinery/pkg/util/validation"

	brv1alpha1 "github.com/pingcap/tidb-operator/api/v2/br/v1alpha1"
)

// Target identifies the TiDB cluster against which a Backup runs.
type Target struct {
	Namespace string
	Cluster   string
}

// ResolveScheduleTarget returns the immutable same-namespace target declared
// by spec.cluster. The nested template target is accepted only as a matching
// compatibility projection and is never authoritative.
func ResolveScheduleTarget(schedule *brv1alpha1.BackupSchedule) (Target, error) {
	if schedule == nil {
		return Target{}, fmt.Errorf("backup schedule is nil")
	}
	cluster := schedule.Spec.Cluster.Name
	if cluster == "" {
		return Target{}, fmt.Errorf("spec.cluster.name is required")
	}
	if problems := utilvalidation.IsDNS1123Subdomain(cluster); len(problems) != 0 {
		return Target{}, fmt.Errorf("spec.cluster.name %q is invalid: %s", cluster, strings.Join(problems, "; "))
	}

	br := schedule.Spec.BackupTemplate.BR
	if br != nil {
		if br.Cluster != cluster {
			return Target{}, fmt.Errorf(
				"spec.backupTemplate.br.cluster %q must equal spec.cluster.name %q",
				br.Cluster,
				cluster,
			)
		}
		if br.ClusterNamespace != "" {
			return Target{}, fmt.Errorf("spec.backupTemplate.br.clusterNamespace must be empty")
		}
	}

	return Target{Namespace: schedule.Namespace, Cluster: cluster}, nil
}

// ResolveBackupTarget returns a Backup's effective target. Unlike
// ResolveScheduleTarget, this helper describes existing manual Backups too, so
// it does not reject a cross-namespace reference.
func ResolveBackupTarget(backup *brv1alpha1.Backup) (Target, error) {
	if backup == nil {
		return Target{}, fmt.Errorf("backup is nil")
	}
	if backup.Spec.BR == nil {
		return Target{}, fmt.Errorf("spec.br is required")
	}
	if backup.Spec.BR.Cluster == "" {
		return Target{}, fmt.Errorf("spec.br.cluster is required")
	}

	namespace := backup.Spec.BR.ClusterNamespace
	if namespace == "" {
		namespace = backup.Namespace
	}
	return Target{Namespace: namespace, Cluster: backup.Spec.BR.Cluster}, nil
}
