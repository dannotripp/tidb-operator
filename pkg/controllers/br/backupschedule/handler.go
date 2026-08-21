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
	"sort"

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	brv1alpha1 "github.com/pingcap/tidb-operator/api/v2/br/v1alpha1"
)

const (
	backupScheduleTargetIndex = "backupschedule.br.pingcap.com/target"
)

// RegisterIndexes installs the BackupSchedule target index used by the
// scheduling controller. It must be called exactly once for a manager.
func RegisterIndexes(ctx context.Context, mgr manager.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(
		ctx,
		&brv1alpha1.BackupSchedule{},
		backupScheduleTargetIndex,
		indexBackupScheduleByTarget,
	); err != nil {
		return fmt.Errorf("index BackupSchedules by target: %w", err)
	}
	return nil
}

func indexBackupScheduleByTarget(obj client.Object) []string {
	schedule, ok := obj.(*brv1alpha1.BackupSchedule)
	if !ok || schedule == nil {
		return nil
	}
	target, err := ResolveScheduleTarget(schedule)
	if err != nil {
		return nil
	}
	return []string{targetIndexValue(target)}
}

func targetIndexValue(target Target) string {
	// Kubernetes namespaces cannot contain '/', so this is unambiguous even
	// before the referenced cluster name is validated by its own API.
	return target.Namespace + "/" + target.Cluster
}

// BackupEventMapper maps Backup events to the scheduling controller queue.
// EnqueueRequestsFromMapFunc invokes this method for both old and new objects
// on updates.
type BackupEventMapper struct {
	Reader client.Reader
}

// SchedulingRequests wakes every schedule targeting the Backup's effective
// TiDB cluster, including for manual and cross-namespace Backup objects.
func (mapper BackupEventMapper) SchedulingRequests(ctx context.Context, obj client.Object) []reconcile.Request {
	backup, ok := obj.(*brv1alpha1.Backup)
	if !ok || backup == nil || mapper.Reader == nil {
		return nil
	}
	target, err := ResolveBackupTarget(backup)
	if err != nil {
		return nil
	}

	schedules := &brv1alpha1.BackupScheduleList{}
	if err := mapper.Reader.List(
		ctx,
		schedules,
		client.MatchingFields{backupScheduleTargetIndex: targetIndexValue(target)},
	); err != nil {
		logr.FromContextOrDiscard(ctx).Error(
			err,
			"map Backup event to target BackupSchedules",
			"backup",
			client.ObjectKeyFromObject(backup),
		)
		return nil
	}
	return scheduleRequests(schedules.Items)
}

func scheduleRequests(schedules []brv1alpha1.BackupSchedule) []reconcile.Request {
	requests := make([]reconcile.Request, 0, len(schedules))
	for i := range schedules {
		requests = append(requests, reconcile.Request{
			NamespacedName: client.ObjectKeyFromObject(&schedules[i]),
		})
	}
	sort.Slice(requests, func(i, j int) bool {
		if requests[i].Namespace != requests[j].Namespace {
			return requests[i].Namespace < requests[j].Namespace
		}
		return requests[i].Name < requests[j].Name
	})
	return requests
}
