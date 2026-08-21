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

	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	brv1alpha1 "github.com/pingcap/tidb-operator/api/v2/br/v1alpha1"

	"github.com/pingcap/tidb-operator/v2/pkg/utils/k8s"
)

const (
	SchedulingControllerName = "backup-schedule-scheduling"
	RetentionControllerName  = "backup-schedule-retention"
)

// Setup installs the shared indexes and registers independent scheduling and
// retention controllers. Backup events are mapped explicitly because generated
// Backups intentionally have no BackupSchedule owner reference.
func Setup(ctx context.Context, mgr manager.Manager, managerClient client.Client) error {
	if err := RegisterIndexes(ctx, mgr); err != nil {
		return err
	}

	apiReader := mgr.GetAPIReader()
	mapper := BackupEventMapper{Reader: managerClient}
	locks := NewTargetLocks()
	scheduling := NewSchedulingReconciler(managerClient, apiReader, clock.RealClock{}, locks)
	retention := NewRetentionReconciler(apiReader, managerClient)

	if err := ctrl.NewControllerManagedBy(mgr).
		Named(SchedulingControllerName).
		For(&brv1alpha1.BackupSchedule{}).
		Watches(
			&brv1alpha1.Backup{},
			handler.EnqueueRequestsFromMapFunc(mapper.SchedulingRequests),
		).
		WithOptions(controller.Options{RateLimiter: k8s.NewRateLimiter()}).
		Complete(scheduling); err != nil {
		return fmt.Errorf("create %s controller: %w", SchedulingControllerName, err)
	}

	if err := ctrl.NewControllerManagedBy(mgr).
		Named(RetentionControllerName).
		For(&brv1alpha1.BackupSchedule{}).
		Watches(
			&brv1alpha1.Backup{},
			handler.EnqueueRequestsFromMapFunc(mapper.RetentionRequests),
		).
		WithOptions(controller.Options{RateLimiter: k8s.NewRateLimiter()}).
		Complete(retention); err != nil {
		return fmt.Errorf("create %s controller: %w", RetentionControllerName, err)
	}

	return nil
}
