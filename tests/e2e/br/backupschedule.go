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

package br

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	brv1alpha1 "github.com/pingcap/tidb-operator/api/v2/br/v1alpha1"
	corev1alpha1 "github.com/pingcap/tidb-operator/api/v2/core/v1alpha1"
	"github.com/pingcap/tidb-operator/v2/pkg/client"
	backupschedulectrl "github.com/pingcap/tidb-operator/v2/pkg/controllers/br/backupschedule"
	brframework "github.com/pingcap/tidb-operator/v2/tests/e2e/br/framework"
	"github.com/pingcap/tidb-operator/v2/tests/e2e/label"
	"github.com/pingcap/tidb-operator/v2/tests/e2e/utils/db/blockwriter"
	utilimage "github.com/pingcap/tidb-operator/v2/tests/e2e/utils/image"
	"github.com/pingcap/tidb-operator/v2/tests/e2e/utils/k8s"
	"github.com/pingcap/tidb-operator/v2/tests/e2e/utils/waiter"
)

const (
	backupScheduleControlPlaneTimeout = 2 * time.Minute
	backupScheduleCreationTimeout     = 3 * time.Minute
	backupSchedulePollInterval        = time.Second
	backupScheduleOperatorNamespace   = "tidb-admin"
)

var _ = ginkgo.Describe(
	"BackupSchedule",
	label.KindBR,
	label.KindBackupSchedule,
	label.P0,
	ginkgo.Serial,
	func() {
		f := brframework.NewFramework("backup-schedule")
		f.SetupBootstrapSQL("SET PASSWORD FOR 'root'@'%' = 'pingcap';")

		ginkgo.It("validates snapshot scheduling and recovery", func(ctx context.Context) {
			const clusterName = "backup-schedule-cluster"
			const databaseName = "backupschedule"
			namespace := f.Namespace.Name
			tidbVersion := utilimage.TiDBLatest

			ginkgo.By("Initializing isolated MinIO storage")
			f.Must(f.Storage.Init(ctx, namespace, "12345678", "12345678"))
			ginkgo.DeferCleanup(func(ctx context.Context) {
				f.Must(f.Storage.Clean(ctx, namespace))
			})

			ginkgo.By("Creating the TiDB cluster and Backup RBAC")
			f.Must(createTidbCluster(f, clusterName, tidbVersion, false, false, true))
			f.Must(waiter.WaitForClusterReady(ctx, f.Client, namespace, clusterName, tidbReadyTimeout))
			f.Must(createRBAC(f))

			ginkgo.By("Seeding data that must survive a scheduled Backup and restore")
			sourceHost, sourcePort, stopSourceForward, err := k8s.ForwardOnePort(
				f.PortForwarder,
				namespace,
				getTiDBServiceResourceName(clusterName),
				corev1alpha1.DefaultTiDBPortClient,
			)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer stopSourceForward()
			sourceDomain := fmt.Sprintf("%s:%d", sourceHost, sourcePort)
			gomega.Expect(initDatabase(sourceDomain, databaseName)).To(gomega.Succeed())
			sourceDSN := getDefaultDSN(sourceDomain, databaseName)
			writer := blockwriter.New(
				blockwriter.WithTableNum(1),
				blockwriter.WithRecordNum(4),
				blockwriter.WithBatchNum(4),
				blockwriter.WithBatchSize(128),
			)
			gomega.Expect(writer.Write(ctx, sourceDSN)).To(gomega.Succeed())

			schedule := newE2EBackupSchedule(f, namespace, "native-snapshot", clusterName)
			ginkgo.By("Creating the BackupSchedule with retention disabled")
			gomega.Expect(f.Client.Create(ctx, schedule)).To(gomega.Succeed())

			schedule = waitForE2EBackupScheduleReady(ctx, f.Client, schedule)
			ginkgo.By("Verifying first observation creates no Backup")
			gomega.Consistently(func() (int, error) {
				items, err := listE2EManagedBackups(ctx, f.Client, schedule)
				return len(items), err
			}).WithTimeout(5 * time.Second).
				WithPolling(backupSchedulePollInterval).
				Should(gomega.Equal(0))

			ginkgo.By("Creating an ambiguous manual snapshot to block overlap")
			gomega.Expect(setE2EClusterPaused(ctx, f.Client, namespace, clusterName, true)).To(gomega.Succeed())
			ginkgo.DeferCleanup(func(ctx context.Context) {
				err := setE2EClusterPaused(ctx, f.Client, namespace, clusterName, false)
				if apierrors.IsNotFound(err) {
					return
				}
				f.Must(err)
			})
			manual := brframework.GetBackup(
				namespace,
				"manual-overlap",
				clusterName,
				f.Storage.Config(namespace, "manual-overlap"),
			)
			manual.Spec.Mode = brv1alpha1.BackupModeSnapshot
			manual.Spec.CleanPolicy = brv1alpha1.CleanPolicyTypeRetain
			gomega.Expect(f.Client.Create(ctx, manual)).To(gomega.Succeed())
			ginkgo.DeferCleanup(func(ctx context.Context) {
				current := &brv1alpha1.Backup{}
				err := f.Client.Get(ctx, ctrlclient.ObjectKeyFromObject(manual), current)
				if err == nil {
					f.Must(f.Client.Delete(ctx, current))
					return
				}
				gomega.Expect(apierrors.IsNotFound(err)).To(gomega.BeTrue())
			})
			gomega.Expect(setE2EBackupCondition(
				ctx,
				f.Client,
				manual,
				string(brv1alpha1.BackupComplete),
				metav1.ConditionUnknown,
			)).To(gomega.Succeed())

			firstDue := time.Date(time.Now().UTC().Year(), time.January, 1, 0, 0, 0, 0, time.UTC)
			blockedCursor := firstDue.AddDate(-1, 0, 0)
			ginkgo.By("Moving the cursor back while the manual snapshot blocks the target")
			gomega.Expect(setE2EBackupScheduleStatus(
				ctx,
				f.Client,
				schedule,
				blockedCursor,
				true,
			)).To(gomega.Succeed())
			gomega.Consistently(func(g gomega.Gomega) {
				current := getE2EBackupSchedule(ctx, g, f.Client, schedule)
				g.Expect(current.Status.LastScheduleTime).NotTo(gomega.BeNil())
				g.Expect(current.Status.LastScheduleTime.Time.Equal(blockedCursor)).To(gomega.BeTrue())
				items, err := listE2EManagedBackups(ctx, f.Client, schedule)
				g.Expect(err).NotTo(gomega.HaveOccurred())
				g.Expect(items).To(gomega.BeEmpty())
			}).WithTimeout(5 * time.Second).
				WithPolling(backupSchedulePollInterval).
				Should(gomega.Succeed())

			ginkgo.By("Making the overlap blocker safely terminal")
			gomega.Expect(setE2EBackupCondition(
				ctx,
				f.Client,
				manual,
				string(brv1alpha1.BackupInvalid),
				metav1.ConditionTrue,
			)).To(gomega.Succeed())

			first := waitForNewE2EManagedBackup(ctx, f.Client, schedule, nil)
			assertE2EManagedBackup(schedule, &first, firstDue, "scheduled")

			ginkgo.By("Rewinding status and restarting the operator to exercise deterministic recovery")
			gomega.Expect(setE2EBackupScheduleStatus(
				ctx,
				f.Client,
				schedule,
				blockedCursor,
				true,
			)).To(gomega.Succeed())
			gomega.Expect(restartE2EOperator(ctx, f.Client)).To(gomega.Succeed())
			gomega.Eventually(func(g gomega.Gomega) {
				current := getE2EBackupSchedule(ctx, g, f.Client, schedule)
				g.Expect(current.Status.LastScheduleTime).NotTo(gomega.BeNil())
				g.Expect(current.Status.LastScheduleTime.Time.Equal(firstDue)).To(gomega.BeTrue())
				g.Expect(current.Status.LastBackup).To(gomega.Equal(first.Name))
				items, err := listE2EManagedBackups(ctx, f.Client, schedule)
				g.Expect(err).NotTo(gomega.HaveOccurred())
				g.Expect(items).To(gomega.HaveLen(1))
			}).WithTimeout(backupScheduleControlPlaneTimeout).
				WithPolling(backupSchedulePollInterval).
				Should(gomega.Succeed())
			gomega.Expect(setE2EClusterPaused(ctx, f.Client, namespace, clusterName, false)).To(gomega.Succeed())

			ginkgo.By("Waiting for the first generated Backup to complete")
			gomega.Expect(brframework.WaitForBackupComplete(
				f.Client,
				namespace,
				first.Name,
				backupCompleteTimeout,
			)).To(gomega.Succeed())

			ginkgo.By("Restoring seeded data into a distinct disposable Cluster")
			const restoreClusterName = "backup-schedule-restore"
			const restoreName = "backup-schedule-restore"
			f.Must(createTidbCluster(f, restoreClusterName, tidbVersion, false, false, true))
			f.Must(waiter.WaitForClusterReady(
				ctx,
				f.Client,
				namespace,
				restoreClusterName,
				tidbReadyTimeout,
			))
			gomega.Expect(createRestoreAndWaitForComplete(
				f,
				restoreName,
				restoreClusterName,
				first.Name,
				nil,
			)).To(gomega.Succeed())
			ginkgo.DeferCleanup(func() {
				f.Must(deleteRestore(f, restoreName))
			})
			restoreHost, restorePort, stopRestoreForward, err := k8s.ForwardOnePort(
				f.PortForwarder,
				namespace,
				getTiDBServiceResourceName(restoreClusterName),
				corev1alpha1.DefaultTiDBPortClient,
			)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer stopRestoreForward()
			restoreDSN := getDefaultDSN(
				fmt.Sprintf("%s:%d", restoreHost, restorePort),
				databaseName,
			)
			gomega.Expect(checkDataIsSame(sourceDSN, restoreDSN)).To(gomega.Succeed())

			minuteCursor := time.Now().UTC().Truncate(time.Minute).Add(-2 * time.Minute)
			ginkgo.By("Switching to a per-minute schedule for a second distinct occurrence")
			gomega.Expect(setE2EBackupScheduleStatus(ctx, f.Client, schedule, minuteCursor, false)).To(gomega.Succeed())
			gomega.Expect(updateE2EBackupSchedule(ctx, f.Client, schedule, func(current *brv1alpha1.BackupSchedule) {
				current.Spec.Schedule = "* * * * *"
			})).To(gomega.Succeed())

			second := waitForNewE2EManagedBackup(
				ctx,
				f.Client,
				schedule,
				map[string]struct{}{first.Name: {}},
			)
			secondDue, err := backupschedulectrl.ParseScheduledTime(&second)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			assertE2EManagedBackup(schedule, &second, secondDue, "scheduled")
			gomega.Expect(second.Name).NotTo(gomega.Equal(first.Name))
			gomega.Expect(second.Spec.S3.Prefix).NotTo(gomega.Equal(first.Spec.S3.Prefix))

			ginkgo.By("Returning to an annual expression before another occurrence is due")
			gomega.Expect(updateE2EBackupSchedule(ctx, f.Client, schedule, func(current *brv1alpha1.BackupSchedule) {
				current.Spec.Schedule = "0 0 1 1 *"
			})).To(gomega.Succeed())
			gomega.Expect(brframework.WaitForBackupComplete(
				f.Client,
				namespace,
				second.Name,
				backupCompleteTimeout,
			)).To(gomega.Succeed())

			ginkgo.By("Pausing across a due annual occurrence")
			gomega.Expect(updateE2EBackupSchedule(ctx, f.Client, schedule, func(current *brv1alpha1.BackupSchedule) {
				current.Spec.Pause = true
			})).To(gomega.Succeed())
			schedule = waitForE2EBackupScheduleReady(ctx, f.Client, schedule)
			pausedDue := firstDue
			gomega.Expect(setE2EBackupScheduleStatus(
				ctx,
				f.Client,
				schedule,
				pausedDue.AddDate(-1, 0, 0),
				false,
			)).To(gomega.Succeed())
			gomega.Eventually(func(g gomega.Gomega) {
				current := getE2EBackupSchedule(ctx, g, f.Client, schedule)
				g.Expect(current.Status.LastScheduleTime).NotTo(gomega.BeNil())
				g.Expect(current.Status.LastScheduleTime.Time.Equal(pausedDue)).To(gomega.BeTrue())
				items, err := listE2EManagedBackups(ctx, f.Client, current)
				g.Expect(err).NotTo(gomega.HaveOccurred())
				g.Expect(items).To(gomega.HaveLen(2))
			}).WithTimeout(backupScheduleControlPlaneTimeout).
				WithPolling(backupSchedulePollInterval).
				Should(gomega.Succeed())

			ginkgo.By("Resuming without backfilling the paused occurrence")
			gomega.Expect(updateE2EBackupSchedule(ctx, f.Client, schedule, func(current *brv1alpha1.BackupSchedule) {
				current.Spec.Pause = false
			})).To(gomega.Succeed())
			schedule = waitForE2EBackupScheduleReady(ctx, f.Client, schedule)
			gomega.Consistently(func() (int, error) {
				items, err := listE2EManagedBackups(ctx, f.Client, schedule)
				return len(items), err
			}).WithTimeout(5 * time.Second).
				WithPolling(backupSchedulePollInterval).
				Should(gomega.Equal(2))

		})
	},
)

func newE2EBackupSchedule(
	f *brframework.Framework,
	namespace string,
	name string,
	clusterName string,
) *brv1alpha1.BackupSchedule {
	template := brframework.GetBackup(
		namespace,
		"template",
		clusterName,
		f.Storage.Config(namespace, "scheduled"),
	)
	template.Spec.Mode = brv1alpha1.BackupModeSnapshot
	template.Spec.CleanPolicy = brv1alpha1.CleanPolicyTypeRetain
	template.Spec.BR = nil

	return &brv1alpha1.BackupSchedule{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
		Spec: brv1alpha1.BackupScheduleSpec{
			Cluster:        corev1alpha1.ClusterReference{Name: clusterName},
			Schedule:       "0 0 1 1 *",
			MaxBackups:     ptr.To[int32](0),
			BackupTemplate: *template.Spec.DeepCopy(),
		},
	}
}

func waitForE2EBackupScheduleReady(
	ctx context.Context,
	c client.Client,
	schedule *brv1alpha1.BackupSchedule,
) *brv1alpha1.BackupSchedule {
	var current *brv1alpha1.BackupSchedule
	gomega.Eventually(func(g gomega.Gomega) {
		current = getE2EBackupSchedule(ctx, g, c, schedule)
		g.Expect(current.Status.LastScheduleTime).NotTo(gomega.BeNil())
		assertE2ECondition(
			g,
			current,
			backupschedulectrl.ConditionSchedulingReady,
			metav1.ConditionTrue,
			backupschedulectrl.ReasonReconciled,
		)
	}).WithTimeout(backupScheduleControlPlaneTimeout).
		WithPolling(backupSchedulePollInterval).
		Should(gomega.Succeed())
	return current
}

func getE2EBackupSchedule(
	ctx context.Context,
	g gomega.Gomega,
	c client.Client,
	schedule *brv1alpha1.BackupSchedule,
) *brv1alpha1.BackupSchedule {
	current := &brv1alpha1.BackupSchedule{}
	g.Expect(c.Get(ctx, ctrlclient.ObjectKeyFromObject(schedule), current)).To(gomega.Succeed())
	return current
}

func assertE2ECondition(
	g gomega.Gomega,
	schedule *brv1alpha1.BackupSchedule,
	conditionType string,
	status metav1.ConditionStatus,
	reason string,
) {
	condition := apiMeta.FindStatusCondition(schedule.Status.Conditions, conditionType)
	g.Expect(condition).NotTo(gomega.BeNil())
	g.Expect(condition.Status).To(gomega.Equal(status))
	g.Expect(condition.Reason).To(gomega.Equal(reason))
	g.Expect(condition.ObservedGeneration).To(gomega.Equal(schedule.Generation))
}

func listE2EManagedBackups(
	ctx context.Context,
	c client.Client,
	schedule *brv1alpha1.BackupSchedule,
) ([]brv1alpha1.Backup, error) {
	items := &brv1alpha1.BackupList{}
	err := c.List(
		ctx,
		items,
		ctrlclient.InNamespace(schedule.Namespace),
		ctrlclient.MatchingLabels{
			backupschedulectrl.ScheduleUIDLabel: string(schedule.UID),
		},
	)
	return items.Items, err
}

func waitForNewE2EManagedBackup(
	ctx context.Context,
	c client.Client,
	schedule *brv1alpha1.BackupSchedule,
	ignored map[string]struct{},
) brv1alpha1.Backup {
	var found brv1alpha1.Backup
	gomega.Eventually(func(g gomega.Gomega) {
		items, err := listE2EManagedBackups(ctx, c, schedule)
		g.Expect(err).NotTo(gomega.HaveOccurred())
		candidates := make([]brv1alpha1.Backup, 0, len(items))
		for i := range items {
			if _, skip := ignored[items[i].Name]; !skip {
				candidates = append(candidates, items[i])
			}
		}
		g.Expect(candidates).To(gomega.HaveLen(1))
		found = candidates[0]
	}).WithTimeout(backupScheduleCreationTimeout).
		WithPolling(backupSchedulePollInterval).
		Should(gomega.Succeed())
	return found
}

func assertE2EManagedBackup(
	schedule *brv1alpha1.BackupSchedule,
	backup *brv1alpha1.Backup,
	scheduledTime time.Time,
	basePrefix string,
) {
	gomega.Expect(backup.Name).To(gomega.Equal(
		backupschedulectrl.BackupName(schedule.Name, schedule.UID, scheduledTime),
	))
	gomega.Expect(backup.Namespace).To(gomega.Equal(schedule.Namespace))
	gomega.Expect(backup.OwnerReferences).To(gomega.BeEmpty())
	gomega.Expect(backup.Labels).To(gomega.HaveKeyWithValue(
		backupschedulectrl.ScheduleUIDLabel,
		string(schedule.UID),
	))
	gomega.Expect(backup.Annotations).To(gomega.HaveKeyWithValue(
		backupschedulectrl.ScheduleNameAnnotation,
		schedule.Name,
	))
	gomega.Expect(backup.Annotations).To(gomega.HaveKeyWithValue(
		backupschedulectrl.ScheduledTimeAnnotation,
		backupschedulectrl.CanonicalScheduledTime(scheduledTime),
	))
	gomega.Expect(backup.Spec.Mode).To(gomega.Equal(brv1alpha1.BackupModeSnapshot))
	gomega.Expect(backup.Spec.CleanPolicy).To(gomega.Equal(brv1alpha1.CleanPolicyTypeRetain))
	gomega.Expect(backup.Spec.BR).NotTo(gomega.BeNil())
	gomega.Expect(backup.Spec.BR.Cluster).To(gomega.Equal(schedule.Spec.Cluster.Name))
	gomega.Expect(backup.Spec.BR.ClusterNamespace).To(gomega.BeEmpty())
	gomega.Expect(backup.Spec.S3).NotTo(gomega.BeNil())
	gomega.Expect(backup.Spec.S3.Prefix).To(gomega.Equal(path.Join(basePrefix, backup.Name)))
}

func setE2EBackupScheduleStatus(
	ctx context.Context,
	c client.Client,
	schedule *brv1alpha1.BackupSchedule,
	cursor time.Time,
	clearLastBackup bool,
) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current := &brv1alpha1.BackupSchedule{}
		if err := c.Get(ctx, ctrlclient.ObjectKeyFromObject(schedule), current); err != nil {
			return err
		}
		current.Status.LastScheduleTime = &metav1.Time{Time: cursor.UTC().Truncate(time.Second)}
		if clearLastBackup {
			current.Status.LastBackup = ""
			current.Status.LastBackupTime = nil
		}
		return c.Status().Update(ctx, current)
	})
}

func setE2EBackupCondition(
	ctx context.Context,
	c client.Client,
	backup *brv1alpha1.Backup,
	conditionType string,
	status metav1.ConditionStatus,
) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current := &brv1alpha1.Backup{}
		if err := c.Get(ctx, ctrlclient.ObjectKeyFromObject(backup), current); err != nil {
			return err
		}
		current.Status.Conditions = []metav1.Condition{{
			Type:               conditionType,
			Status:             status,
			Reason:             "BackupScheduleE2E",
			Message:            "controlled overlap fixture",
			ObservedGeneration: current.Generation,
			LastTransitionTime: metav1.Now(),
		}}
		return c.Status().Update(ctx, current)
	})
}

func setE2EClusterPaused(
	ctx context.Context,
	c client.Client,
	namespace string,
	name string,
	paused bool,
) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cluster := &corev1alpha1.Cluster{}
		if err := c.Get(ctx, ctrlclient.ObjectKey{Namespace: namespace, Name: name}, cluster); err != nil {
			return err
		}
		cluster.Spec.Paused = paused
		return c.Update(ctx, cluster)
	})
}

func restartE2EOperator(ctx context.Context, c client.Client) error {
	key := ctrlclient.ObjectKey{
		Namespace: backupScheduleOperatorNamespace,
		Name:      "tidb-operator",
	}
	restartToken := time.Now().UTC().Format(time.RFC3339Nano)
	var generation int64
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		deployment := &appsv1.Deployment{}
		if err := c.Get(ctx, key, deployment); err != nil {
			return err
		}
		if deployment.Spec.Template.Annotations == nil {
			deployment.Spec.Template.Annotations = make(map[string]string)
		}
		deployment.Spec.Template.Annotations["backupschedule.e2e.pingcap.com/restarted-at"] = restartToken
		if err := c.Update(ctx, deployment); err != nil {
			return err
		}
		generation = deployment.Generation
		return nil
	}); err != nil {
		return err
	}

	gomega.Eventually(func(g gomega.Gomega) {
		deployment := &appsv1.Deployment{}
		g.Expect(c.Get(ctx, key, deployment)).To(gomega.Succeed())
		replicas := int32(1)
		if deployment.Spec.Replicas != nil {
			replicas = *deployment.Spec.Replicas
		}
		g.Expect(deployment.Generation).To(gomega.BeNumerically(">=", generation))
		g.Expect(deployment.Spec.Template.Annotations["backupschedule.e2e.pingcap.com/restarted-at"]).
			To(gomega.Equal(restartToken))
		g.Expect(deployment.Status.ObservedGeneration).To(gomega.BeNumerically(">=", deployment.Generation))
		g.Expect(deployment.Status.UpdatedReplicas).To(gomega.Equal(replicas))
		g.Expect(deployment.Status.Replicas).To(gomega.Equal(replicas))
		g.Expect(deployment.Status.AvailableReplicas).To(gomega.Equal(replicas))
	}).WithTimeout(backupScheduleControlPlaneTimeout).
		WithPolling(backupSchedulePollInterval).
		Should(gomega.Succeed())
	return nil
}

func updateE2EBackupSchedule(
	ctx context.Context,
	c client.Client,
	schedule *brv1alpha1.BackupSchedule,
	mutate func(*brv1alpha1.BackupSchedule),
) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		current := &brv1alpha1.BackupSchedule{}
		if err := c.Get(ctx, ctrlclient.ObjectKeyFromObject(schedule), current); err != nil {
			return err
		}
		mutate(current)
		return c.Update(ctx, current)
	})
}
