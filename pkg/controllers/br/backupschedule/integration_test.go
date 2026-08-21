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

//go:build integration
// +build integration

package backupschedule

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/yaml"

	brv1alpha1 "github.com/pingcap/tidb-operator/api/v2/br/v1alpha1"
	corev1alpha1 "github.com/pingcap/tidb-operator/api/v2/core/v1alpha1"
)

var (
	integrationConfig *rest.Config
	integrationClient client.Client
	integrationScheme *runtime.Scheme
)

func TestMain(m *testing.M) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		fmt.Fprintf(os.Stderr, "add core API to integration scheme: %v\n", err)
		os.Exit(1)
	}
	if err := brv1alpha1.AddToScheme(scheme); err != nil {
		fmt.Fprintf(os.Stderr, "add BR API to integration scheme: %v\n", err)
		os.Exit(1)
	}

	backupCRD, err := loadIntegrationCRD("br.pingcap.com_backups.yaml")
	if err != nil {
		fmt.Fprintf(os.Stderr, "load Backup CRD: %v\n", err)
		os.Exit(1)
	}
	scheduleCRD, err := loadIntegrationCRD("br.pingcap.com_backupschedules.yaml")
	if err != nil {
		fmt.Fprintf(os.Stderr, "load BackupSchedule CRD: %v\n", err)
		os.Exit(1)
	}

	testEnvironment := &envtest.Environment{
		Scheme: scheme,
		CRDs:   []*apiextensionsv1.CustomResourceDefinition{backupCRD, scheduleCRD},
		CRDInstallOptions: envtest.CRDInstallOptions{
			CleanUpAfterUse: true,
		},
	}
	integrationConfig, err = testEnvironment.Start()
	if err != nil {
		fmt.Fprintf(os.Stderr, "start BackupSchedule integration API server: %v\n", err)
		os.Exit(1)
	}
	integrationScheme = scheme
	integrationClient, err = client.New(integrationConfig, client.Options{Scheme: integrationScheme})
	if err != nil {
		fmt.Fprintf(os.Stderr, "create BackupSchedule integration client: %v\n", err)
		_ = testEnvironment.Stop()
		os.Exit(1)
	}

	code := m.Run()
	if err := testEnvironment.Stop(); err != nil {
		fmt.Fprintf(os.Stderr, "stop BackupSchedule integration API server: %v\n", err)
		if code == 0 {
			code = 1
		}
	}
	os.Exit(code)
}

func TestIntegrationCRDMigrationAndClusterFieldSelection(t *testing.T) {
	currentScheduleCRD, err := loadIntegrationCRD("br.pingcap.com_backupschedules.yaml")
	require.NoError(t, err)
	legacyScheduleCRD := currentScheduleCRD.DeepCopy()
	makeLegacyBackupScheduleCRD(t, legacyScheduleCRD)
	backupCRD, err := loadIntegrationCRD("br.pingcap.com_backups.yaml")
	require.NoError(t, err)

	upgradeEnvironment := &envtest.Environment{
		Scheme: integrationScheme,
		CRDs:   []*apiextensionsv1.CustomResourceDefinition{backupCRD, legacyScheduleCRD},
		CRDInstallOptions: envtest.CRDInstallOptions{
			CleanUpAfterUse: true,
		},
	}
	config, err := upgradeEnvironment.Start()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, upgradeEnvironment.Stop())
	})

	apiClient, err := client.New(config, client.Options{Scheme: integrationScheme})
	require.NoError(t, err)
	namespace := createNamespaceWithClient(t, apiClient, "backupschedule-upgrade-")
	fixture := newIntegrationSchedule(namespace, "stored-before-upgrade")
	fixture.Spec.Cluster = corev1alpha1.ClusterReference{}
	fixture.Spec.BackupTemplate.BR = &brv1alpha1.BRConfig{
		Cluster:          "legacy-cluster",
		ClusterNamespace: "legacy-namespace",
	}
	require.NoError(t, apiClient.Create(t.Context(), fixture))

	stored := &brv1alpha1.BackupSchedule{}
	require.NoError(t, apiClient.Get(t.Context(), client.ObjectKeyFromObject(fixture), stored))
	originalUID := stored.UID
	assert.Empty(t, stored.Spec.Cluster.Name)
	stored.Status.LastBackup = "legacy-backup"
	require.NoError(t, apiClient.Status().Update(t.Context(), stored))

	extensionsClient, err := apiextensionsclient.NewForConfig(config)
	require.NoError(t, err)
	liveCRD, err := extensionsClient.ApiextensionsV1().CustomResourceDefinitions().Get(
		t.Context(),
		currentScheduleCRD.Name,
		metav1.GetOptions{},
	)
	require.NoError(t, err)
	liveCRD.Spec = currentScheduleCRD.Spec
	_, err = extensionsClient.ApiextensionsV1().CustomResourceDefinitions().Update(
		t.Context(),
		liveCRD,
		metav1.UpdateOptions{},
	)
	require.NoError(t, err)
	require.NoError(t, waitForEstablishedCRD(t.Context(), extensionsClient, currentScheduleCRD.Name))

	upgraded := &brv1alpha1.BackupSchedule{}
	require.NoError(t, apiClient.Get(t.Context(), client.ObjectKeyFromObject(fixture), upgraded))
	assert.Equal(t, originalUID, upgraded.UID)
	assert.Equal(t, "legacy-cluster", upgraded.Spec.BackupTemplate.BR.Cluster)
	assert.Equal(t, "legacy-namespace", upgraded.Spec.BackupTemplate.BR.ClusterNamespace)
	assert.Equal(t, "legacy-backup", upgraded.Status.LastBackup)

	upgraded.Spec.Pause = true
	err = apiClient.Update(t.Context(), upgraded)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "spec.cluster")

	require.NoError(t, apiClient.Get(t.Context(), client.ObjectKeyFromObject(fixture), upgraded))
	upgraded.Spec.Cluster = corev1alpha1.ClusterReference{Name: "legacy-cluster"}
	upgraded.Spec.BackupTemplate.BR.Cluster = "legacy-cluster"
	upgraded.Spec.BackupTemplate.BR.ClusterNamespace = ""
	upgraded.Spec.Pause = true
	require.NoError(t, apiClient.Update(t.Context(), upgraded))

	migrated := &brv1alpha1.BackupSchedule{}
	require.NoError(t, apiClient.Get(t.Context(), client.ObjectKeyFromObject(fixture), migrated))
	assert.Equal(t, originalUID, migrated.UID)
	assert.Equal(t, "legacy-cluster", migrated.Spec.Cluster.Name)
	assert.Empty(t, migrated.Spec.BackupTemplate.BR.ClusterNamespace)
	assert.Equal(t, "legacy-backup", migrated.Status.LastBackup)

	migrated.Spec.Cluster.Name = "replacement-cluster"
	migrated.Spec.BackupTemplate.BR.Cluster = "replacement-cluster"
	err = apiClient.Update(t.Context(), migrated)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "immutable")

	first := newIntegrationSchedule(namespace, "field-selection-first")
	first.Spec.Cluster.Name = "selection-one"
	require.NoError(t, apiClient.Create(t.Context(), first))
	second := newIntegrationSchedule(namespace, "field-selection-second")
	second.Spec.Cluster.Name = "selection-two"
	require.NoError(t, apiClient.Create(t.Context(), second))

	selected := &brv1alpha1.BackupScheduleList{}
	require.NoError(t, apiClient.List(
		t.Context(),
		selected,
		client.InNamespace(namespace),
		client.MatchingFields{"spec.cluster.name": "selection-two"},
	))
	require.Len(t, selected.Items, 1)
	assert.Equal(t, second.Name, selected.Items[0].Name)

	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	reconciler := NewSchedulingReconciler(apiClient, apiClient, staticClock{now: now}, NewTargetLocks())
	_, err = reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(fixture)})
	require.NoError(t, err)
	require.NoError(t, apiClient.Get(t.Context(), client.ObjectKeyFromObject(fixture), migrated))
	require.NotNil(t, migrated.Status.LastScheduleTime)
	assert.True(t, migrated.Status.LastScheduleTime.Time.Equal(now))
	backups := &brv1alpha1.BackupList{}
	require.NoError(t, apiClient.List(t.Context(), backups, client.InNamespace(namespace)))
	assert.Empty(t, backups.Items)
}

func TestIntegrationStatusSubresourceConflictAndFreshRetry(t *testing.T) {
	namespace := createIntegrationNamespace(t, "backupschedule-status-")
	schedule := createIntegrationSchedule(t, namespace, "status-race")
	key := client.ObjectKeyFromObject(schedule)

	arrived := make(chan struct{}, 2)
	release := make(chan struct{})
	reader := &statusBarrierReader{
		Reader:  integrationClient,
		arrived: arrived,
		release: release,
	}
	updater := &StatusUpdater{Reader: reader, Writer: integrationClient.Status()}
	now := metav1.NewTime(time.Date(2026, 8, 20, 13, 0, 0, 0, time.UTC))

	mutations := map[string]StatusMutation{
		"scheduling": func(latest *brv1alpha1.BackupSchedule) (bool, error) {
			return ApplySchedulingStatus(latest, &SchedulingStatus{
				LastScheduleTime: &now,
				LastBackup:       "status-race-backup",
				LastBackupTime:   &now,
				Condition: metav1.Condition{
					Status:             metav1.ConditionTrue,
					Reason:             ReasonReconciled,
					Message:            "scheduling status won or retried",
					LastTransitionTime: now,
				},
			}), nil
		},
		"external": func(latest *brv1alpha1.BackupSchedule) (bool, error) {
			latest.Status.LastCompact = "external-status"
			apiMeta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
				Type:               "ExternalReady",
				Status:             metav1.ConditionTrue,
				Reason:             "Updated",
				Message:            "external status won or retried",
				ObservedGeneration: latest.Generation,
				LastTransitionTime: now,
			})
			return true, nil
		},
	}
	type updateResult struct {
		owner string
		err   error
	}
	results := make(chan updateResult, len(mutations))
	for owner, mutation := range mutations {
		go func() {
			results <- updateResult{
				owner: owner,
				err: updater.Update(
					t.Context(),
					key,
					schedule.UID,
					schedule.Generation,
					mutation,
				),
			}
		}()
	}

	for range mutations {
		select {
		case <-arrived:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for both status readers")
		}
	}
	close(release)

	var conflictOwner string
	for range mutations {
		select {
		case result := <-results:
			if result.err == nil {
				continue
			}
			require.True(t, apierrors.IsConflict(result.err), "unexpected %s status error: %v", result.owner, result.err)
			require.Empty(t, conflictOwner, "both status writes conflicted")
			conflictOwner = result.owner
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for status patch results")
		}
	}
	require.NotEmpty(t, conflictOwner, "both optimistic status writes unexpectedly succeeded")

	freshUpdater := NewStatusUpdater(integrationClient, integrationClient)
	require.NoError(t, freshUpdater.Update(
		t.Context(),
		key,
		schedule.UID,
		schedule.Generation,
		mutations[conflictOwner],
	))

	verified := &brv1alpha1.BackupSchedule{}
	require.NoError(t, integrationClient.Get(t.Context(), key, verified))
	assert.Equal(t, "status-race-backup", verified.Status.LastBackup)
	require.NotNil(t, verified.Status.LastScheduleTime)
	assert.Equal(t, "external-status", verified.Status.LastCompact)
	require.NotNil(t, apiMeta.FindStatusCondition(verified.Status.Conditions, ConditionSchedulingReady))
	require.NotNil(t, apiMeta.FindStatusCondition(verified.Status.Conditions, "ExternalReady"))
}

func TestIntegrationManagerCacheAndSchedulingBackupWatch(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	manager, err := ctrl.NewManager(integrationConfig, ctrl.Options{
		Scheme:                 integrationScheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		Cache: cache.Options{
			DefaultLabelSelector: labels.SelectorFromSet(labels.Set{
				"integration-test.pingcap.com/cached": "true",
			}),
			ByObject: map[client.Object]cache.ByObject{
				&brv1alpha1.Backup{}: {
					Label: labels.Everything(),
				},
				&brv1alpha1.BackupSchedule{}: {
					Label: labels.Everything(),
				},
			},
		},
	})
	require.NoError(t, err)
	require.NoError(t, Setup(ctx, manager, manager.GetClient()))

	managerResult := make(chan error, 1)
	go func() {
		managerResult <- manager.Start(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-managerResult:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Error("timed out stopping BackupSchedule integration manager")
		}
	})
	require.True(t, manager.GetCache().WaitForCacheSync(ctx))

	namespace := createIntegrationNamespace(t, "backupschedule-manager-")
	schedule := createIntegrationSchedule(t, namespace, "unlabeled-schedule")
	require.Empty(t, schedule.Labels)
	key := client.ObjectKeyFromObject(schedule)
	require.Eventually(t, func() bool {
		observed := &brv1alpha1.BackupSchedule{}
		if err := integrationClient.Get(t.Context(), key, observed); err != nil {
			return false
		}
		return observed.Status.LastScheduleTime != nil &&
			apiMeta.FindStatusCondition(observed.Status.Conditions, ConditionSchedulingReady) != nil
	}, 15*time.Second, 100*time.Millisecond)

	require.Greater(t, controllerReconcileCount(t, SchedulingControllerName), float64(0))
	baseline := waitForControllerQuiet(t, SchedulingControllerName)

	probe := &brv1alpha1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: "scheduling-watch-probe", Namespace: namespace},
		Spec:       integrationBackupSpec(),
	}
	require.NoError(t, integrationClient.Create(t.Context(), probe))
	waitForControllerIncrement(t, SchedulingControllerName, baseline, "Backup create")
	baseline = waitForControllerQuiet(t, SchedulingControllerName)

	probe = &brv1alpha1.Backup{}
	require.NoError(t, integrationClient.Get(
		t.Context(),
		client.ObjectKey{Namespace: namespace, Name: "scheduling-watch-probe"},
		probe,
	))
	probe.Annotations = map[string]string{"integration-test.pingcap.com/event": "update"}
	require.NoError(t, integrationClient.Update(t.Context(), probe))
	waitForControllerIncrement(t, SchedulingControllerName, baseline, "Backup update")
	baseline = waitForControllerQuiet(t, SchedulingControllerName)

	require.NoError(t, integrationClient.Delete(t.Context(), probe))
	waitForControllerIncrement(t, SchedulingControllerName, baseline, "Backup delete")
}

func loadIntegrationCRD(name string) (*apiextensionsv1.CustomResourceDefinition, error) {
	_, sourceFile, _, ok := goruntime.Caller(0)
	if !ok {
		return nil, fmt.Errorf("resolve integration test source path")
	}
	repositoryRoot := filepath.Clean(filepath.Join(filepath.Dir(sourceFile), "..", "..", "..", ".."))
	data, err := os.ReadFile(filepath.Join(repositoryRoot, "manifests", "crd", name))
	if err != nil {
		return nil, err
	}
	crd := &apiextensionsv1.CustomResourceDefinition{}
	if err := yaml.Unmarshal(data, crd); err != nil {
		return nil, err
	}
	return crd, nil
}

func makeLegacyBackupScheduleCRD(t *testing.T, crd *apiextensionsv1.CustomResourceDefinition) {
	t.Helper()
	require.NotEmpty(t, crd.Spec.Versions)
	for i := range crd.Spec.Versions {
		version := &crd.Spec.Versions[i]
		require.NotNil(t, version.Schema)
		require.NotNil(t, version.Schema.OpenAPIV3Schema)
		root := version.Schema.OpenAPIV3Schema
		spec, ok := root.Properties["spec"]
		require.True(t, ok)
		delete(spec.Properties, "cluster")
		spec.Required = removeString(spec.Required, "cluster")
		validations := spec.XValidations[:0]
		for _, validation := range spec.XValidations {
			if strings.Contains(validation.Rule, "self.cluster.name") ||
				strings.Contains(validation.Rule, "clusterNamespace") {
				continue
			}
			validations = append(validations, validation)
		}
		spec.XValidations = validations
		root.Properties["spec"] = spec

		status, ok := root.Properties["status"]
		require.True(t, ok)
		delete(status.Properties, "lastScheduleTime")
		delete(status.Properties, "conditions")
		root.Properties["status"] = status

		selectable := version.SelectableFields[:0]
		for _, field := range version.SelectableFields {
			if field.JSONPath != ".spec.cluster.name" {
				selectable = append(selectable, field)
			}
		}
		version.SelectableFields = selectable

		columns := version.AdditionalPrinterColumns[:0]
		for _, column := range version.AdditionalPrinterColumns {
			if column.JSONPath != ".spec.cluster.name" {
				columns = append(columns, column)
			}
		}
		version.AdditionalPrinterColumns = columns
	}
}

func removeString(values []string, target string) []string {
	result := values[:0]
	for _, value := range values {
		if value != target {
			result = append(result, value)
		}
	}
	return result
}

func waitForEstablishedCRD(
	ctx context.Context,
	clientset *apiextensionsclient.Clientset,
	name string,
) error {
	return wait.PollUntilContextTimeout(ctx, 50*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
		crd, err := clientset.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		for _, condition := range crd.Status.Conditions {
			if condition.Type == apiextensionsv1.Established {
				return condition.Status == apiextensionsv1.ConditionTrue, nil
			}
		}
		return false, nil
	})
}

func createIntegrationNamespace(t *testing.T, prefix string) string {
	t.Helper()
	return createNamespaceWithClient(t, integrationClient, prefix)
}

func createNamespaceWithClient(t *testing.T, apiClient client.Client, prefix string) string {
	t.Helper()
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: prefix}}
	require.NoError(t, apiClient.Create(t.Context(), namespace))
	return namespace.Name
}

func newIntegrationSchedule(namespace, name string) *brv1alpha1.BackupSchedule {
	return &brv1alpha1.BackupSchedule{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: brv1alpha1.BackupScheduleSpec{
			Cluster:        corev1alpha1.ClusterReference{Name: "integration-cluster"},
			Schedule:       "0 0 1 1 *",
			BackupTemplate: integrationBackupTemplate(),
		},
	}
}

func createIntegrationSchedule(t *testing.T, namespace, name string) *brv1alpha1.BackupSchedule {
	t.Helper()
	schedule := newIntegrationSchedule(namespace, name)
	require.NoError(t, integrationClient.Create(t.Context(), schedule))
	require.NoError(t, integrationClient.Get(t.Context(), client.ObjectKeyFromObject(schedule), schedule))
	return schedule
}

func integrationBackupTemplate() brv1alpha1.BackupSpec {
	return brv1alpha1.BackupSpec{
		StorageProvider: brv1alpha1.StorageProvider{
			S3: &brv1alpha1.S3StorageProvider{
				Provider: brv1alpha1.S3StorageProviderTypeAWS,
				Bucket:   "integration-bucket",
				Prefix:   "snapshots",
			},
		},
	}
}

func integrationBackupSpec() brv1alpha1.BackupSpec {
	result := integrationBackupTemplate()
	result.BR = &brv1alpha1.BRConfig{Cluster: "integration-cluster"}
	return result
}

type statusBarrierReader struct {
	client.Reader
	arrived chan<- struct{}
	release <-chan struct{}
}

func (reader *statusBarrierReader) Get(
	ctx context.Context,
	key client.ObjectKey,
	obj client.Object,
	opts ...client.GetOption,
) error {
	if err := reader.Reader.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	reader.arrived <- struct{}{}
	select {
	case <-reader.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func controllerReconcileCount(t *testing.T, controllerName string) float64 {
	t.Helper()
	families, err := metrics.Registry.Gather()
	require.NoError(t, err)
	var total float64
	for _, family := range families {
		if family.GetName() != "controller_runtime_reconcile_total" {
			continue
		}
		for _, metric := range family.Metric {
			if metricLabel(metric, "controller") == controllerName {
				total += metric.GetCounter().GetValue()
			}
		}
	}
	return total
}

func metricLabel(metric *dto.Metric, name string) string {
	for _, pair := range metric.Label {
		if pair.GetName() == name {
			return pair.GetValue()
		}
	}
	return ""
}

func waitForControllerIncrement(t *testing.T, controllerName string, baseline float64, event string) {
	t.Helper()
	require.Eventually(t, func() bool {
		return controllerReconcileCount(t, controllerName) > baseline
	}, 10*time.Second, 50*time.Millisecond, "%s did not wake %s", event, controllerName)
}

func waitForControllerQuiet(t *testing.T, controllerName string) float64 {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	last := controllerReconcileCount(t, controllerName)
	stableSince := time.Now()
	for time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
		current := controllerReconcileCount(t, controllerName)
		if current != last {
			last = current
			stableSince = time.Now()
			continue
		}
		if time.Since(stableSince) >= 300*time.Millisecond {
			return current
		}
	}
	t.Fatalf("controller %s did not become quiet", controllerName)
	return 0
}
