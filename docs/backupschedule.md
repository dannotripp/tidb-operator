# BackupSchedule operator guide

<!-- toc -->
- [Supported scope](#supported-scope)
- [Target contract](#target-contract)
- [Minimal paused schedule](#minimal-paused-schedule)
- [Schedule grammar](#schedule-grammar)
- [Scheduling behavior](#scheduling-behavior)
  - [First observation and recovery](#first-observation-and-recovery)
  - [Pause, resume, and catch-up](#pause-resume-and-catch-up)
  - [Overlap behavior](#overlap-behavior)
- [Generated Backup identity](#generated-backup-identity)
- [Retention and remote cleanup](#retention-and-remote-cleanup)
- [Status and fail-closed behavior](#status-and-fail-closed-behavior)
- [Rollout order](#rollout-order)
- [Staging validation](#staging-validation)
- [Rollback](#rollback)
<!-- /toc -->

`BackupSchedule` creates deterministic snapshot `Backup` objects from a UTC
schedule and prunes old Backup objects according to `maxBackups`. This second
TiDB Operator v2 stage layers retention onto the scheduling implementation. It
is intentionally narrower than the full API surface exposed by the CRD.

## Supported scope

This implementation supports:

- Snapshot Backups only. `backupTemplate.backupMode` may be omitted or set to
  `snapshot`.
- A five-field UTC cron expression, a supported descriptor, or `@every` with a
  duration of at least one minute.
- Exactly one storage provider in `backupTemplate`: S3, GCS, Azure Blob, or
  Local.
- An optional nonnegative `maxBackups`. A missing value or `0` disables all
  retention pruning.
- Pause and resume without backfilling occurrences consumed while paused.
- Best-effort overlap avoidance for all snapshot Backups targeting the same
  TiDB cluster.

The following fields are not supported in this stage. Setting any of them makes
the schedule invalid:

- `maxReservedTime`
- `logBackupTemplate`
- `compactSpan` or `compactBackupTemplate`
- Schedule-level BR or storage-provider inheritance
- Schedule-level `storageClassName`, `storageSize`, or `imagePullSecrets`
- Template log subcommands, log truncation, or `logStop: true`

Use a complete template storage configuration. The scheduling controller
validates static target, storage, and retry fields before creating anything.
The existing Backup controller remains responsible for live Cluster, TiKV,
Secret, credential, Job, and execution checks.

For Azure Blob, configure a container plus either `secretName` or both
`storageAccount` and `sasToken`. An incomplete inline credential pair is
rejected before a Backup is created.

The controller normalizes a relative storage prefix. Leading, trailing, and
repeated slashes and `.` path components are removed. Any `..` component makes
the schedule invalid. The generated Backup name is appended to the prefix so
each occurrence has a deterministic destination.

## Target contract

`spec.cluster.name` is the authoritative, immutable TiDB cluster target. The
BackupSchedule, generated Backups, target Cluster, and referenced namespaced
resources must be in the BackupSchedule namespace.

`backupTemplate.br` may be omitted. When the controller renders a Backup, it
deep-copies the template, allocates `spec.br` when needed, sets
`spec.br.cluster` from `spec.cluster.name`, and leaves
`spec.br.clusterNamespace` empty. Rendering never mutates the BackupSchedule
template.

For compatibility, a user may include `backupTemplate.br`, but its `cluster`
must exactly match `spec.cluster.name` and its `clusterNamespace` must be empty.
A namespace explicitly equal to the BackupSchedule namespace is still rejected.

## Minimal paused schedule

Start with the [paused S3 example](../examples/backup-schedule/paused-s3.yaml).
Replace every placeholder, create the namespace and credentials, and confirm
that the Cluster exists in the same namespace before applying it.

```sh
kubectl apply -f examples/backup-schedule/paused-s3.yaml
kubectl get backupschedule nightly-snapshot -n tidb-canary -o yaml
```

Keep a new schedule paused until both `SchedulingReady` and `RetentionReady`
have observed the current `metadata.generation`, and
`status.lastScheduleTime` is present. First reconciliation initializes the
scheduling cursor and creates no Backup. Retention also sends no deletion
requests while paused.

When the canary is ready, unpause it:

```sh
kubectl patch backupschedule nightly-snapshot \
  -n tidb-canary \
  --type merge \
  -p '{"spec":{"pause":false}}'
```

Creation occurs at the next due UTC occurrence. For a short staging test, use a
supported `@every` interval of at least one minute, then restore the production
expression before rollout.

## Schedule grammar

All expressions run in UTC, regardless of the operator process time zone.

| Form | Supported values |
| --- | --- |
| Standard cron | Exactly five fields: minute, hour, day of month, month, day of week |
| Named descriptor | `@yearly`, `@annually`, `@monthly`, `@weekly`, `@daily`, `@midnight`, or `@hourly` |
| Fixed interval | `@every <duration>`, where the Go duration is at least `1m`, such as `@every 15m` or `@every 1h30m` |

Six-field cron expressions, impossible calendar dates, `TZ=`, and `CRON_TZ=`
are rejected. Convert a desired wall-clock schedule to UTC before configuring
it. Daylight-saving changes are not applied automatically.

`@every` is anchored to the persisted scheduling cursor. Restarting the
operator or reconciling early does not shift the interval.

## Scheduling behavior

### First observation and recovery

On first observation of a new schedule, the controller initializes
`status.lastScheduleTime` to the current UTC time and creates no Backup. This
prevents preexisting objects from producing unexpected historical catch-up when
the controller is activated.

If valid Backups for the current schedule UID already exist, the controller
recovers the newest one instead. A recovery candidate must have the expected UID
label, valid UTC scheduled-time annotation, deterministic name, exact immutable
target, empty raw `clusterNamespace`, snapshot mode, and a valid canonical
destination ending in its own name.

Recovery validates each existing Backup against its own destination. A later
template bucket or base-prefix edit does not erase durable evidence of a Backup
that was already created. Later occurrences use the updated template.

For subsequent reconciliations, the controller considers occurrences strictly
after `status.lastScheduleTime` and no later than the current time. If several
are due, it creates only the newest one. Scanning is limited to 1,000 entries per
reconciliation and continues in bounded batches when a cursor is far behind.

Before creating an occurrence, the controller reads the deterministic Backup
name directly from the API server. A matching object is recovered. An object
with the same name but conflicting identity is left untouched and reported as
an error.

### Pause, resume, and catch-up

While `spec.pause` is `true`, due occurrences advance
`status.lastScheduleTime` without creating Backups, and retention sends no
deletion requests. Running Backup work is not cancelled.

Resuming never backfills occurrences consumed while paused. Editing the cron
expression retains the existing cursor and applies the new expression to future
calculations. If the controller was unavailable while unpaused, it creates at
most the newest due occurrence after bounded cursor processing.

### Overlap behavior

A nonterminal snapshot Backup for the same effective target blocks scheduled
creation. This includes manual Backups and Backups created by another schedule.
A log Backup does not block. A snapshot Backup is safely terminal only when
exactly one of `Complete`, `Failed`, or `Invalid` is `True`. Unknown or
contradictory terminal conditions fail closed and block creation.

The controller combines a process-local per-target lock with a final uncached
API-server list before create. This protects against same-process races and most
stale-cache cases, but it is not distributed fencing. Independently active
controller processes can still pass their final checks concurrently and create
different Backups for one target.

Do not run an external or legacy scheduler and native BackupSchedule creation
for the same target during migration. When a blocker exists, the controller
leaves the cursor unchanged so the missed occurrence remains eligible after the
blocker becomes terminal.

## Generated Backup identity

Every generated Backup receives these keys:

| Kind | Key | Meaning |
| --- | --- | --- |
| Label | `tidb.pingcap.com/backup-schedule-uid` | Full UID of the immutable BackupSchedule incarnation |
| Annotation | `tidb.pingcap.com/backup-schedule-name` | Human-readable schedule name |
| Annotation | `tidb.pingcap.com/scheduled-at` | Logical occurrence in zero-offset RFC3339 UTC |

The UID label and scheduled time are authoritative. The schedule-name
annotation is informational. Required metadata overrides conflicting template
values. Arbitrary BackupSchedule labels and annotations are not copied.

Generated names contain a bounded schedule-name prefix, a hash of the full
schedule UID, and the occurrence in UTC seconds. A recreated schedule receives
a new UID and therefore different Backup names. Generated Backups have no owner
reference, so deleting a BackupSchedule does not delete its Backups.

Do not manually change the identity metadata, target, mode, or storage
destination of a managed Backup. Such changes cause retention to fail closed.

## Retention and remote cleanup

`maxBackups` is a quota for terminal, nonterminating successful Backup objects.
It is not a bound on remote snapshots, bytes, or storage cost.

When `maxBackups` is greater than zero, retention keeps:

- The newest `maxBackups` Backups with `Complete=True`.
- The newest five Backups with `Failed=True` or `Invalid=True`, as a separate
  combined diagnostic history.
- Every active or terminating Backup.

Retention selects the oldest excess object and sends at most one delete request
per reconciliation. A missing or zero `maxBackups` disables all pruning,
including the failed and invalid history limit. Pausing the schedule freezes
retention without cancelling active Backup work.

The generated Backup preserves `backupTemplate.cleanPolicy`. Retention never
rewrites that policy and never removes cleanup finalizers. Remote cleanup
therefore follows the existing Backup behavior:

| `cleanPolicy` | Result after retention requests deletion of the Backup CR |
| --- | --- |
| Omitted or `Retain` | Remote snapshot data is retained. |
| `OnFailure` | Remote data is cleaned only when the Backup has `Failed=True`; otherwise it is retained. |
| `Delete` | The Backup cleanup flow attempts to delete remote data before object removal completes. |

For example, `maxBackups: 3` with `cleanPolicy: Retain` can leave more than
three remote snapshots even after only three completed, nonterminating Backup
CRs remain. A cleanup failure or finalizer stall can also leave an excess
Backup in `Terminating`. The controller never selects that object again, but it
does not count terminating objects toward the quota, so later reconciliations
may request deletion of other excess Backups while the first remains stalled.
Monitor terminating objects as well as provider storage.

Before every delete, retention directly rereads the candidate Backup and its
BackupSchedule from the API server. It revalidates the schedule UID and
generation, pause and deletion state, immutable target, Backup UID and resource
version, specification, identity metadata, destination, and terminal state.
The delete request carries both UID and resource-version preconditions. Any
change makes the plan stale and triggers a fresh calculation.

Historical Backups for the current immutable schedule UID remain eligible
after supported template storage edits. Each Backup is validated against its
own durable identity and canonical destination, not the current template. A
malformed current-UID Backup blocks all retention deletion for that schedule
until the object is repaired or removed. Backups from an older schedule UID and
unrelated Backups are never adopted into the retention quota.

## Status and fail-closed behavior

The scheduling controller owns only:

- `status.lastScheduleTime`
- `status.lastBackup`
- `status.lastBackupTime`
- The `SchedulingReady` condition

The independent retention controller owns only the `RetentionReady` condition.
Both loops use optimistic status updates so a conflict forces a fresh read and
reconciliation instead of overwriting fields owned by the other loop.

`SchedulingReady=True` with reason `Reconciled` means the controller reached a
safe result. A pause, overlap wait, or no-op can be a safe result.
`SchedulingReady=False` with reason `InvalidSpec` means validation failed before
side effects. Reason `ReconcilerError` means scheduling could not complete
safely. Check `observedGeneration` before treating the condition as current.

`RetentionReady=True` with reason `Reconciled` means retention safely paused,
was disabled, found no excess Backup, or accepted one deletion request.
`RetentionReady=False` uses the same `InvalidSpec` and `ReconcilerError`
reasons. Check its `observedGeneration` independently.

An invalid schedule creates nothing, deletes nothing, and does not advance its
cursor. A conflicting deterministic name is not adopted or overwritten.
Ambiguous Backup status blocks scheduling and retention. Identity metadata is a
controller convention, not an unforgeable security boundary, so grant Backup
write access only to trusted principals.

## Rollout order

Activation makes every visible BackupSchedule eligible for reconciliation:

1. Inventory live BackupSchedule objects in every target cluster.
2. Review or pause every existing object before activation.
3. Deploy the updated `br.pingcap.com_backupschedules.yaml` CRD.
4. Migrate stored objects to the required immutable `spec.cluster` contract.
5. Deploy and validate the scheduling-only Stage 1 binary with `maxBackups`
   omitted or `0`.
6. Add Backup `delete` permission and deploy the Stage 2 binary containing the
   independent retention controller.
7. Keep the canary paused with an isolated prefix and `cleanPolicy: Retain`
   until both readiness conditions observe the current generation.
8. Enable a positive `maxBackups` only after the scheduling baseline and
   retention inventory have been reviewed.

The CRD must be deployed before the binary because the controller writes the
new cursor and condition fields. The Stage 2 source RBAC marker grants Backup
`delete` in addition to the Stage 1 `create` permission. The current Helm chart
groups BR resources under broader wildcard permissions, so inspect rendered
release RBAC as part of deployment review.

## Staging validation

Use a disposable namespace and isolated storage prefix:

1. Establish the Stage 1 baseline with the schedule paused, `maxBackups: 0`,
   `cleanPolicy: Retain`, and no other scheduler targeting the same Cluster.
2. Deploy Stage 2 and verify current `SchedulingReady`, `RetentionReady`, and
   `status.lastScheduleTime`.
3. Confirm first observation creates no Backup.
4. Unpause and observe exactly one Backup at the next due UTC occurrence.
5. Verify its deterministic name, projected same-namespace target, metadata,
   `cleanPolicy`, and destination suffix.
6. Wait for completion and confirm the expected remote snapshot exists.
7. Restart the operator after create and verify recovery without duplication.
8. Edit the template bucket or base prefix after a completed occurrence. Verify
   the old object remains recoverable and the next occurrence uses the new
   destination.
9. Start a manual snapshot Backup for the same target. Verify the schedule waits
   without advancing its cursor until the blocker becomes terminal.
10. Exercise pause and resume. Confirm paused occurrences are not backfilled
    and no retention delete request is sent while paused.
11. With disposable Backup history, set a small positive `maxBackups` while
    keeping `cleanPolicy: Retain`. Verify oldest-first, one-at-a-time Backup CR
    deletion, separate failed and invalid history, and preservation of remote
    snapshots.
12. Verify active, terminating, malformed current-UID, older-UID, and unrelated
    Backups are never selected. Repair the malformed fixture before continuing.
13. If remote deletion is required, test `cleanPolicy: Delete` separately with
    an explicitly approved disposable bucket or prefix. Confirm cleanup and
    finalizer behavior before using that policy outside staging.
14. Restore one retained canary snapshot successfully.

Keep any external or legacy scheduler and native creation disabled from sharing
a target throughout the test and cutover.

## Rollback

To roll Stage 2 back safely to the scheduling-only Stage 1 binary:

1. Atomically set `spec.pause: true` and `spec.maxBackups: 0` on every native
   schedule.
2. Verify both readiness conditions have observed the new generation and that
   no Backup is still entering deletion.
3. Roll back the controller binary to Stage 1. Stage 1 intentionally treats a
   positive `maxBackups` as `InvalidSpec`, which is why the quota must be
   disabled before rollback.
4. Remove the added Backup `delete` permission when the release mechanism
   permits it.
5. Leave the updated CRD installed until stored-object compatibility is
   reviewed.
6. Preserve generated Backups and remote snapshots for audit and recovery.

Pausing does not cancel a running Backup. Stage 1 no longer updates
`RetentionReady`; treat the old condition as stale after rollback. Rolling back
the retention controller does not delete generated Backups or remote data.
