# BackupSchedule scheduling guide

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
- [Status and fail-closed behavior](#status-and-fail-closed-behavior)
- [Rollout order](#rollout-order)
- [Staging validation](#staging-validation)
- [Rollback](#rollback)
<!-- /toc -->

`BackupSchedule` creates deterministic snapshot `Backup` objects from a UTC
schedule. This first TiDB Operator v2 stage implements scheduling only. It does
not delete Backup objects or remote backup data.

## Supported scope

This stage supports:

- Snapshot Backups only. `backupTemplate.backupMode` may be omitted or set to
  `snapshot`.
- A five-field UTC cron expression, a supported descriptor, or `@every` with a
  duration of at least one minute.
- Exactly one storage provider in `backupTemplate`: S3, GCS, Azure Blob, or
  Local.
- `maxBackups` omitted or set to `0`. Any positive value fails closed until the
  separate retention stage is installed.
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

Keep a new schedule paused until `SchedulingReady` has observed the current
`metadata.generation` and `status.lastScheduleTime` is present. First
reconciliation initializes the scheduling cursor and creates no Backup.

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
`status.lastScheduleTime` without creating Backups. Running Backup work is not
cancelled.

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

## Status and fail-closed behavior

The scheduling controller owns only:

- `status.lastScheduleTime`
- `status.lastBackup`
- `status.lastBackupTime`
- The `SchedulingReady` condition

`SchedulingReady=True` with reason `Reconciled` means the controller reached a
safe result. A pause, overlap wait, or no-op can be a safe result.
`SchedulingReady=False` with reason `InvalidSpec` means validation failed before
side effects. Reason `ReconcilerError` means scheduling could not complete
safely. Check `observedGeneration` before treating the condition as current.

An invalid schedule creates nothing and does not advance its cursor. A
conflicting deterministic name is not adopted or overwritten. Ambiguous Backup
status blocks scheduling. Identity metadata is a controller convention, not an
unforgeable security boundary, so grant Backup write access only to trusted
principals.

## Rollout order

Activation makes every visible BackupSchedule eligible for reconciliation:

1. Inventory live BackupSchedule objects in every target cluster.
2. Review or pause every existing object before activation.
3. Deploy the updated `br.pingcap.com_backupschedules.yaml` CRD.
4. Migrate stored objects to the required immutable `spec.cluster` contract.
5. Deploy the updated TiDB Operator controller binary.
6. Create the canary schedule paused with an isolated prefix and
   `cleanPolicy: Retain`.

The CRD must be deployed before the binary because the controller writes the
new cursor and condition fields. The source RBAC marker grants Backup `create`
but not `delete` for this stage. The current Helm chart groups BR resources
under broader wildcard permissions, so inspect rendered release RBAC as part of
deployment review.

## Staging validation

Use a disposable namespace and isolated storage prefix:

1. Apply the schedule paused with `maxBackups` omitted or `0`.
2. Verify current `SchedulingReady` and `status.lastScheduleTime`.
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
10. Exercise pause and resume and confirm paused occurrences are not backfilled.
11. Restore one canary snapshot successfully.

Keep any external or legacy scheduler and native creation disabled from sharing
a target throughout the test and cutover.

## Rollback

If activation must be rolled back:

1. Set `spec.pause: true` on every native schedule.
2. Verify `SchedulingReady` has observed the new generation.
3. Roll back the controller binary.
4. Leave the updated CRD installed until stored-object compatibility is
   reviewed.
5. Preserve generated Backups and remote snapshots for audit and recovery.

Pausing does not cancel a running Backup. Rolling back the scheduling controller
does not delete generated Backups or remote data.
