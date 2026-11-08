# Roots

A **root** is a directory on a storage device that Canon tracks. Each root is identified by its absolute path and assigned a role.

## Roles

**Source roots**: assets you want to explore, reconcile, or archive, possibly unstructured, incomplete, or holding duplicates. Examples: old backup drives, phone exports, download directories.

**Archive roots**: an intentional structure that you maintain, where archived files go. Examples: your organized photo library, music collection, document archive.

## Rules

- Roots may not overlap (one root cannot be inside another), suspended roots included
- You can have multiple roots of each type

## Typical Setup

```
Source roots:
  /Volumes/OldBackup       (unorganized photos from 2015)
  /Volumes/PhoneExport     (recent phone backup)
  ~/Downloads/Photos       (miscellaneous downloads)

Archive roots:
  /Volumes/Archive/Photos  (canonical photo library)
  /Volumes/Archive/Music   (canonical music library)
```

## Offline Access

Commands that read the index work with the storage detached. `scan` and `apply` need it online.

## Suspended roots

Suspending a root takes it out of view, and Canon does nothing on it until you unsuspend it. It says nothing about whether the storage is attached.

- **Standing**: unchanged. Its sources stay indexed.
- **Reports**: a report on a suspended place lists nothing and says it is suspended. A report across all roots leaves the root out.
- **Actions**: an action that names a suspended place is rejected by name and writes nothing.
- **Records**: the trail, the story, notes and books can still be read.
- **Evidence**: its copies still count. Content that also sits in a suspended archive root counts as archived.
- **Content**: facts and exclusions belong to the content, so acting on content from a live root reaches its copies on suspended roots.

[`canon roots`](../commands/roots/roots.md#suspending-roots) shows what each command does.
