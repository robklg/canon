# Roots

A **root** is a directory on a storage device that Canon tracks. Each root is identified by its absolute path and assigned a role.

## Roles

Canon distinguishes two root roles:

**Source roots** contain assets you want to explore, reconcile, or archive. They may be unstructured, incomplete, or contain duplicates. Examples: old backup drives, phone exports, download folders.

**Archive roots** hold an intentional structure that you maintain. Files archived by Canon are placed here. Examples: your organized photo library, music collection, document archive.

## Rules

- Roots may not overlap (one root cannot be inside another)
- A root can be any directory, not just a drive or mount point
- You can have multiple roots of each type
- Roots can be [suspended](../commands/roots/roots.md): closing the door on a root, keeping everything in it safe and set aside until you open it again

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

Query commands (`ls`, `facts`, `coverage`, `worklist`, `compare`, `cluster generate`, `exclude`, `roots`) work even when the underlying storage is detached. Canon resolves path arguments against known roots in the database, so you can explore sources, check coverage, and generate manifests without the storage being physically attached.

Commands that access file contents (`scan`, `apply`) still require the storage to be online.
