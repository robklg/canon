# canon facts delete

Delete facts by key. Useful for removing incorrect or unwanted metadata.

```bash
# Preview deletion (dry-run by default)
canon facts delete content.mime --on object
canon facts delete content.Make --on source /path/to/photos --where 'source.ext=jpg'

# Execute deletion
canon facts delete content.mime --on object --yes
```

- `--on source` or `--on object` is required to specify entity type
- Protected namespaces (`source.*`) cannot be deleted
- Dry-run by default; use `--yes` to execute
