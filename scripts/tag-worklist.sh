#!/usr/bin/env bash
#
# Import macOS Finder tags as canon facts
#
# Usage:
#   canon worklist <scope> | ./scripts/tag-worklist.sh | canon import-facts
#
# Reads JSONL from stdin, reads macOS Finder tags for each file, outputs
# one fact per tag as "tag.<name>" with the tag name as the value.
#
# Example: a file tagged "vacation" and "kids" in Finder produces:
#   {"source_id": 42, "basis_rev": 0, "facts": {"tag.vacation": "vacation", "tag.kids": "kids"}}
#
# Querying:
#   canon ls --where 'tag.vacation?'                        # files tagged "vacation"
#   canon ls --where 'tag.vacation? AND tag.kids?'          # both tags
#   canon ls --where 'tag.vacation? AND NOT tag.kids?'      # vacation without kids
#   canon facts                                             # see all tag.* keys with counts
#
# Requires: jq, python3 (macOS built-in)

set -euo pipefail

if ! command -v jq &> /dev/null; then
  echo "Error: jq is required but not installed" >&2
  exit 1
fi

if ! command -v python3 &> /dev/null; then
  echo "Error: python3 is required but not installed" >&2
  exit 1
fi

# Python helper that reads Finder tags from xattr (works on network volumes too)
read_tags() {
  python3 -c "
import sys, subprocess, plistlib, json

path = sys.argv[1]
try:
    result = subprocess.run(
        ['xattr', '-px', 'com.apple.metadata:_kMDItemUserTags', path],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        sys.exit(0)  # No tags
    hex_str = result.stdout.replace(' ', '').replace('\n', '')
    tags = plistlib.loads(bytes.fromhex(hex_str))
    # Each tag is 'name' or 'name\ncolor_index' — extract just the name
    names = []
    for tag in tags:
        name = tag.split('\n')[0] if '\n' in tag else tag
        if name:
            names.append(name)
    if names:
        json.dump(names, sys.stdout)
except Exception:
    pass
" "$1"
}

while IFS= read -r line; do
  [[ -z "$line" ]] && continue

  if ! path=$(jq -r '.path // empty' <<<"$line"); then
    echo "SKIP (invalid JSONL line): $line" >&2
    continue
  fi
  source_id=$(jq -r '.source_id // empty' <<<"$line")
  basis_rev=$(jq -r '.basis_rev // empty' <<<"$line")

  if [[ -z "$path" ]]; then
    echo "SKIP (missing .path): $line" >&2
    continue
  fi

  if [[ ! -f "$path" ]]; then
    echo "SKIP (not found): $path" >&2
    continue
  fi

  # Read Finder tags via xattr (more reliable than mdls on network volumes)
  tags_json=$(read_tags "$path")

  if [[ -z "$tags_json" || "$tags_json" == "[]" ]]; then
    continue
  fi

  # Convert tag names to facts: ["vakantie", "kids"] -> {"tag.vakantie": "vakantie", "tag.kids": "kids"}
  if ! facts=$(echo "$tags_json" | jq -c '
    map({
      key: ("tag." + (gsub("[^a-zA-Z0-9_-]"; "_") | ascii_downcase)),
      value: .
    }) | from_entries
  '); then
    echo "SKIP (parse error): $path" >&2
    continue
  fi

  if [[ -z "$facts" || "$facts" == "{}" ]]; then
    continue
  fi

  jq -nc \
    --argjson source_id "$source_id" \
    --argjson basis_rev "$basis_rev" \
    --argjson facts "$facts" \
    '{
      source_id: $source_id,
      basis_rev: $basis_rev,
      observed_at: (now | floor),
      facts: $facts
    }'
done
