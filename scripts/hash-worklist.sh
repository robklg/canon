#!/usr/bin/env bash
#
# Hash files from canon worklist and output facts for import
#
# Usage:
#   canon worklist | ./scripts/hash-worklist.sh | canon import-facts
#
# Reads JSONL from stdin, computes sha256 for each file, outputs JSONL facts.
# Skips files that don't exist or can't be read.

set -euo pipefail

while IFS= read -r line; do
    path=$(echo "$line" | jq -r '.path')
    source_id=$(echo "$line" | jq -r '.source_id')
    basis_rev=$(echo "$line" | jq -r '.basis_rev')

    if [[ ! -f "$path" ]]; then
        echo "SKIP (not found): $path" >&2
        continue
    fi

    hash=$(shasum -a 256 "$path" 2>/dev/null | cut -d' ' -f1) || {
        echo "SKIP (read error): $path" >&2
        continue
    }

    jq -nc \
        --argjson source_id "$source_id" \
        --argjson basis_rev "$basis_rev" \
        --arg hash "$hash" \
        '{
            source_id: $source_id,
            basis_rev: $basis_rev,
            observed_at: (now | floor),
            facts: {"content_hash.sha256": $hash}
        }'
done
