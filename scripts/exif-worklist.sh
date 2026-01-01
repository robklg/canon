#!/usr/bin/env bash
#
# Extract EXIF metadata from files in canon worklist
#
# Usage:
#   canon worklist | ./scripts/exif-worklist.sh | canon import-facts
#
# Reads JSONL from stdin, extracts EXIF data using exiftool, outputs JSONL facts.
# Skips files that don't exist or have no EXIF data.
#
# Requires: exiftool, jq

set -euo pipefail

if ! command -v exiftool &> /dev/null; then
    echo "Error: exiftool is required but not installed" >&2
    exit 1
fi

while IFS= read -r line; do
    path=$(echo "$line" | jq -r '.path')
    source_id=$(echo "$line" | jq -r '.source_id')
    basis_rev=$(echo "$line" | jq -r '.basis_rev')

    if [[ ! -f "$path" ]]; then
        echo "SKIP (not found): $path" >&2
        continue
    fi

    # Extract EXIF as JSON, filter to useful fields
    exif_json=$(exiftool -json -DateTimeOriginal -CreateDate -ModifyDate \
        -Make -Model -LensModel -FocalLength -FNumber -ExposureTime -ISO \
        -ImageWidth -ImageHeight -Orientation -GPSLatitude -GPSLongitude \
        -MIMEType -FileType -Duration -VideoFrameRate -AudioChannels \
        "$path" 2>/dev/null | jq '.[0] // empty') || {
        echo "SKIP (exiftool error): $path" >&2
        continue
    }

    if [[ -z "$exif_json" || "$exif_json" == "null" ]]; then
        echo "SKIP (no metadata): $path" >&2
        continue
    fi

    # Convert exiftool output to canon facts format
    # Prefix keys with appropriate namespace and normalize
    facts=$(echo "$exif_json" | jq '
        to_entries
        | map(select(.key != "SourceFile" and .value != null and .value != ""))
        | map({
            key: (
                if .key == "MIMEType" then "mime_type"
                elif .key == "FileType" then "file_type"
                elif .key == "ImageWidth" then "image.width"
                elif .key == "ImageHeight" then "image.height"
                elif .key == "Duration" then "video.duration"
                elif .key == "VideoFrameRate" then "video.frame_rate"
                elif .key == "AudioChannels" then "audio.channels"
                else "exif." + (.key | gsub("(?<a>[a-z])(?<b>[A-Z])"; "\(.a)_\(.b)") | ascii_downcase)
                end
            ),
            value: .value
        })
        | from_entries
    ')

    if [[ "$facts" == "{}" ]]; then
        echo "SKIP (no useful metadata): $path" >&2
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
