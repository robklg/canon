#!/usr/bin/env bash
#
# Extract EXIF metadata from files in canon worklist
#
# Usage:
#   canon worklist | ./scripts/exif-worklist.sh | canon import-facts
#
# Reads JSONL from stdin, extracts EXIF data using exiftool, outputs JSONL facts.
# Skips files that don't exist or have no useful metadata.
#
# Requires: exiftool, jq

set -euo pipefail

if ! command -v exiftool &> /dev/null; then
  echo "Error: exiftool is required but not installed" >&2
  exit 1
fi

if ! command -v jq &> /dev/null; then
  echo "Error: jq is required but not installed" >&2
  exit 1
fi

while IFS= read -r line; do
  # Skip empty lines defensively
  [[ -z "$line" ]] && continue

  # Parse input JSONL safely (skip invalid lines)
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

  # Extract EXIF as JSON, filter to useful fields (focused on images and videos)
  if ! exif_json=$(
    exiftool -json -n \
      -api LargeFileSupport=1 \
      -api QuickTimeUTC=1 \
      -api Geolocation \
      -charset filename=utf8 \
      -DateTimeOriginal -SubSecDateTimeOriginal \
      -CreateDate -SubSecCreateDate \
      -ModifyDate -SubSecModifyDate \
      -MediaCreateDate -MediaModifyDate \
      -TrackCreateDate -TrackModifyDate \
      -Make -Model -Software \
      -LensModel -LensMake \
      -FocalLength -FNumber -ExposureTime -ISO \
      -SerialNumber -BodySerialNumber \
      -GPSLatitude -GPSLongitude -GPSAltitude \
      -GPSDateTime \
      "-Geolocation*" \
      -ImageWidth -ImageHeight -Orientation \
      -Duration -MediaDuration \
      -VideoCodec -AudioCodec -AudioChannels \
      -VideoFrameRate -AvgBitrate -Bitrate \
      -Megapixels \
      "$path" 2>/dev/null | jq -c '.[0] // empty'
  ); then
    echo "SKIP (exiftool/jq error): $path" >&2
    continue
  fi

  if [[ -z "$exif_json" || "$exif_json" == "null" ]]; then
    echo "SKIP (no metadata): $path" >&2
    continue
  fi

  # Convert exiftool output to canon facts format + normalized duplicates
  if ! facts=$(jq -c '
    def snake:
      gsub("(?<a>[a-z])(?<b>[A-Z])"; "\(.a)_\(.b)") | ascii_downcase;

    # Fields that have normalized versions (excluded from exif.* namespace)
    def is_normalized:
      IN(
        # Dimensions/media
        "ImageWidth", "ImageHeight", "Megapixels", "Orientation",
        "AvgBitrate", "Bitrate", "Duration", "MediaDuration",
        "VideoFrameRate", "AudioChannels", "VideoCodec", "AudioCodec",
        # Device
        "Make", "Model", "Software", "SerialNumber", "BodySerialNumber",
        # Lens
        "LensMake", "LensModel", "FocalLength",
        # Camera
        "FNumber", "ExposureTime", "ISO",
        # GPS
        "GPSLatitude", "GPSLongitude", "GPSAltitude", "GPSDateTime",
        # Datetime
        "DateTimeOriginal", "SubSecDateTimeOriginal",
        "CreateDate", "SubSecCreateDate",
        "ModifyDate", "SubSecModifyDate",
        "MediaCreateDate", "MediaModifyDate",
        "TrackCreateDate", "TrackModifyDate"
      );

    . as $ex
    | (to_entries
        | map(select(
            .key != "SourceFile" and
            (.key | startswith("Geolocation") | not) and
            (.key | is_normalized | not) and
            .value != null and .value != ""
          ))
        | map({ key: ("exif." + (.key | snake)), value: .value })
        | from_entries
      ) as $facts

    | ($facts + {
        "media.capture_datetime":
          ( $ex.SubSecDateTimeOriginal // $ex.DateTimeOriginal
            // $ex.MediaCreateDate // $ex.TrackCreateDate
            // $ex.SubSecCreateDate // $ex.CreateDate
          ),
        "media.create_datetime":
          ( $ex.MediaCreateDate // $ex.TrackCreateDate
            // $ex.SubSecCreateDate // $ex.CreateDate
          ),
        "media.modify_datetime":
          ( $ex.MediaModifyDate // $ex.TrackModifyDate
            // $ex.SubSecModifyDate // $ex.ModifyDate
            // $ex.FileModifyDate
          ),

        "device.make": ($ex.Make | if type == "number" then tostring else . end),
        "device.model": ($ex.Model | if type == "number" then tostring else . end),
        "device.software": ($ex.Software | if type == "number" then tostring else . end),
        "device.serial": (($ex.BodySerialNumber // $ex.SerialNumber) | if type == "number" then tostring else . end),

        "lens.make": ($ex.LensMake | if type == "number" then tostring else . end),
        "lens.model": ($ex.LensModel | if type == "number" then tostring else . end),
        "lens.focal_length": $ex.FocalLength,

        "camera.aperture": $ex.FNumber,
        "camera.shutter": $ex.ExposureTime,
        "camera.iso": $ex.ISO,

        "media.width": $ex.ImageWidth,
        "media.height": $ex.ImageHeight,
        "media.orientation": $ex.Orientation,
        "image.megapixels": $ex.Megapixels,

        "geo.lat": $ex.GPSLatitude,
        "geo.lon": $ex.GPSLongitude,
        "geo.alt": $ex.GPSAltitude,
        "geo.datetime": $ex.GPSDateTime,

        "geo.city": ($ex.GeolocationCity | if type == "number" then tostring else . end),
        "geo.region": ($ex.GeolocationRegion | if type == "number" then tostring else . end),
        "geo.subregion": ($ex.GeolocationSubregion | if type == "number" then tostring else . end),
        "geo.country_code": ($ex.GeolocationCountryCode | if type == "number" then tostring else . end),
        "geo.country": ($ex.GeolocationCountry | if type == "number" then tostring else . end),
        "geo.timezone": ($ex.GeolocationTimeZone | if type == "number" then tostring else . end),
        "geo.feature_code": ($ex.GeolocationFeatureCode | if type == "number" then tostring else . end),
        "geo.feature_type": ($ex.GeolocationFeatureType | if type == "number" then tostring else . end),
        "geo.population": $ex.GeolocationPopulation,
        "geo.distance": $ex.GeolocationDistance,
        "geo.bearing": $ex.GeolocationBearing,

        "video.codec": ($ex.VideoCodec | if type == "number" then tostring else . end),
        "video.duration": ($ex.MediaDuration // $ex.Duration),
        "video.frame_rate": $ex.VideoFrameRate,

        "audio.codec": ($ex.AudioCodec | if type == "number" then tostring else . end),
        "audio.channels": $ex.AudioChannels,

        "media.bitrate": ($ex.AvgBitrate // $ex.Bitrate)
      })
    | with_entries(select(.value != null and .value != ""))
  ' <<<"$exif_json"); then
    echo "SKIP (facts jq error): $path" >&2
    continue
  fi

  if [[ -z "$facts" || "$facts" == "{}" ]]; then
    echo "SKIP (no useful metadata): $path" >&2
    continue
  fi

  # Emit canon import-facts JSONL record
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
