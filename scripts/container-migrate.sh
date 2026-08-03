#!/bin/sh
set -eu

: "${DATABASE_URL:?DATABASE_URL must be set}"

migrations_dir="${CERES_MIGRATIONS_DIR:-/usr/local/share/ceres/migrations}"

if [ ! -d "$migrations_dir" ]; then
    echo "Migration directory not found: $migrations_dir" >&2
    exit 1
fi

psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c \
    "CREATE TABLE IF NOT EXISTS schema_migrations (filename text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now());"

for migration in "$migrations_dir"/*.sql; do
    [ -f "$migration" ] || continue
    filename=$(basename "$migration")
    escaped_filename=$(printf '%s' "$filename" | sed "s/'/''/g")
    applied=$(psql "$DATABASE_URL" -t -A -c \
        "SELECT 1 FROM schema_migrations WHERE filename = '$escaped_filename'" || true)

    if [ "$applied" = "1" ]; then
        echo "already applied: $filename"
        continue
    fi

    echo "applying: $filename"
    psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f "$migration"
    psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c \
        "INSERT INTO schema_migrations (filename) VALUES ('$escaped_filename');"
done

echo "Ceres database migrations are current."
