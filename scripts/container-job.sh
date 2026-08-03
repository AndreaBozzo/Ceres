#!/bin/sh
set -eu

# JSON is the scheduler-friendly default for this finite job entrypoint. An
# explicit CERES_LOG_FORMAT value still wins.
: "${CERES_LOG_FORMAT:=json}"
export CERES_LOG_FORMAT

case "${CERES_MIGRATE_ON_START:-false}" in
    1 | true | yes)
        if ! ceres-migrate; then
            echo "Ceres migrations failed; harvest was not started." >&2
            exit 1
        fi
        ;;
    0 | false | no | "")
        ;;
    *)
        echo "CERES_MIGRATE_ON_START must be true or false." >&2
        exit 1
        ;;
esac

# exec preserves the CLI's stable 0 (success), 2 (partial), and 1 (fatal) exit
# codes for the container scheduler.
exec ceres harvest --metadata-only "$@"
