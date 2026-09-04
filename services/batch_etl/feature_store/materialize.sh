#!/bin/sh
set -eu

# Materialize through the previous completed hour; the scheduler runs at :05.
END_TIME="$(date -u -d '1 hour ago' '+%Y-%m-%dT%H:00:00')"
exec feast materialize-incremental "$END_TIME"
