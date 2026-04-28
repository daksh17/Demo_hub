#!/bin/sh
# Called by PostgreSQL archive_command as: ... %p %f (paths substituted before sh -c).
set -e
WAL_ARCHIVE_DIR=/bitnami/postgresql/wal_archive
mkdir -p "$WAL_ARCHIVE_DIR"
cp "$1" "$WAL_ARCHIVE_DIR/$2"
