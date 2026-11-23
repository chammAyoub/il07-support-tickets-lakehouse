#!/bin/bash
set -e

superset db upgrade

superset fab create-admin \
  --username "${SUPERSET_ADMIN_USER:-admin}" \
  --firstname "${SUPERSET_ADMIN_FIRSTNAME:-Zainab}" \
  --lastname "${SUPERSET_ADMIN_LASTNAME:-Errossafi}" \
  --email "${SUPERSET_ADMIN_EMAIL:-admin@example.com}" \
  --password "${SUPERSET_ADMIN_PASSWORD:-admin}" || true

superset init

exec superset run -h 0.0.0.0 -p 8088 --with-threads