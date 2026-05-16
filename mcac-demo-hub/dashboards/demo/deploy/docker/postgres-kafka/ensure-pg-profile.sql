-- Run on primary database postgres as superuser (after image includes pg_profile extension files).
CREATE EXTENSION IF NOT EXISTS dblink;
CREATE SCHEMA IF NOT EXISTS profile;
CREATE EXTENSION IF NOT EXISTS pg_profile SCHEMA profile;
