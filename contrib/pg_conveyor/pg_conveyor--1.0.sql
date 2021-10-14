/* contrib/pg_conveyor/pg_conveyor--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_conveyor" to load this file. \quit

-- Initialize the conveyor belt for the relation.
CREATE FUNCTION pg_conveyor_init(relid OID)
RETURNS void
AS 'MODULE_PATHNAME', 'pg_conveyor_init'
LANGUAGE C STRICT;

/* Insert given data in the relation's conveyor belt. */
CREATE FUNCTION pg_conveyor_insert(relid OID, data TEXT)
RETURNS void
AS 'MODULE_PATHNAME', 'pg_conveyor_insert'
LANGUAGE C STRICT;

/* Read relation's conveyor belt data. */
CREATE FUNCTION pg_conveyor_read(relid OID, blockno int)
RETURNS TEXT
AS 'MODULE_PATHNAME', 'pg_conveyor_read'
LANGUAGE C STRICT;
