/* contrib/pgspider_ext/pgcpider_ext--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pgspider_ext" to load this file. \quit

				CREATE FUNCTION pgspider_ext_handler()
			RETURNS fdw_handler
			AS 'MODULE_PATHNAME'
			LANGUAGE C STRICT;

CREATE		FUNCTION
pgspider_ext_validator(text[], oid)
RETURNS void
			AS 'MODULE_PATHNAME'
			LANGUAGE C STRICT;

CREATE		FOREIGN DATA WRAPPER pgspider_ext
			HANDLER pgspider_ext_handler
			VALIDATOR pgspider_ext_validator;
