/*-------------------------------------------------------------------------
 *
 * pgspider_ext_option.c
 * contrib/pgspider_ext/pgspider_ext_option.c
 *
 * Portions Copyright (c) 2020 - 2022, TOSHIBA CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/reloptions.h"
#include "catalog/pg_foreign_table.h"
#include "commands/defrem.h"
#include "foreign/foreign.h"
#include "miscadmin.h"

#include "pgspider_ext.h"

/* Option name for CREATE FOREIGN TABLE. */
#define OPTION_TABLE	"child_name"

/*
 * Describes the valid options for objects that this wrapper uses.
 */
typedef struct SpdFdwOption
{
	const char *keyword;
	Oid			optcontext;		/* OID of catalog in which option may appear */
}			SpdFdwOption;

/*
 * Valid options for pgspider_ext.
 *
 */
static SpdFdwOption spd_options[] =
{
	/* Connection options */
	{
		OPTION_TABLE, ForeignTableRelationId
	},
	/* Sentinel */
	{
		NULL, InvalidOid
	}
};

extern Datum pgspider_ext_validator(PG_FUNCTION_ARGS);

/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * USER MAPPING or FOREIGN TABLE that uses pgspider_ext.
 *
 * Raise an ERROR if the option or its value is considered invalid.
 */
PG_FUNCTION_INFO_V1(pgspider_ext_validator);




/*
 * Check if the provided option is one of the valid options.
 * context is the Oid of the catalog holding the object the option is for.
 */
static bool
spdIsValidOption(const char *option, Oid context)
{
	SpdFdwOption *opt;

	for (opt = spd_options; opt->keyword; opt++)
	{
		if (context == opt->optcontext && strcmp(opt->keyword, option) == 0)
			return true;
	}
	return false;
}

/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * USER MAPPING or FOREIGN TABLE that uses pgspider_ext.
 *
 * Raise an ERROR if the option or its value is considered invalid.
 */

Datum
pgspider_ext_validator(PG_FUNCTION_ARGS)
{
	List	   *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalog = PG_GETARG_OID(1);
	ListCell   *cell;

	/*
	 * Check that only options supported by griddb_fdw, and allowed for the
	 * current object type, are given.
	 */
	foreach(cell, options_list)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

		if (!spdIsValidOption(def->defname, catalog))
		{
			SpdFdwOption *opt;
			StringInfoData buf;

			/*
			 * Unknown option specified, complain about it. Provide a hint
			 * with list of valid options for the object.
			 */
			initStringInfo(&buf);
			for (opt = spd_options; opt->keyword; opt++)
			{
				if (catalog == opt->optcontext)
					appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "",
									 opt->keyword);
			}

			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid option \"%s\"", def->defname),
					 errhint("Valid options in this context are: %s", buf.len ? buf.data : "<none>")
					 ));
		}
	}
	PG_RETURN_VOID();
}

/*
 * Fetch the options for a griddb_fdw foreign table.
 */
SpdPpt *
spd_get_options(Oid foreignoid)
{
	ForeignTable *f_table = NULL;
	ForeignServer *f_server = NULL;
	UserMapping *f_mapping;
	List	   *options;
	ListCell   *lc;
	SpdPpt	   *opt;

	opt = (SpdPpt *) palloc0(sizeof(SpdPpt));

	/*
	 * Extract options from FDW objects.
	 */
	PG_TRY();
	{
		f_table = GetForeignTable(foreignoid);
		f_server = GetForeignServer(f_table->serverid);
	}
	PG_CATCH();
	{
		f_table = NULL;
		f_server = GetForeignServer(foreignoid);
	}
	PG_END_TRY();

	f_mapping = GetUserMapping(GetUserId(), f_server->serverid);

	options = NIL;
	if (f_table)
		options = list_concat(options, f_table->options);
	options = list_concat(options, f_server->options);
	options = list_concat(options, f_mapping->options);

	/* Loop through the options, and get the server/port */
	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, OPTION_TABLE) == 0)
			opt->child_name = defGetString(def);

	}

	return opt;
}
