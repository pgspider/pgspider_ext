/*-------------------------------------------------------------------------
 *
 * pgspider_ext.c
 * contrib/pgspider_ext/pgspider_ext.c
 *
 * Portions Copyright (c) 2020, TOSHIBA CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "fmgr.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#include <stdio.h>
#include <stddef.h>
#include "access/relation.h"
#include "access/table.h"
#include "catalog/namespace.h"
#include "commands/explain.h"
#include "catalog/partition.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "nodes/makefuncs.h"
#include "nodes/value.h"
#include "miscadmin.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/optimizer.h"
#include "optimizer/tlist.h"
#include "parser/parse_clause.h"
#include "parser/parse_oper.h"
#if PG_VERSION_NUM >= 160000
#include "parser/parse_relation.h"
#endif
#include "utils/builtins.h"
#if PG_VERSION_NUM >= 160000
#include "utils/guc_hooks.h"
#else
#include "utils/guc.h"
#endif
#include "utils/lsyscache.h"
#include "utils/partcache.h"
#include "utils/syscache.h"
#include "storage/lmgr.h"

#include "pgspider_ext.h"


#define PGSPIDER_FDW_NAME "pgspider_fdw"
#define PARQUET_S3_FDW_NAME "parquet_s3_fdw"


#define createChildPathKey(pathkey, attrno_to_child) \
	pathKeyDeepCopyWithAttrnoChange(pathkey, attrno_to_child)

#define createParentPathKey(pathkey, attrno_to_parent) \
	pathKeyDeepCopyWithAttrnoChange(pathkey, attrno_to_parent)

/*
 * This structure stores a child node plan information for child table.
 */
typedef struct ChildPlanInfo
{
	Oid			server_oid;		/* child's server oid */
	Oid			table_oid;		/* child's table oid */
	AttrNumber *attrno_to_child;	/* attno mapping from parent to child */
	AttrNumber *attrno_to_parent;	/* attno mapping from child to parent */
	FdwRoutine *fdw_routine;	/* FDW routine */
	RelOptInfo *baserel;		/* child's baserel */
	Path	   *path;			/* child's path */
	Plan	   *plan;			/* child's plan */
	PlannerInfo *root;			/* child's root */
	RelOptInfo *grouped_rel_local;	/* child's upper relation */
	PlannerInfo *grouped_root_local;	/* child's upper root */
}			ChildPlanInfo;

/*
 * This structure stores a child node state when executing a query.
 */
typedef struct ChildScanInfo
{
	Oid			server_oid;		/* child's server oid */
	Oid			table_oid;		/* child's table oid */
	AttrNumber *attrno_to_child;	/* attno mapping from parent to child */
	AttrNumber *attrno_to_parent;	/* attno mapping from child to parent */
	FdwRoutine *fdw_routine;	/* FDW routine */
	Plan	   *plan;			/* child's plan */
	Query	   *parse;			/* child's root->parse */
	ForeignScanState *fsstate;	/* ForeignScan state data */
}			ChildScanInfo;

/*
 * This structure stores information shared between FDW routines of planning like
 * GetForeignRelSize(), GetForeignPaths(), GetForeignUpperPaths() and GetForeignPlan().
 */
typedef struct SpdFdwPlanState
{
	Oid			table_oid;		/* parent's server oid */
	bool		is_upper;		/* true if upper relation */
	int			partkey_idx;	/* location of partition key in table slot. -1
								 * if not used */
	AttrNumber	partkey_attno;	/* column number of partition key */
	List	   *partkey_conds;	/* Expr using partition key in WHERE clause */
	Expr	   *partkey_expr;	/* Expr using partition key in target list on
								 * upper path */
	RelOptInfo *outerrel;		/* join information */
	ChildPlanInfo child_plan_info;	/* child information */
	HTAB	   *aggsplit_history;	/* store old aggsplit value */
}			SpdFdwPlanState;

/*
 * This structure stores information shared between FDW routines of execution like
 * BeginForeignScan(), IterateForeignScan() and EndForeignScan().
 */
typedef struct SpdFdwScanState
{
	Oid			table_oid;		/* parent's server oid */
	int			partkey_idx;	/* index of partition key in table slot. -1 if
								 * not used */
	AttrNumber	partkey_attno;	/* column number of partition key */
	bool		is_upper;		/* true if upper relation */
	bool		is_first;		/* true if it is a first time of
								 * IterationForeignScan */
	ChildScanInfo child_scan_info;	/* child information */
}			SpdFdwScanState;

typedef struct SpdFdwModifyState
{
	Oid			modify_server_oid;
}			SpdFdwModifyState;

/*
 * FDW callback routines
 */
static void spdGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
								 Oid foreigntableid);
static void spdGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
							   Oid foreigntableid);
static ForeignScan *spdGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel,
									  Oid foreigntableid, ForeignPath *best_path,
									  List *tlist, List *scan_clauses,
									  Plan *outer_plan);
static void spdBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *spdIterateForeignScan(ForeignScanState *node);
static void spdReScanForeignScan(ForeignScanState *node);
static void spdEndForeignScan(ForeignScanState *node);
static void spdExplainForeignScan(ForeignScanState *node, ExplainState *es);
static void spdGetForeignUpperPaths(PlannerInfo *root,
									UpperRelationKind stage,
									RelOptInfo *input_rel,
									RelOptInfo *output_rel, void *extra);
static bool spdIsForeignScanParallelSafe(PlannerInfo *root,
										 RelOptInfo *rel, RangeTblEntry *rte);

static void spdBuildRelationAliases(TupleDesc tupdesc, Alias *alias, Alias *eref);

/* Declarations for dynamic loading */
PG_FUNCTION_INFO_V1(pgspider_ext_handler);
PG_FUNCTION_INFO_V1(pgspider_ext_version);

/*
 * pgspider_ext_handler populates an FdwRoutine with pointers to the functions
 * implemented within this file.
 */
Datum
pgspider_ext_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *routine = makeNode(FdwRoutine);

	/* Functions for scanning foreign tables */
	routine->GetForeignRelSize = spdGetForeignRelSize;
	routine->GetForeignPaths = spdGetForeignPaths;
	routine->GetForeignPlan = spdGetForeignPlan;
	routine->BeginForeignScan = spdBeginForeignScan;
	routine->IterateForeignScan = spdIterateForeignScan;
	routine->ReScanForeignScan = spdReScanForeignScan;
	routine->EndForeignScan = spdEndForeignScan;

	/* Functions for updating foreign tables */
	routine->AddForeignUpdateTargets = NULL;
	routine->PlanForeignModify = NULL;
	routine->BeginForeignModify = NULL;
	routine->ExecForeignInsert = NULL;
	routine->ExecForeignUpdate = NULL;
	routine->ExecForeignDelete = NULL;
	routine->EndForeignModify = NULL;
	routine->BeginForeignInsert = NULL;
	routine->EndForeignInsert = NULL;
	routine->IsForeignRelUpdatable = NULL;
	routine->PlanDirectModify = NULL;
	routine->BeginDirectModify = NULL;
	routine->IterateDirectModify = NULL;
	routine->EndDirectModify = NULL;

	/* Function for EvalPlanQual rechecks */
	routine->RecheckForeignScan = NULL;
	/* Support functions for EXPLAIN */
	routine->ExplainForeignScan = spdExplainForeignScan;
	routine->ExplainForeignModify = NULL;
	routine->ExplainDirectModify = NULL;

	/* Support functions for ANALYZE */
	routine->AnalyzeForeignTable = NULL;

	/* Support functions for IMPORT FOREIGN SCHEMA */
	routine->ImportForeignSchema = NULL;

	/* Support functions for join push-down */
	routine->GetForeignJoinPaths = NULL;

	/* Support functions for upper relation push-down */
	routine->GetForeignUpperPaths = spdGetForeignUpperPaths;

	/* Support functions for parallelism under Gather node */
	routine->IsForeignScanParallelSafe = spdIsForeignScanParallelSafe;

	PG_RETURN_POINTER(routine);
}

Datum
pgspider_ext_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(CODE_VERSION);
}

/*
 * child_tableid_from_parentid
 *	  Get child table oid from parent table oid. If an option of child
 *	  table name is specified, we use it. If it is not specified, we
 *	  use the parent table name + "_child".
 *
 * @param[in] foreigntableid - parent's table oid
 * @return Oid - child's table oid
 */
static Oid
child_tableid_from_parentid(Oid userid, Oid foreigntableid)
{
	char	   *child_name;
	SpdOpt	   *option = spd_get_options(userid, foreigntableid);
	Oid			child_table_oid;
	bool		change_search_path = false;

	if (option->child_name)
		child_name = option->child_name;
	else
	{
		Relation	rel = RelationIdGetRelation(foreigntableid);
		char	   *parent_name = RelationGetRelationName(rel);

		child_name = psprintf("%s_child", parent_name);
		RelationClose(rel);
	}

	/*
	 * By default, the search path is "pg_catalog" for remote session.
	 * Therefore, need to update namespace_search_path to be able to search in
	 * remote server
	 */
	if (strcmp(namespace_search_path, "pg_catalog") == 0)
	{
		change_search_path = true;
		namespace_search_path = "\"$user\", public";
		assign_search_path("", NULL);
	}

	child_table_oid = RelnameGetRelid(child_name);

	/*
	 * Change back the search path to avoid affecting original behavior
	 */
	if (change_search_path)
	{
		namespace_search_path = "pg_catalog";
		assign_search_path("", NULL);
	}

	if (!OidIsValid(child_table_oid))
		elog(ERROR, "Not found child table: %s", child_name);

	return child_table_oid;
}

/*
 * serverid_of_relation
 *	  Get server oid from table oid.
 *
 * @param[in] foreigntableid - table oid
 * @return Oid - server oid
 */
static Oid
serverid_of_relation(Oid foreigntableid)
{
	ForeignTable *ft = GetForeignTable(foreigntableid);

	return ft->serverid;
}

/*
 * pathKeyDeepCopyWithAttrnoChange
 *	  Create new pathkey by deep-copy manually because copyObject for PathKey
 *	  is sharrow-copy. Only necessary variables is copied. attribute numbers
 *	  are also updated based on attrno_to.
 *
 * @param[in] src - pathkey as a source
 * @param[in] attrno_to_child - information of attrno mapping
 * @return PathKey - created pathkey
 */
static PathKey *
pathKeyDeepCopyWithAttrnoChange(PathKey *src, AttrNumber *attrno_to)
{
	PathKey    *pathkey;
	EquivalenceClass *pk_eclass;
	ListCell   *lc;
	List	   *ec_members = NULL;

	/* This is sharrow copy. */
	pathkey = (PathKey *) copyObject(src);

	/* Copy PathKey->pk_eclass. */
	pk_eclass = (EquivalenceClass *) palloc0(sizeof(EquivalenceClass));
	memcpy(pk_eclass, src->pk_eclass, sizeof(EquivalenceClass));
	pathkey->pk_eclass = pk_eclass;

	/* Copy PathKey->pk_eclass->ec_members. */
	foreach(lc, pk_eclass->ec_members)
	{
		Expr	   *expr;
		EquivalenceMember *em = (EquivalenceMember *) palloc0(sizeof(EquivalenceMember));

		memcpy(em, (EquivalenceMember *) lfirst(lc), sizeof(EquivalenceMember));
		ec_members = lappend(ec_members, em);

		/* Copy PathKey->pk_eclass->ec_members->expr. */
		expr = (Expr *) copyObject(em->em_expr);
		mapVarAttnos((Node *) expr, attrno_to);
		em->em_expr = expr;
	}
	pk_eclass->ec_members = ec_members;

	return pathkey;
}

/*
 * freePathKey
 *	  Free memory of pathkey allocated by createChildPathKey.
 *
 * @param[in] pathkey to be freed
 */
static void
freePathKey(PathKey *pathkey)
{
	EquivalenceClass *pk_eclass = pathkey->pk_eclass;
	ListCell   *lc;

	foreach(lc, pk_eclass->ec_members)
	{
		EquivalenceMember *em = (EquivalenceMember *) lfirst(lc);

		pfree(em);
	}
	list_free(pk_eclass->ec_members);
	pfree(pk_eclass);
}

/*
 * freePathKeys
 *	  Free memory allocated by createChildPathKey in the pathkey list.
 *
 * @param[in] pathkeys pathey's list
 */
static void
freePathKeyList(List *pathkeys)
{
	ListCell   *lc;

	foreach(lc, pathkeys)
	{
		PathKey    *pathkey = (PathKey *) lfirst(lc);

		freePathKey(pathkey);
	}
	list_free(pathkeys);
}

/*
 * createChildRoot
 *	  Create a child PlanerInfo.
 *
 * @param[in] root - parent's planner info
 * @param[in] baserel - parent's relation option
 * @param[in] tableid - child's table oid
 * @param[in] partkey_attno - column number of partition key
 * @param[in] attrno_to_child - information of attrno mapping from parent to child
 * @return PlannerInfo* - child's planner info
 */
static PlannerInfo *
createChildRoot(PlannerInfo *root, RelOptInfo *baserel, Oid tableid,
				AttrNumber partkey_attno, AttrNumber *attrno_to_child)
{
	RangeTblEntry *rte;
	Query	   *query;
	int			k;
	PlannerInfo *child_root;
	ListCell   *lc;
	List	   *query_pathkeys = NIL;

	/* Build a minimal RTE for the rel */
	rte = makeNode(RangeTblEntry);
	rte->rtekind = RTE_RELATION;
	rte->relid = tableid;
	rte->relkind = RELKIND_RELATION;
	rte->lateral = false;
	rte->inh = false;
	rte->inFromCl = true;
	rte->rellockmode = AccessShareLock;

	if (tableid != 0)
	{
		Relation rel;
		rte->alias = rte->eref = makeAlias(pstrdup(get_rel_name(tableid)), NIL);
		rel = relation_open(tableid, AccessShareLock);
		spdBuildRelationAliases(rel->rd_att, rte->alias, rte->eref);
		
		table_close(rel, NoLock);	
	}

	/*
	 * Because in build_simple_rel() function, it assumes that a relation was
	 * already locked before open. So, we need to lock relation by id in dummy
	 * root in advance.
	 */
	LockRelationOid(rte->relid, rte->rellockmode);

	/*
	 * Set up mostly-dummy planner state PlannerInfo can not deep copy with
	 * copyObject(). BUt It should create dummy PlannerInfo for each child
	 * tables. Following code is copy from plan_cluster_use_sort(), it create
	 * simple PlannerInfo.
	 */
	query = makeNode(Query);
	query->commandType = CMD_SELECT;

	/* Create child range table */
	query->rtable = list_make1(rte);
	for (k = 1; k < baserel->relid; k++)
	{
		query->rtable = lappend(query->rtable, rte);
	}

#if PG_VERSION_NUM >= 160000
	/* Create RTEPermissionInfo */
	addRTEPermissionInfo(&query->rteperminfos, rte);
#endif

	child_root = makeNode(PlannerInfo);
	child_root->parse = query;
	child_root->glob = makeNode(PlannerGlobal);
	child_root->query_level = 1;
	child_root->planner_cxt = CurrentMemoryContext;
	child_root->wt_param_id = -1;
	child_root->rowMarks = (List *) copyObject(root->rowMarks);
#if PG_VERSION_NUM >= 160000
	/* Copy JoinDomain list */
	child_root->join_domains = root->join_domains;
#endif

	/*
	 * Check whether ORDER BY clause uses a partition key. If it is used, we
	 * cannot pushdown. If it is not used, we can pass the information to
	 * child with updating attrno.
	 */
	foreach(lc, root->query_pathkeys)
	{
		PathKey    *pathkey = (PathKey *) lfirst(lc);
		PathKey    *child_pathkey;
		Expr	   *expr;

		/* Check if a partition key is used. */
		expr = getExprInPathKey(pathkey, baserel);
		if (hasPartKeyExpr((Node *) expr, partkey_attno))
		{
			freePathKeyList(query_pathkeys);
			query_pathkeys = NULL;
			break;
		}

		child_pathkey = createChildPathKey(pathkey, attrno_to_child);
		query_pathkeys = lappend(query_pathkeys, child_pathkey);
	}
	child_root->query_pathkeys = query_pathkeys;

	/*
	 * Use placeholder list only for child node's GetForeignRelSize in this
	 * routine. PlaceHolderVar in relation target list will be checked against
	 * PlaceHolder List in root planner info.
	 */
	child_root->placeholder_list = (List *) copyObject(root->placeholder_list);

	/* Set up RTE/RelOptInfo arrays */
	setup_simple_rel_arrays(child_root);

	return child_root;
}

/*
 * createChildBaserel
 *	  Create child base relation based on from parent information.
 *	  varattno in expr in relation target are updated.
 *
 * @param[in] root - parent's planner info
 * @param[in] baserel - parent's base relation
 * @param[in] child_root - child's planner info
 * @param[in] attrno_to_child - information of attrno mapping from parent to child
 * @param[in] is_pgspider_fdw - true if it is PGSpider FDW
 * @param[in] partkey_attno - column number of partition key
 * @param[out] partkey_conds - WHERE consition expressions which contains partition key
 * @param[out] partkey_idx - index of partition key in target list
 * @return RelOptInfo - created relation info
 */
static RelOptInfo *
createChildBaserel(PlannerInfo *root, RelOptInfo *baserel,
				   PlannerInfo *child_root, AttrNumber *attrno_to_child,
				   AttrNumber partkey_attno, List **partkey_conds,
				   List **partkey_idxes)
{
	RelOptInfo *child_baserel;
	ListCell   *lc;
	List	   *exprs = NIL;	/* new exprs after remove a partition key */
	List	   *restrictinfo = NIL; /* new restrictinfo after remove a
									 * partition key */
	List	   *removed_exprs = NIL;	/* Removed exprs */
	PartkeyWalkerContext context;

	context.partkey_attno = partkey_attno;
	context.exprs = NIL;

	child_baserel = build_simple_rel(child_root, baserel->relid, (RelOptInfo *) RELOPT_BASEREL);

	/*
	 * Get target lists by removing a partition key from parent's target
	 * lists.
	 */
	exprs = copyObject(baserel->reltarget->exprs);
	exprs = removePartkeyFromTargets(exprs, partkey_attno, partkey_idxes);

	/* Update varattno for mapping from a parent table to a child table. */
	exprs = mapVarAttnosInList(exprs, attrno_to_child);

	/*
	 * Create restrictinfo and append exprs of WHERE clauses if a partition
	 * key is not contained.
	 */
	foreach(lc, baserel->baserestrictinfo)
	{
		RestrictInfo *clause = (RestrictInfo *) lfirst(lc);
		Expr	   *expr = (Expr *) clause->clause;

		/*
		 * If the condition uses a partition key, it cannot pushdown to child
		 * FDW. PGSpider finds variables which are used in the condition in
		 * order to add them into a target list. PostgreSQL core needs them
		 * for calculating the clause. If the condition does not use partition
		 * key, it can pass to child FDW. So we add them into restrictinfo for
		 * child.
		 */
		if (hasPartKeyExpr((Node *) expr, partkey_attno))
		{
			removed_exprs = lappend(removed_exprs, expr);
			extract_var_walker((Node *) expr, &context);
		}
		else
		{
			RestrictInfo *child_clause = (RestrictInfo *) copyObject(clause);

			/*
			 * Update varattno for mapping from a parent table to a child
			 * table.
			 */
			child_clause->clause = (Expr *) mapVarAttnos((Node *) child_clause->clause,
														 attrno_to_child);

			restrictinfo = lappend(restrictinfo, child_clause);
		}
	}

	/* Append exprs used in restrictinfo which cannot be pushed down. */
	foreach(lc, context.exprs)
	{
		Expr	   *expr = (Expr *) lfirst(lc);

		/* Chech if it already exists. */
		if (!exprlist_member(expr, exprs))
			exprs = lappend(exprs, expr);
	}

	child_baserel->reltarget->exprs = exprs;
	child_baserel->baserestrictinfo = restrictinfo;
	*partkey_conds = removed_exprs;

	return child_baserel;
}

/*
 * getForeignRelSizeChild
 *	  Call GetForeignRelSize for child FDW and set ChildInfo.
 *
 * @param[in] root - parent's planner info
 * @param[in] baserel - parent's relation option
 * @param[in] foreigntableid - parent's table oid
 * @param[in] partkey_attno - column number of partition key
 * @param[out] childplaninfo - child's plan info is set to here
 * @param[out] partkey_conds - WHERE consition expressions which contains a partition key
 * @param[out] partkey_idx - index of partition key in the target list
 */
static void
getForeignRelSizeChild(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid,
					   AttrNumber partkey_attno, ChildPlanInfo * childplaninfo,
					   List **partkey_conds, List **partkey_idx)
{
	Oid			child_table_oid;
	FdwRoutine *fdw_routine;
	Oid			child_server_oid;
	PlannerInfo *child_root;
	RelOptInfo *child_baserel;
	AttrNumber *attrno_to_child;
	AttrNumber *attrno_to_parent;
	Oid			userid;
#if PG_VERSION_NUM >= 160000
	/*
	 * If the table or the server is configured to use remote estimates,
	 * identify which user to do remote access as during planning.  This
	 * should match what ExecCheckPermissions() does.  If we fail due to lack
	 * of permissions, the query would have failed at runtime anyway.
	 */
	userid = OidIsValid(baserel->userid) ? baserel->userid : GetUserId();
#else
	userid = GetUserId();
#endif
	/* Find an oid of child table. */
	child_table_oid = child_tableid_from_parentid(userid, foreigntableid);

	/* Find a child FDW routine. */
	child_server_oid = serverid_of_relation(child_table_oid);

	fdw_routine = GetFdwRoutineByServerId(child_server_oid);

	/*
	 * Create base plan for each child table with updating varattno in
	 * relation target. Refer createVarAttrnoMapping()'s comment for the
	 * reason why varattno are updated.
	 */
	createVarAttrnoMapping(foreigntableid, child_table_oid, partkey_attno, &attrno_to_child, &attrno_to_parent);
	child_root = createChildRoot(root, baserel, child_table_oid, partkey_attno, attrno_to_child);
	child_baserel = createChildBaserel(root, baserel, child_root, attrno_to_child,
									   partkey_attno, partkey_conds, partkey_idx);

	fdw_routine->GetForeignRelSize(child_root, child_baserel, child_table_oid);

	childplaninfo->table_oid = child_table_oid;
	childplaninfo->server_oid = child_server_oid;
	childplaninfo->fdw_routine = fdw_routine;
	childplaninfo->root = child_root;
	childplaninfo->baserel = child_baserel;
	childplaninfo->attrno_to_child = attrno_to_child;
	childplaninfo->attrno_to_parent = attrno_to_parent;
}

/*
 * getPartColumnAttno
 *	  Get a column number of partition key.
 *
 * @param[in] foreigntableid - parent's foreing table oid
 * @return AttrNumber - column number
 */
static AttrNumber
getPartColumnAttno(Oid foreigntableid)
{
	Oid			parentid;
	Relation	relation;
	PartitionKey partkey;
	int			i;
	int			num_dropped = 0;

	/* Get a partion parent table. */
#if (PG_VERSION_NUM >= 150000)
	parentid = get_partition_parent(foreigntableid, false);
#else
	parentid = get_partition_parent(foreigntableid);
#endif
	relation = RelationIdGetRelation(parentid);

	/* Get a partion key information. */
	partkey = RelationGetPartitionKey(relation);
	/* We expects that a partition key is only single column. */
	if (partkey->partexprs != NULL || partkey->partnatts != 1)
		elog(ERROR, "A partition key must be only single column.");

	for (i = 0; i < partkey->partattrs[0]; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(relation->rd_att, i);

		if (attr->attisdropped)
			num_dropped++;
	}
	RelationClose(relation);

	return partkey->partattrs[0] - num_dropped;
}

/*
 * spdGetForeignRelSize
 *	  Create a base plan for child table and save it into fdw_private.
 *	  Then estimate # of rows and width of the result of the scan based on child relation.
 *
 * @param[in] root - palrent's planner information
 * @param[in] baserel - parent's relation option
 * @param[in] foreigntableid - parent's foreing table oid
 */
static void
spdGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	SpdFdwPlanState *fdw_private;
	List	   *partkey_idxes = NIL;
	AttrNumber	partkey_attno;

	elog(DEBUG1, "GetForeignRelSize");

	fdw_private = (SpdFdwPlanState *) palloc0(sizeof(SpdFdwPlanState));
	baserel->fdw_private = (void *) fdw_private;

	partkey_attno = getPartColumnAttno(foreigntableid);

	getForeignRelSizeChild(root, baserel, foreigntableid, partkey_attno,
						   &fdw_private->child_plan_info, &fdw_private->partkey_conds,
						   &partkey_idxes);

	/* Use the estimated values of child. */
	if (baserel->pages == 0 && baserel->tuples == 0)
	{
		baserel->rows = fdw_private->child_plan_info.baserel->rows;
		baserel->pages = fdw_private->child_plan_info.baserel->pages;
		baserel->tuples = fdw_private->child_plan_info.baserel->tuples;
	}
	set_baserel_size_estimates(root, baserel);

	fdw_private->table_oid = foreigntableid;
	fdw_private->partkey_attno = partkey_attno;

	/* Memorize expr if the target list has a partition key. */
	if (list_length(partkey_idxes) > 0)
	{
		int			partkey_idx;

		Assert(list_length(partkey_idxes) == 1);
		partkey_idx = intVal(list_nth(partkey_idxes, 0));
		fdw_private->partkey_expr = (Expr *) list_nth(baserel->reltarget->exprs, partkey_idx);
	}
}

/*
 * spdGetForeignPaths
 *	  Create foreign paths for child tables.
 *    Then create parent scan paths based on child paths.
 *
 * @param[in] root - palrent's planner information
 * @param[in] baserel - parent's relation option
 * @param[in] foreigntableid - parent's foreing table oid
 */
static void
spdGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	SpdFdwPlanState *fdw_private = (SpdFdwPlanState *) baserel->fdw_private;
	ChildPlanInfo *child_plan_info;
	ListCell   *lc;
	int			path_pos = 0;
	Oid			oid_server;
	ForeignServer *fs;
	ForeignDataWrapper *fdw;


	elog(DEBUG1, "GetForeignPaths");

	if (fdw_private == NULL)
		elog(ERROR, "fdw_private is NULL");

	child_plan_info = &fdw_private->child_plan_info;

	oid_server = serverid_of_relation(child_plan_info->table_oid);
	fs = GetForeignServer(oid_server);
	fdw = GetForeignDataWrapper(fs->fdwid);

	/*
	 * The ECs need to reached canonical state. Otherwise, pathkeys of
	 * parquet_s3_fdw could be rendered non-canonical.
	 */
	if (strcmp(fdw->fdwname, PARQUET_S3_FDW_NAME) == 0)
		child_plan_info->root->ec_merging_done = root->ec_merging_done;

	/* Create Foreign paths using base_rel_list to each child node. */
	child_plan_info->fdw_routine->GetForeignPaths(child_plan_info->root,
												  child_plan_info->baserel,
												  child_plan_info->table_oid);

	if (child_plan_info->baserel->pathlist == NULL)
		return;

	/* Add paths based on child paths. */
	foreach(lc, child_plan_info->baserel->pathlist)
	{

		Path	   *childpath = (Path *) lfirst(lc);
		Cost		startup_cost;
		Cost		total_cost;
		Cost		rows;
		PathTarget *target;
		List	   *pathkeys = NIL;

		/* Use child node costs */
		startup_cost = childpath->startup_cost;
		total_cost = childpath->total_cost;
		rows = childpath->rows;

		/*
		 * Construct a target list of parent based on child by deep-copy of
		 * Expr.
		 */
		target = copy_pathtarget(childpath->pathtarget);
		list_free(target->exprs);
		target->exprs = copyObject(childpath->pathtarget->exprs);

		/* Update varattno for mapping from a child table to a parent table. */
		target->exprs = mapVarAttnosInList(target->exprs, child_plan_info->attrno_to_parent);

		/* Inserted a partition key. */
		if (fdw_private->partkey_expr)
		{
			/*
			 * If tuple descriptor of child slot will be created based on
			 * relation, parent is also created as same way by setting target
			 * = NULL.
			 */
			if (childpath->pathtarget == child_plan_info->baserel->reltarget)
				target = NULL;
			else
			{
				Expr	   *expr = (Expr *) copyObject(fdw_private->partkey_expr);

				target->exprs = lappend(target->exprs, expr);
			}
		}

		if (childpath->pathkeys)
		{
			/*
			 * PathKey's equality is done by comparing pointers in PostgreSQL
			 * core. So we cannot use copyObject if it needs to keep the
			 * equality.
			 */
			if (compare_pathkeys(childpath->pathkeys, child_plan_info->root->query_pathkeys) == PATHKEYS_EQUAL)
				pathkeys = root->query_pathkeys;
		}

		/*
		 * We specify path_pos as a fdw_private so that spdGetForeignPlan()
		 * can know which path is selected as a best path.
		 */
		add_path(baserel, (Path *) create_foreignscan_path(root, baserel,
														   target,
														   rows,
														   startup_cost,
														   total_cost,
														   pathkeys,
														   baserel->lateral_relids,
														   NULL,	/* no outerpath */
#if PG_VERSION_NUM >= 170000
														   NIL, /* no fdw_restrictinfo list */
#endif
														   list_make1_int(path_pos)));	/* fdw_private */
		path_pos++;
	}
}

/*
 * serializeSpdFdwPrivate
 *	  Serialize SpdFdwPlanState as a list in order to share variables to SpdFdwScanState.
 *	  The order of elements in the list must be the same on serializing and deserializing functions.
 *
 * @param[in] fdw_private - seriarizing data
 * @return List* - serialized data
 */
static List *
serializeSpdFdwPrivate(SpdFdwPlanState * fdw_private)
{
	List	   *fdw_private_list = NIL;
	ChildPlanInfo *child_plan_info = &fdw_private->child_plan_info;

	fdw_private_list = lappend(fdw_private_list, makeInteger(fdw_private->table_oid));
	fdw_private_list = lappend(fdw_private_list, makeInteger(fdw_private->partkey_idx));
	fdw_private_list = lappend(fdw_private_list, makeInteger(fdw_private->is_upper));
	fdw_private_list = lappend(fdw_private_list, makeInteger(fdw_private->partkey_attno));

	/* Append ChildInfo */
	fdw_private_list = lappend(fdw_private_list, makeInteger(child_plan_info->server_oid));
	fdw_private_list = lappend(fdw_private_list, makeInteger(child_plan_info->table_oid));
	fdw_private_list = lappend(fdw_private_list, child_plan_info->plan);
	fdw_private_list = lappend(fdw_private_list, child_plan_info->root->parse);

	return fdw_private_list;
}

/*
 * deserializeSpdFdwPrivate
 *	  Deserialize SpdFdwScanState from SpdFdwPlanState.
 *	  The order of elements in the list must be the same on serializing and deserializing functions.
 *
 * @param[in] fdw_private_list - deseriarizing data
 * @return SpdFdwScanState* - deserialized data
 */
static SpdFdwScanState *
deserializeSpdFdwPrivate(List *fdw_private_list)
{
	ListCell   *lc = list_head(fdw_private_list);
	SpdFdwScanState *fdw_state = (SpdFdwScanState *) palloc0(sizeof(SpdFdwScanState));
	ChildScanInfo *child_scan_info = &fdw_state->child_scan_info;
	AttrNumber *attrno_to_child;
	AttrNumber *attrno_to_parent;

	fdw_state->table_oid = intVal(lfirst(lc));
	lc = lnext(fdw_private_list, lc);

	fdw_state->partkey_idx = intVal(lfirst(lc));
	lc = lnext(fdw_private_list, lc);

	fdw_state->is_upper = intVal(lfirst(lc)) ? true : false;
	lc = lnext(fdw_private_list, lc);

	fdw_state->partkey_attno = intVal(lfirst(lc));
	lc = lnext(fdw_private_list, lc);

	child_scan_info->server_oid = intVal(lfirst(lc));
	lc = lnext(fdw_private_list, lc);

	child_scan_info->table_oid = intVal(lfirst(lc));
	lc = lnext(fdw_private_list, lc);

	child_scan_info->plan = (Plan *) lfirst(lc);
	lc = lnext(fdw_private_list, lc);

	child_scan_info->parse = (Query *) lfirst(lc);
	lc = lnext(fdw_private_list, lc);

	/*
	 * Following varibales cannot be serialized because copyObject called by
	 * PostgreSQL core does not support it. So we calculate them again.
	 */
	createVarAttrnoMapping(fdw_state->table_oid, child_scan_info->table_oid, fdw_state->partkey_attno,
						   &attrno_to_child, &attrno_to_parent);
	child_scan_info->attrno_to_child = attrno_to_child;
	child_scan_info->attrno_to_parent = attrno_to_parent;

	child_scan_info->fdw_routine = GetFdwRoutineByServerId(child_scan_info->server_oid);

	return fdw_state;
}

/*
 * createChildPlan
 *	  Create foreign plan for child tables.
 *
 * @param[in] root - parent's planner info
 * @param[in] baserel - parent's base relation
 * @param[in] best_path_pos - position of best path in the path list
 * @param[in] tlist - parent's target list
 * @param[in] outer_plan - parent's outer plan
 * @param[in] partkey_attno - column number of partition key
 * @param[in,out] child_plan_info - child plan info
 * @return ForeignScan - created foreign plan
 */
static ForeignScan *
createChildPlan(PlannerInfo *root, RelOptInfo *baserel, int best_path_pos,
				List *tlist, Plan *outer_plan, AttrNumber partkey_attno,
				ChildPlanInfo * child_plan_info)
{
	ForeignScan *child_plan;
	Path	   *child_path;
	List	   *child_tlist = NIL;
	Oid			child_table_oid = child_plan_info->table_oid;
	PlannerInfo *child_root;
	RelOptInfo *child_baserel;
	List	   *restrictinfo;
	List	   *idxes = NIL;

	/* Choose the best path in the list. */
	child_path = (Path *) list_nth(child_plan_info->baserel->pathlist, best_path_pos);
	child_plan_info->path = child_path;

	/* Create child tlist based on that of parent by removing a partition key. */
	child_tlist = removePartkeyFromTargets(copyObject(tlist), partkey_attno, &idxes);
	child_tlist = mapVarAttnosInList(child_tlist, child_plan_info->attrno_to_child);

	/* Create plan of child FDW */
	if (IS_SIMPLE_REL(baserel))
	{
		child_root = child_plan_info->root;
		child_baserel = child_plan_info->baserel;
		restrictinfo = child_plan_info->baserel->baserestrictinfo;
	}
	else
	{
		child_root = child_plan_info->grouped_root_local;
		child_baserel = child_plan_info->grouped_rel_local;
		restrictinfo = NIL;
	}

	child_plan = child_plan_info->fdw_routine->GetForeignPlan(child_root,
															  child_baserel,
															  child_table_oid,
															  (ForeignPath *) child_path,
															  child_tlist,
															  restrictinfo,
															  outer_plan);

	if (child_plan_info->baserel->reloptkind == RELOPT_UPPER_REL)
		child_plan->fs_relids = child_plan_info->grouped_root_local->all_baserels;
	else
		child_plan->fs_relids = ((ForeignPath *) child_path)->path.parent->relids;

#if PG_VERSION_NUM >= 160000

	/*
	 * Join relid sets include relevant outer joins, but FDWs may need to know
	 * which are the included base rels.  That's a bit tedious to get without
	 * access to the plan-time data structures, so compute it here.
	 */
	child_plan->fs_base_relids = bms_difference(child_plan->fs_relids,
												root->outer_join_rels);
#endif

	return child_plan;
}

/*
 * spdGetForeignPlan
 *	  Create foreign plan of child talbe.
 *	  Then create foreign plan of parent table based on the plan of child table.
 *
 * @param[in] root - parent's planner infromation
 * @param[in] baserel - parent's relation option
 * @param[in] foreigntableid - parent's foreing table id
 * @param[in] best_path - parent's path
 * @param[in] tlist - parent target_list
 * @param[in] scan_clauses parent's where conditions
 * @param[in] outer_plan parent's outer plan
 * @return ForeignScan - created foreign plan
 */
static ForeignScan *
spdGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid,
				  ForeignPath *best_path, List *tlist, List *scan_clauses,
				  Plan *outer_plan)
{
	SpdFdwPlanState *fdw_private = (SpdFdwPlanState *) baserel->fdw_private;
	int			best_path_pos = 0;
	Index		scan_relid;
	List	   *fdw_scan_tlist = NIL;
	ListCell   *lc;
	ChildPlanInfo *child_plan_info;
	ForeignScan *child_plan;
	List	   *lfdw_private = NIL;
	List	   *local_exprs = NIL;

	elog(DEBUG1, "GetForeignPlan");

	if (fdw_private == NULL)
		elog(ERROR, "fdw_private is NULL");

	child_plan_info = &fdw_private->child_plan_info;

	/*
	 * Detect a location of best path in the path list. It was stored by
	 * spdGetForeignPaths() and spdGetForeignUpperPaths()
	 */
	best_path_pos = linitial_int(best_path->fdw_private);
	Assert(best_path_pos < list_length(baserel->pathlist));

	/* Create a child plan. */
	child_plan = createChildPlan(root, baserel, best_path_pos, tlist, outer_plan,
								 fdw_private->partkey_attno, child_plan_info);
	child_plan_info->plan = (Plan *) child_plan;

	/*
	 * pgspider_ext uses the same fdw_scan_tlist of child FDW. NULL is also
	 * acceptable. But aggsplit should be reverted because it was changed
	 * forcibly. Refer foreign_expr_walker_aggsplit_change().
	 */
	foreach(lc, child_plan->fdw_scan_tlist)
	{
		TargetEntry *te = (TargetEntry *) copyObject((TargetEntry *) lfirst(lc));
		Expr	   *expr = (Expr *) copyObject(te->expr);
		AggSplitChangeWalkerContext context;

		context.walk_mode = AGG_SPLIT_WALK_REVERT;
		context.history = fdw_private->aggsplit_history;

		foreign_expr_walker_aggsplit_change((Node *) expr, &context);

		te->expr = (Expr *) mapVarAttnos((Node *) expr, child_plan_info->attrno_to_parent);
		fdw_scan_tlist = lappend(fdw_scan_tlist, te);
	}

	/*
	 * If fdw_scan_tlist is not NULL, a partition key will be appended in case
	 * of base relation. If fdw_scan_tlist is NULL, pgspider_ext also uses
	 * NULL as fdw_scan_tlist.
	 */
	if (IS_SIMPLE_REL(baserel))
	{

		scan_relid = baserel->relid;

		/*
		 * Determine the position of partition key in a tuple slot and add it
		 * into fdw_scan_tlist.
		 */
		if (fdw_private->partkey_expr || fdw_private->partkey_conds)
		{
			if (fdw_scan_tlist)
			{
				/*
				 * If fdw_scan_tlist is specified, a tuple descriptor will be
				 * created based on fdw_scan_tlist. So we append a partition
				 * key to fdw_scan_tlist at the tail and memorize the
				 * position.
				 */
				fdw_private->partkey_idx = list_length(fdw_scan_tlist);
				fdw_scan_tlist = add_to_flat_tlist(fdw_scan_tlist, list_make1(fdw_private->partkey_expr));
			}

			/*
			 * If fdw_scan_tlist is not specified, a tuple descriptor will be
			 * created based on the target list which is specified at
			 * GetForeignPaths.
			 */
			else if (child_plan_info->path->pathtarget == child_plan_info->baserel->reltarget)
			{
				/*
				 * If child FDW does not specify a pathtarget when creating a
				 * foreign path, a relation target is used automatically
				 * (Refer create_foreignscan_path()). In this case, a tuple
				 * descriptor of child slot is created based on relation. So
				 * the position of partition key is the last column.
				 */
				Relation	rd = RelationIdGetRelation(foreigntableid);

				fdw_private->partkey_idx = rd->rd_att->natts - 1;
				RelationClose(rd);
			}
			else
			{
				/*
				 * If child FDW specifies a pathtarget when creating a foreign
				 * path, we appended an expr of partition key into the
				 * pathtarget at the tail in GetForeignPaths(). So the
				 * position of partition key is the last column.
				 */
				fdw_private->partkey_idx = list_length(child_plan_info->path->pathtarget->exprs);
			}
		}
		else
		{
			/*
			 * Check if there is a whole-row reference. A target list might
			 * have an expression of which type is T_ConvertRowtypeExpr such
			 * as "SELECT table FROM table". In this case, a partition key is
			 * used.
			 */
			Bitmapset  *attrs_used = NULL;
			bool		have_wholerow;

			pull_varattnos((Node *) baserel->reltarget->exprs, baserel->relid, &attrs_used);
			have_wholerow = bms_is_member(0 - FirstLowInvalidHeapAttributeNumber,
										  attrs_used);
			if (have_wholerow)
				fdw_private->partkey_idx = fdw_private->partkey_attno - 1;
			else
				fdw_private->partkey_idx = -1;
		}

		/*
		 * We collect local conditions each fdw did not push down to make
		 * postgresql core execute that filter
		 */
		foreach(lc, child_plan_info->plan->qual)
		{
			Expr	   *child_expr = (Expr *) lfirst(lc);
			Expr	   *expr = (Expr *) copyObject(child_expr);

			expr = (Expr *) mapVarAttnos((Node *) expr, child_plan_info->attrno_to_parent);
			local_exprs = list_append_unique_ptr(local_exprs, expr);
		}
	}
	else
	{
		/* Aggregate push down */
		scan_relid = 0;
		if (fdw_private->partkey_expr)
		{
			/*
			 * Append a partition key to the tail. Tuple descriptor will be
			 * created based on fdw_scan_tlist. In order to insert the
			 * partition key value into the correct posision, we memorize
			 * partkey_idx. Note: partkey_idx points the end of
			 * fdw_scan_tlist. So this variable is unnecessary beucase it can
			 * be calculated by attribute count(natts)?
			 */
			fdw_private->partkey_idx = list_length(fdw_scan_tlist);
			fdw_scan_tlist = add_to_flat_tlist(fdw_scan_tlist, list_make1(fdw_private->partkey_expr));
		}
		else
			fdw_private->partkey_idx = -1;

		local_exprs = scan_clauses;
	}

	local_exprs = list_concat(local_exprs, fdw_private->partkey_conds);

	/*
	 * Serialize fdw_private's members to a list. The list to be placed in the
	 * ForeignScan plan node, where they will be available to be deserialized
	 * at execution time The list must be represented in a form that
	 * copyObject knows how to copy.
	 */
	lfdw_private = serializeSpdFdwPrivate(fdw_private);

	return make_foreignscan(tlist,
							local_exprs,
							scan_relid,
							NIL,
							lfdw_private,
							fdw_scan_tlist,
							NIL,
							outer_plan);
}

/*
 * createChildEstate
 *	  Create a child execution state.
 *
 * @param[in] estate - parent's execution state
 * @param[in] eflags - parent's flags defined in executor.h.
 * @param[in,out] childscaninfo - child scan info
 * @return EState* - created execution state
 */
static EState *
createChildEstate(EState *estate, int eflags, ChildScanInfo * childscaninfo
#if PG_VERSION_NUM >= 160000
				  ,List *permInfos
#endif
)
{
	EState	   *child_estate;
	Query	   *query = childscaninfo->parse;

	child_estate = CreateExecutorState();
	child_estate->es_top_eflags = eflags;

	/* Init external params */
	child_estate->es_param_list_info =
		copyParamList(estate->es_param_list_info);

	/*
	 * Init range table, in which we use range table array for exec_rt_fetch()
	 * because it is faster than rt_fetch().
	 */
#if PG_VERSION_NUM >= 160000
	ExecInitRangeTable(child_estate, query->rtable, permInfos);
#else
	ExecInitRangeTable(child_estate, query->rtable);
#endif

	child_estate->es_plannedstmt = (PlannedStmt *) copyObject(estate->es_plannedstmt);
	child_estate->es_plannedstmt->planTree = (Plan *) copyObject(childscaninfo->plan);

	child_estate->es_query_cxt = estate->es_query_cxt;

	return child_estate;
}

/*
 * createChildTupleTableSlot
 *	  Create a tuple table slot of child table.
 *
 * @param[in,out] child_ss - table slot is created here
 */
static void
createChildTupleTableSlot(ScanState *child_ss)
{
	TupleDesc	child_tupledesc;
	ForeignScan *child_plan = (ForeignScan *) child_ss->ps.plan;

	/*
	 * Determine the scan tuple type.  If the FDW provided a targetlist
	 * describing the scan tuples, use that; else use base relation's rowtype.
	 * Refer nodeForeignscan.c.
	 */
	if (child_plan->fdw_scan_tlist != NIL || child_plan->scan.scanrelid == 0)
	{
		child_tupledesc = ExecTypeFromTL(child_plan->fdw_scan_tlist);
		ExecInitScanTupleSlot(child_ss->ps.state, child_ss, child_tupledesc,
							  &TTSOpsHeapTuple);
	}
	else
	{
		Relation	currentRelation = child_ss->ss_currentRelation;

		/* don't trust FDWs to return tuples fulfilling NOT NULL constraints */
		child_tupledesc = CreateTupleDescCopy(RelationGetDescr(currentRelation));
		ExecInitScanTupleSlot(child_ss->ps.state, child_ss, child_tupledesc,
							  &TTSOpsHeapTuple);
	}
}

/*
 * createChildFsstate
 *	  Create a foreign scan state of child table.
 *
 * @param[in] ss - parent's scan state
 * @param[in] eflags - parent's eflag passed to spdBeginForeignScan
 * @param[in] childscaninfo - child scan info
 * @return EState* - created foreign scan state
 */
static ForeignScanState *
createChildFsstate(ScanState *ss, int eflags, ChildScanInfo * childscaninfo
#if PG_VERSION_NUM >= 160000
				   ,List *permInfos
#endif
)
{
	ForeignScanState *child_fsstate;
	EState	   *estate = ss->ps.state;
	EState	   *child_estate;
	Relation	rd;

	child_fsstate = makeNode(ForeignScanState);
	memcpy(&child_fsstate->ss, ss, sizeof(ScanState));

	child_fsstate->ss.ps.plan = childscaninfo->plan;

	/* Create Estate */
#if PG_VERSION_NUM >= 160000
	child_estate = createChildEstate(estate, eflags, childscaninfo, permInfos);
#else
	child_estate = createChildEstate(estate, eflags, childscaninfo);
#endif
	ExecAssignExprContext(child_estate, &child_fsstate->ss.ps);
	child_fsstate->ss.ps.state = child_estate;

	rd = RelationIdGetRelation(childscaninfo->table_oid);
	child_fsstate->ss.ss_currentRelation = rd;

	/*
	 * For prepared statement, dummy root is not created at the next
	 * execution, so we need to lock relation again. We don't need unlock
	 * relation because lock will be released at transaction end.
	 * https://www.postgresql.org/docs/12/sql-lock.html
	 */
	if (!CheckRelationLockedByMe(rd, AccessShareLock, true))
		LockRelationOid(childscaninfo->table_oid, AccessShareLock);

	/* Initialize a tuple slot. */
	createChildTupleTableSlot(&child_fsstate->ss);

	return child_fsstate;
}

/*
 * spdBeginForeignScan
 *	  Setup ForeignScanState for child table including tuple descriptor.
 *	  Then call BeginForeignScan of child table.
 *
 * @param[in,out] node - parent's foreign scan state
 * @param[in] eflags - parent's flags defined in executor.h.
 */
static void
spdBeginForeignScan(ForeignScanState *node, int eflags)
{
	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
	SpdFdwScanState *fdw_state;
	ChildScanInfo *child_scan_info;
	EState	   *estate = node->ss.ps.state;
	Query	   *query;
	RangeTblEntry *rte;
	int			k;

	elog(DEBUG1, "BeginForeignScan");

	/* Deserialize fdw_private list to SpdFdwPrivate object */
	fdw_state = deserializeSpdFdwPrivate(fsplan->fdw_private);

	child_scan_info = &fdw_state->child_scan_info;

	/* This should be a new RTE list. coming from dummy rtable */
	query = child_scan_info->parse;

	rte = lfirst_node(RangeTblEntry, list_head(query->rtable));

	/* Create child's foreign scan state. */
#if PG_VERSION_NUM >= 160000
	child_scan_info->fsstate = createChildFsstate(&node->ss, eflags, child_scan_info, query->rteperminfos);
#else
	child_scan_info->fsstate = createChildFsstate(&node->ss, eflags, child_scan_info);
#endif

	if (query->rtable->length != estate->es_range_table->length)
		for (k = query->rtable->length; k < estate->es_range_table->length; k++)
			query->rtable = lappend(query->rtable, rte);

	/* Call BeginForeignScan of child table. */
	child_scan_info->fdw_routine->BeginForeignScan(child_scan_info->fsstate, eflags);

	fdw_state->is_first = true;

	node->fdw_state = (void *) fdw_state;

	return;
}

/*
 * getPartitionKeyName
 *	  Get a column value of partition key.
 *
 * @param[in] rel - parent table's relation
 * @return char* - partition key string
 */
static char *
getPartitionKeyName(Relation rel)
{
	Oid			inhrelid = rel->rd_id;
	HeapTuple	tuple;
	Datum		datum;
	bool		isnull;
	PartitionBoundSpec *bspec;
	Node	   *node;
	Const	   *con;
	char	   *keyname;

	/* Get PartitionBoundSpec of the relation. */
	tuple = SearchSysCache1(RELOID, inhrelid);
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for foreign table %u", inhrelid);

	datum = SysCacheGetAttr(RELOID, tuple,
							Anum_pg_class_relpartbound,
							&isnull);
	if (isnull)
		elog(ERROR, "null relpartbound for relation %u", inhrelid);

	bspec = (PartitionBoundSpec *)
		stringToNode(TextDatumGetCString(datum));
	if (!IsA(bspec, PartitionBoundSpec))
		elog(ERROR, "expected PartitionBoundSpec");

	/* Get an expression of partiotion key. */
	Assert(list_length(bspec->listdatums) == 1);
	node = (Node *) linitial(bspec->listdatums);
	Assert(nodeTag(node) == T_Const);
	con = (Const *) node;

	/* Convert Const value to char*. */
	keyname = TextDatumGetCString(con->constvalue);

	ReleaseSysCache(tuple);

	return keyname;
}

/*
 * addPartkeyToVirtualTuple
 *	  Create a tuple by copying a child tuple. A partition key is also appended if necessary.
 *    This function is called if a child slot is virtual tuple.
 *
 * @param[in,out] parent_slot - parent's tuple slot
 * @param[in] child_slot - child's tuple slot
 * @param[in] partkey - partition key string
 * @param[in] partkey_idx - position of partition key
 */
static void
addPartkeyToVirtualTuple(TupleTableSlot *parent_slot, TupleTableSlot *child_slot,
						 char *partkey, int partkey_idx)
{
	Datum	   *values;
	bool	   *nulls;
	int			natts = parent_slot->tts_tupleDescriptor->natts;
	int			offset = 0;
	int			i;

	Assert(TTS_IS_VIRTUAL(child_slot));

	/* Initialize new tuple buffer */
	values = (Datum *) palloc0(sizeof(Datum) * natts);
	nulls = (bool *) palloc0(sizeof(bool) * natts);

	for (i = 0; i < natts; i++)
	{
		if (i == partkey_idx)
		{
			values[i] = CStringGetTextDatum(partkey);
			nulls[i] = false;
			offset = -1;
		}
		else
		{
			values[i] = child_slot->tts_values[i + offset];
			nulls[i] = child_slot->tts_isnull[i + offset];
		}
	}

	parent_slot->tts_values = values;
	parent_slot->tts_isnull = nulls;
	parent_slot->tts_flags |= TTS_FLAG_EMPTY;
	ExecStoreVirtualTuple(parent_slot);
}

/*
 * addPartkeyToHeapTupleForUpperRel
 *	  Create a tuple by copying a child tuple. A partition key is also appended if necessary.
 *    This function is called if a child slot is heap tuple and upper relation.
 *
 * @param[in,out] parent_slot - parent's tuple slot
 * @param[in] child_slot - child's tuple slot
 * @param[in] attrno_to_child - information of attrno mapping from parent to child
 * @param[in] partkey - partition key string
 * @param[in] partkey_idx - position of partition key
 */
static void
addPartkeyToHeapTupleWithReplace(TupleTableSlot *parent_slot, TupleTableSlot *child_slot,
								 AttrNumber *attrno_to_child, char *partkey, int partkey_idx)
{
	Datum	   *values;
	bool	   *nulls;
	int			natts = parent_slot->tts_tupleDescriptor->natts;

	HeapTuple	newtuple;

	/* Initialize new tuple buffer */
	values = (Datum *) palloc0(sizeof(Datum) * natts);
	nulls = (bool *) palloc0(sizeof(bool) * natts);

	/* Extract tuple to values/isnulls. */
	heap_deform_tuple(child_slot->tts_ops->get_heap_tuple(child_slot), child_slot->tts_tupleDescriptor, values, nulls);

	if (attrno_to_child)
	{
		int			i;

		for (i = 0; i < natts; i++)
		{
			if (i + attrno_to_child[i] >= child_slot->tts_tupleDescriptor->natts)
			{
				nulls[i] = true;
				continue;
			}
			values[i] = values[i + attrno_to_child[i]];
			nulls[i] = nulls[i + attrno_to_child[i]];
		}

		/*
		 * If the last element is a partition column, it does not exist in
		 * child slot. So it is set to NULL. Value of a partition column will
		 * be set later.
		 */
		if (attrno_to_child[natts - 1] == 0)
			nulls[natts - 1] = true;
	}

	if (partkey_idx > 0)
	{
		int			i;

		/* Shift array elements in order to make space of partition key. */
		for (i = natts - 1; i > partkey_idx; i--)
		{
			values[i] = values[i - 1];
			nulls[i] = nulls[i - 1];
		}
		/* Insert a partition key to the array */
		values[partkey_idx] = CStringGetTextDatum(partkey);
		nulls[partkey_idx] = false;
	}

	/* Form new tuple with new values. */
	newtuple = heap_form_tuple(parent_slot->tts_tupleDescriptor, values, nulls);
	newtuple->t_self = newtuple->t_data->t_ctid = child_slot->tts_tid;
	ExecStoreHeapTuple(newtuple, parent_slot, false);

	pfree(values);
	pfree(nulls);
}

/*
 * addPartkeyToHeapTupleForBaseRel
 *	  Create a tuple by copying a child tuple. A partition key is also appended if necessary.
 *    This function is called if a child slot is heap tuple and base relation.
 *
 * @param[in,out] parent_slot - parent's tuple slot
 * @param[in] child_slot - child's tuple slot
 * @param[in] attrno_to_child - information of attrno mapping from parent to child
 * @param[in] partkey - partition key string
 * @param[in] partkey_idx - position of partition key
 */
static void
addPartkeyToHeapTupleByCopy(TupleTableSlot *parent_slot, TupleTableSlot *child_slot,
							AttrNumber *attrno_to_child, char *partkey, int partkey_idx)
{
	Datum	   *values;
	bool	   *nulls;
	bool	   *replaces;
	int			natts = parent_slot->tts_tupleDescriptor->natts;
	HeapTuple	newtuple;

	if (attrno_to_child != NULL)
	{
		addPartkeyToHeapTupleWithReplace(parent_slot, child_slot, attrno_to_child, partkey, partkey_idx);
		return;
	}

	/* Copy tuple from child to parent. */
	ExecStoreHeapTuple(child_slot->tts_ops->copy_heap_tuple(child_slot), parent_slot, false);

	/* Set a replacing elememt. */
	if (partkey_idx >= 0)
	{
		/* Initialize new tuple buffer */
		values = (Datum *) palloc0(sizeof(Datum) * natts);
		nulls = (bool *) palloc0(sizeof(bool) * natts);
		replaces = (bool *) palloc0(sizeof(bool) * natts);

		replaces[partkey_idx] = true;
		nulls[partkey_idx] = false;
		values[partkey_idx] = CStringGetTextDatum(partkey);

		/*
		 * Add a partition key value by replacing the last element in the
		 * slot.
		 */
		newtuple = heap_modify_tuple(parent_slot->tts_ops->get_heap_tuple(parent_slot),
									 parent_slot->tts_tupleDescriptor,
									 values, nulls, replaces);
		ExecStoreHeapTuple(newtuple, parent_slot, false);
	}
}

/*
 * createTuple
 *	  Create a tuple by copying a child tuple. A partition key is also appended if necessary.
 *	  If child is PGSpider, a partition key is concatinated that from child table.
 *
 * @param[in,out] parent_slot - parent's tuple slot
 * @param[in] child_slot - child's tuple slot
 * @param[in] attrno_to_child - information of attrno mapping from parent to child
 * @param[in] fdw_state - foreign scan state
 * @return TupleTableSlot* - created tuple
 */
static TupleTableSlot *
createTuple(TupleTableSlot *parent_slot, TupleTableSlot *child_slot,
			AttrNumber *attrno_to_child, SpdFdwScanState * fdw_state)
{
	Relation	rel;
	char	   *partkey;

	/*
	 * Length of parent should be greater than or equal to length of child
	 * slot If a partition key is not specified, length is same.
	 */
	Assert(parent_slot->tts_tupleDescriptor->natts >= child_slot->tts_tupleDescriptor->natts);

	/* Get a partition key string */
	rel = RelationIdGetRelation(fdw_state->table_oid);
	partkey = getPartitionKeyName(rel);
	RelationClose(rel);

	/*
	 * Insert a partition key column to slot. heap_modify_tuple will replace
	 * the existing column. To insert new column and its data, we also follow
	 * the similar steps like heap_modify_tuple. First, deform tuple to get
	 * data values, Second, modify data values (insert new columm). Then, form
	 * tuple with new data values. Finally, copy identification info (if any).
	 */
	if (fdw_state->is_upper)
	{
		int			partkey_idx = fdw_state->partkey_idx;

		if (TTS_IS_HEAPTUPLE(child_slot))
			addPartkeyToHeapTupleWithReplace(parent_slot, child_slot, NULL, partkey, partkey_idx);
		else
			addPartkeyToVirtualTuple(parent_slot, child_slot, partkey, partkey_idx);
	}
	else
	{
		int			partkey_idx = fdw_state->partkey_idx;

		/* Store tuple */
		if (TTS_IS_HEAPTUPLE(child_slot))
			addPartkeyToHeapTupleByCopy(parent_slot, child_slot, attrno_to_child,
										partkey, partkey_idx);
		else
			addPartkeyToVirtualTuple(parent_slot, child_slot, partkey, partkey_idx);
	}

	return parent_slot;
}

/*
 * spdIterateForeignScan
 *	  Call child's InterateForeignScan and get a slot. Then create parent's slot.
 *
 * @param[in] node - foreign scan state
 * @return TupleTableSlot* - created tuple
 */
static TupleTableSlot *
spdIterateForeignScan(ForeignScanState *node)
{
	SpdFdwScanState *fdw_state;
	ChildScanInfo *child_scan_info;
	TupleTableSlot *slot;
	TupleTableSlot *child_slot = NULL;

	elog(DEBUG1, "IterateForeignScan");

	fdw_state = (SpdFdwScanState *) node->fdw_state;
	if (fdw_state == NULL)
		elog(ERROR, "fdw_private is NULL");

	child_scan_info = &fdw_state->child_scan_info;

	if (fdw_state->is_first)
		child_scan_info->fsstate->ss.ps.ps_ExprContext->ecxt_param_exec_vals =
			node->ss.ps.ps_ExprContext->ecxt_param_exec_vals;

	child_slot = child_scan_info->fdw_routine->IterateForeignScan(child_scan_info->fsstate);
	if (!TupIsNull(child_slot))
		slot = createTuple(node->ss.ss_ScanTupleSlot, child_slot,
						   child_scan_info->attrno_to_child, fdw_state);
	else
		slot = ExecClearTuple(node->ss.ss_ScanTupleSlot);

	return slot;
}

/*
 * spdReScanForeignScan
 *	  Call child's RescanForeignScan and reset fdw state.
 *
 * @param[in] node - foreign scan state
 */
static void
spdReScanForeignScan(ForeignScanState *node)
{
	SpdFdwScanState *fdw_state;
	ChildScanInfo *child_scan_info;

	elog(DEBUG1, "ReScanForeignScan");

	fdw_state = (SpdFdwScanState *) node->fdw_state;
	if (fdw_state == NULL)
		elog(ERROR, "fdw_private is NULL");

	fdw_state->is_first = true;

	child_scan_info = &fdw_state->child_scan_info;
	/* Need to update chgParam to notify child node to change binding params */
	child_scan_info->fsstate->ss.ps.chgParam = bms_copy(node->ss.ps.chgParam);
	child_scan_info->fdw_routine->ReScanForeignScan(child_scan_info->fsstate);
}

/*
 * spdEndForeignScan
 *	  Call child's EndForeignScan and close a relation.
 *
 * @param[in] node - foreign scan state
 */
static void
spdEndForeignScan(ForeignScanState *node)
{
	SpdFdwScanState *fdw_state;
	ChildScanInfo *child_scan_info;

	elog(DEBUG1, "EndForeignScan");

	fdw_state = (SpdFdwScanState *) node->fdw_state;
	if (fdw_state == NULL)
		elog(ERROR, "fdw_private is NULL");

	child_scan_info = &fdw_state->child_scan_info;

	child_scan_info->fdw_routine->EndForeignScan(child_scan_info->fsstate);
	RelationClose(child_scan_info->fsstate->ss.ss_currentRelation);
}

/*
 * spdExplainForeignScan
 *	  Produce an extra output for EXPLAIN of a ForeignScan on a foreign table.
 *	  Reduce an indent for an output message temporary and call child's
 *	  ExplainForeignScan().
 *
 * @param[in] node - foreign scan state
 * @param[in] es - explain state
 */
static void
spdExplainForeignScan(ForeignScanState *node,
					  ExplainState *es)
{
	ChildScanInfo *child_scan_info;
	SpdFdwScanState *fdw_state;
	ExplainState *child_es;

	elog(DEBUG1, "ExplainForeignScan");

	fdw_state = (SpdFdwScanState *) node->fdw_state;
	if (fdw_state == NULL)
		elog(ERROR, "fdw_private is NULL");

	child_scan_info = &fdw_state->child_scan_info;
	if (child_scan_info->fdw_routine->ExplainForeignScan == NULL)
		return;

	es->indent++;

	/*
	 * Create child ExplainState before calling child's ExplainForeignScan().
	 */

	child_es = NewExplainState();

	memcpy(child_es, es, sizeof(ExplainState));
	child_es->rtable = child_scan_info->parse->rtable;

	/* Call child FDW's ExplainForeignScan(). */
	child_scan_info->fdw_routine->ExplainForeignScan(child_scan_info->fsstate, child_es);

	pfree(child_es);
	es->indent--;
}

/*
 * createChildOutrel
 *	  Create outer relation option info for child table.
 *
 * @param[in] relids - relation identifiers
 * @return RelOptInfo* - Created relation option info
 */
static RelOptInfo *
createChildOutrel(Relids relids)
{
	RelOptInfo *child_output_rel = makeNode(RelOptInfo);

	child_output_rel->reloptkind = RELOPT_UPPER_REL;
	child_output_rel->reltarget = create_empty_pathtarget();

	child_output_rel->relids = bms_copy(relids);

	return child_output_rel;
}

/*
 * createChildGroupClause
 *	  Create child's grouping clause.
 *	  If a grouping clause has a partition key without an expression, it can be pushed down
 *    and the child's grouping clause is created by removing the partition key.
 *	  But if a grouping clause has a partition key with an expression, it cannot be pushed down.
 *	  This function returns NULL and has_partkey is set to 1.
 *
 * @param[in] root - parent's planner info
 * @param[in] attrno_to_child - information of attrno mapping from parent to child
 * @param[in] partkey_attno - column number of partition key
 * @param[in] is_pgspider_fdw - true if child is PGSpider
 * @param[out] has_partkey - true if parent grouping clause includes a partition key with expression
 * @return List* - created grouping clause
 */
static List *
createChildGroupClause(PlannerInfo *root, AttrNumber *attrno_to_child,
					   AttrNumber partkey_attno, bool *has_partkey)
{
	List	   *target_list = root->parse->targetList;
	List	   *group_clause;
	ListCell   *lc;
	List	   *child_group_clause = NIL;

#if PG_VERSION_NUM >= 160000
	group_clause = root->processed_groupClause;
#else
	group_clause = root->parse->groupClause;
#endif

	*has_partkey = false;

	foreach(lc, group_clause)
	{
		SortGroupClause *sgc = (SortGroupClause *) lfirst(lc);
		TargetEntry *te = get_sortgroupclause_tle(sgc, target_list);
		Node	   *node;
		SortGroupClause *child_sgc;
		TargetEntry *child_te;

		if (te == NULL)
			return NIL;

		node = (Node *) te->expr;

		/*
		 * If grouping target is a single partition key (no calculation on
		 * partition key column, pgspider can pushdown the other expressions
		 * by removing it. Ex: GROUP BY col1, partkey, col2 % 2 Remote ->
		 * GROUP BY col1, col2 % 2
		 */
		if (IsA(node, Var) && var_is_partkey((Var *) node, partkey_attno))
		{
			continue;
		}
		else

			/*
			 * If the target includes a partition key with calculation, it
			 * cannot pushdown them.
			 */
		if (hasPartKeyExpr(node, partkey_attno))
		{
			*has_partkey = true;
			list_free(child_group_clause);
			return NIL;
		}

		child_sgc = (SortGroupClause *) copyObject(sgc);
		/* Update varattno for mapping from a parent table to a child table. */
		child_te = (TargetEntry *) copyObject(te);
		child_te->expr = (Expr *) mapVarAttnos((Node *) child_te->expr, attrno_to_child);

		child_group_clause = lappend(child_group_clause, child_sgc);
	}

	return child_group_clause;
}

/*
 * foreign_grouping_ok
 *	  Assess whether the aggregation, grouping and having operations can be pushed
 *	  down to the foreign server.
 *
 * @param[in] root - parents's planner info
 * @param[in] grouped_rel - grouped relation option info
 * @return bool - true if it can be pushed down.
 */
static bool
foreign_grouping_ok(PlannerInfo *root, RelOptInfo *grouped_rel, AttrNumber partkey_attno)
{
	Query	   *query = root->parse;
	ListCell   *lc;

	if (!query->groupClause && !query->groupingSets && !query->hasAggs &&
		!root->hasHavingQual)
		return false;

	/* Grouping Sets are not pushable */
	if (query->groupingSets)
		return false;

	/*
	 * If the expression contains unshippable function, it cannot be pushed
	 * down.
	 */
	foreach(lc, grouped_rel->reltarget->exprs)
	{
		Node	   *node = (Node *) lfirst(lc);
		Node	   *varnode;
		AggShippabilityContext ctx;

		ctx.shippable = true;
		ctx.hasAggref = false;
		ctx.partkey_attno = partkey_attno;

		if (IsA(node, TargetEntry))
			varnode = (Node *) (((TargetEntry *) node)->expr);
		else
			varnode = node;

		foreign_expr_walker_agg_shippability(varnode, &ctx);
		if (!ctx.shippable)
			return false;
	}

	return true;
}

/*
 * add_foreign_grouping_paths
 *	  Add paths of upper relation of parent. Costs are calculated based on child's path.
 *	  We specify path_pos as a fdw_private so that spdGetForeignPlan() can know which
 *	  path is selected as a best path.
 *
 * @param[in] root - parents's planner info
 * @param[in] grouped_rel - grouped relation option info
 * @param[in] child_path - child's upper path
 * @param[in] path_pos - position of the path in the path list
 */
static void
add_foreign_grouping_paths(PlannerInfo *root, RelOptInfo *grouped_rel,
						   Path *child_path, int path_pos)
{
	double		rows = child_path->rows;
	Cost		startup_cost = child_path->startup_cost;
	Cost		total_cost = child_path->total_cost;
	ForeignPath *grouppath;

	/* Create and add foreign path to the grouping relation. */
	grouppath = create_foreign_upper_path(root,
										  grouped_rel,
										  grouped_rel->reltarget,
										  rows,
										  startup_cost,
										  total_cost,
										  NIL,	/* no pathkeys */
										  NULL, /* no fdw_outerpath */
#if PG_VERSION_NUM >= 170000
										  NIL, /* no fdw_restrictinfo list */
#endif
										  list_make1_int(path_pos));	/* fdw_private */

	/* Add generated path into grouped_rel by add_path(). */
	add_path(grouped_rel, (Path *) grouppath);
}

/*
 * addGroupingTargetFromRelTarget
 *	  Add grouping target if targets in reltarget don't exist.
 *	  For example, SQL is "SELECT sum(c2) * (c2/2) FROM ft1 GROUP BY c2/2".
 *	  In GetForeignUpperPaths, root->parse->groupClause has 2 targets: c2/2, sum(c2).
 *	  But output_rel->reltarget has 3 targets: c2/2, sum(c2) and c2. c2 is added by
 *	  pull_var_clause() called by make_partial_grouping_target() in planner.c.
 *	  So we add target c2 into the grouping clause and update sortgrouprefs in reltarget.
 * @param[in,out] child_group_clause - grouping clause
 * @param[in,out] child_reltarget - grouped relation targets
 * @return List* - new grouping clause
 */
static List *
addGroupingTargetFromRelTarget(List *child_group_clause, PathTarget *child_reltarget)
{
	int			i;

	for (i = 0; i < list_length(child_reltarget->exprs); i++)
	{
		ListCell   *lc;
		bool		found = false;
		Expr	   *expr;
		TargetEntry *tle;
		SortGroupClause *grpcl;
		Oid			sortop;
		Oid			eqop;
		bool		hashable;
		Oid			restype;

		/* Check if the target exists or not. */
		foreach(lc, child_group_clause)
		{
			SortGroupClause *sgc = (SortGroupClause *) lfirst(lc);

			if (sgc->tleSortGroupRef == child_reltarget->sortgrouprefs[i])
			{
				found = true;
				break;
			}
		}
		if (found)
			continue;

		/*
		 * pull_var_clause() in make_partial_grouping_target() adds only Ver.
		 * So we can ignore non Var expr.
		 */
		expr = (Expr *) list_nth(child_reltarget->exprs, i);
		if (!IsA(expr, Var))
			continue;

		/*
		 * Add new target. We refered addTargetToGroupList() in
		 * parse_clause.c.
		 */
		tle = makeTargetEntry((Expr *) copyObject(expr),
							  (AttrNumber) i + 1,
							  NULL,
							  false);

		grpcl = makeNode(SortGroupClause);
		restype = exprType((Node *) tle->expr);
		Assert(restype != UNKNOWNOID);

		/* determine the eqop and optional sortop */
		get_sort_group_operators(restype,
								 false, true, false,
								 &sortop, &eqop, NULL,
								 &hashable);

		grpcl->tleSortGroupRef = list_length(child_group_clause) + 1;
		grpcl->eqop = eqop;
		grpcl->sortop = sortop;
		grpcl->nulls_first = false; /* OK with or without sortop */
		grpcl->hashable = hashable;

		child_group_clause = lappend(child_group_clause, grpcl);

		child_reltarget->sortgrouprefs[i] = list_length(child_group_clause);
	}

	return child_group_clause;
}

/*
 * modifyAggModeInGroupingtarget
 *	  Change aggregation's operating mode in grouping target.
 *	  FDWs does not push down aggregations if the mode is AGGSPLIT_INITIAL_SERIAL
 *	  or AGGSPLIT_FINAL_DESERIAL. So we change it to AGGSPLIT_SIMPLE forcibly.
 *
 * @param[in] group_clause - grouping clause
 * @param[in,out] grouping_target - grouping target to be updated
 * @param[in,out] aggsplit_history - hash table storing old aggsplit values
 */
static void
modifyAggModeInGroupingTarget(List *group_clause, PathTarget *grouping_target,
							  HTAB *aggsplit_history)
{
	ListCell   *lc;
	int			i = 0;

	foreach(lc, grouping_target->exprs)
	{
		Expr	   *expr = (Expr *) lfirst(lc);
		Index		sgref = get_pathtarget_sortgroupref(grouping_target, i++);
		AggSplitChangeWalkerContext context;

		/* Check whether this expression is part of GROUP BY clause */
		if (sgref && get_sortgroupref_clause_noerr(sgref, group_clause))
			continue;

		context.walk_mode = AGG_SPLIT_WALK_CHANGE;
		context.history = aggsplit_history;
		context.new_aggsplit = AGGSPLIT_SIMPLE;

		foreign_expr_walker_aggsplit_change((Node *) expr, &context);
	}
}

/*
 * spdGetForeignUpperPaths
 *	  Add paths for post-join operations like aggregation, grouping etc. if
 *	  corresponding operations are safe to push down.
 *	  Right now, we only support aggregate, grouping and having clause pushdown.
 *
 * @param[in] root - planner info
 * @param[in] stage - upper relation's statge
 * @param[in] input_rel - input relation option info
 * @param[out] output_rel - output relation option info
 * @param[in] extra - extra parameter
 */
static void
spdGetForeignUpperPaths(PlannerInfo *root, UpperRelationKind stage,
						RelOptInfo *input_rel, RelOptInfo *output_rel, void *extra)
{
	SpdFdwPlanState *fdw_private,
			   *in_fdw_private;
	bool		groupby_has_partkey;
	RelOptInfo *child_output_rel;
	RelOptInfo *child_input_rel;
	PlannerInfo *child_root;
	ChildPlanInfo *child_plan_info;
	List	   *child_group_clause;
	ListCell   *lc;
	int			path_pos = 0;

	elog(DEBUG1, "GetForeignUpperPaths");

	in_fdw_private = (SpdFdwPlanState *) input_rel->fdw_private;

	/*
	 * If input rel is not safe to pushdown, then simply return as we cannot
	 * perform any post-join operations on the foreign server.
	 */
	if (!in_fdw_private)
		return;

	/* Skip any duplicate calls. */
	if (output_rel->fdw_private)
		return;

	/* Ignore stages we don't support. */
	if (stage != UPPERREL_GROUP_AGG && stage != UPPERREL_PARTIAL_GROUP_AGG)
		return;

	/* Prepare SpdFdwPrivate for output RelOptInfo. */
	fdw_private = (SpdFdwPlanState *) palloc0(sizeof(SpdFdwPlanState));
	fdw_private->table_oid = in_fdw_private->table_oid;
	fdw_private->is_upper = true;
	fdw_private->child_plan_info = in_fdw_private->child_plan_info;
	fdw_private->partkey_attno = in_fdw_private->partkey_attno;

	child_plan_info = &fdw_private->child_plan_info;

	/* Save the input_rel as outerrel in fpinfo */
	fdw_private->outerrel = input_rel;

	output_rel->fdw_private = fdw_private;
	output_rel->relid = input_rel->relid;

	/*
	 * Check whether the aggregation, grouping and having operations can be
	 * pushed down to the foreign server.
	 */
	if (!foreign_grouping_ok(root, output_rel, fdw_private->partkey_attno))
		return;

	/*
	 * Create child's group clause. And detect whether a partition key with
	 * calculation is used or not.
	 */
	child_group_clause = createChildGroupClause(root, child_plan_info->attrno_to_child,
												fdw_private->partkey_attno, &groupby_has_partkey);

	/* Cannot pushdown GROUP BY using a partition key with calculation. */
	if (groupby_has_partkey)
		return;

	/* Create path for child node */
	child_input_rel = child_plan_info->baserel;
	child_output_rel = createChildOutrel(child_input_rel->relids);
	child_root = child_plan_info->root;

	child_root->parse->groupClause = child_group_clause;
#if PG_VERSION_NUM >= 160000
	child_root->processed_groupClause = child_group_clause;
#endif
	child_root->parse->hasAggs = root->parse->hasAggs;

	/* Make pathtarget */
	child_root->upper_targets[UPPERREL_GROUP_AGG] =
		copy_pathtarget(root->upper_targets[UPPERREL_GROUP_AGG]);
	child_root->upper_targets[UPPERREL_WINDOW] =
		copy_pathtarget(root->upper_targets[UPPERREL_WINDOW]);
	child_root->upper_targets[UPPERREL_FINAL] =
		copy_pathtarget(root->upper_targets[UPPERREL_FINAL]);
	child_root->upper_rels[UPPERREL_GROUP_AGG] =
		lappend(child_root->upper_rels[UPPERREL_GROUP_AGG], child_output_rel);

	/* Call the child FDW's GetForeignUpperPaths */
	if (child_plan_info->fdw_routine->GetForeignUpperPaths != NULL)
	{
		List	   *partkey_idxes = NIL;
		PathTarget *child_reltarget;
		List	   *exprs = NIL;
		GroupPathExtraData *ext = (GroupPathExtraData *) extra;
		bool		change_patype = false;

		child_reltarget = copy_pathtarget(output_rel->reltarget);
		exprs = copyObject(child_reltarget->exprs);
		exprs = removePartkeyFromTargets(exprs,
										 fdw_private->partkey_attno,
										 &partkey_idxes);

		/* Update varattno for mapping from a parent table to a child table. */
		list_free(child_reltarget->exprs);
		child_reltarget->exprs = mapVarAttnosInList(exprs,
													child_plan_info->attrno_to_child);

		/* Update sortgroupref if a partition key is remved from target list. */
		if (list_length(partkey_idxes) > 0)
		{
			Index	   *sgrefs = child_reltarget->sortgrouprefs;
			int			count = 0;	/* Removed count. */

			/*
			 * Shift sgrefs elements by removing thhose of a partition key.
			 * Positions of a partition key in partkey_idxes(partkey_idx) are
			 * sorted by ascending order. So we can start from this position
			 * for each loop. Example: There are 2 partition keys. Original =
			 * [col1][pkey][col2][pkey][col3] 1st foreach =
			 * [col1][col2][pkey][col3][N/A] 2nd foreach =
			 * [col1][col2][col3][N/A][N/A]
			 */
			foreach(lc, partkey_idxes)
			{
				int			partkey_idx = intVal(lfirst(lc));
				int			i = 0;

				for (i = partkey_idx - count; i < list_length(output_rel->reltarget->exprs) - 1 - count; i++)
				{
					sgrefs[i] = sgrefs[i + 1];
				}
				count++;
			}
		}

		/* Add grouping target if targets in reltarget don't exist. */
		child_group_clause = addGroupingTargetFromRelTarget(child_group_clause, child_reltarget);

		child_output_rel->reltarget = child_reltarget;

		/*
		 * Change Agg->aggsplit forcibly in order to pushdown aggregate
		 * functions.
		 */
		fdw_private->aggsplit_history = aggsplit_history_create();
		modifyAggModeInGroupingTarget(child_root->parse->groupClause, child_output_rel->reltarget,
									  fdw_private->aggsplit_history);

		/*
		 * Change kind of partitionwise aggregation (patype). FDWs does not
		 * push down aggregations if the patype is
		 * PARTITIONWISE_AGGREGATE_PARTIAL. There is an assert in
		 * add_foreign_grouping_paths of FDWs which will raise error when we
		 * try to push down aggregate function with
		 * PARTITIONWISE_AGGREGATE_PARTIAL. Therefore, we change it to
		 * PARTITIONWISE_AGGREGATE_FULL forcibly to avoid that error.
		 */
		if (ext->patype == PARTITIONWISE_AGGREGATE_PARTIAL)
		{
			ext->patype = PARTITIONWISE_AGGREGATE_FULL;
			change_patype = true;
		}

		/*
		 * Call GetForeignUpperPaths on UPPERREL_GROUP_AGG stage in order to
		 * enable to pushdown aggregates in child FDW. Originally, a parent
		 * GetForeignUpperPaths is called on UPPERREL_PARTIAL_GROUP_AGG stage.
		 */
		child_plan_info->fdw_routine->GetForeignUpperPaths(child_root,
														   UPPERREL_GROUP_AGG, child_input_rel,
														   child_output_rel, ext);

		/*
		 * After finishes GetForeignUpperPaths of child node, if we have
		 * changed patype forcibly before, we need to set it back to
		 * PARTITIONWISE_AGGREGATE_PARTIAL to avoid wrong code flow in
		 * PostgreSQL core.
		 */
		if (change_patype)
			ext->patype = PARTITIONWISE_AGGREGATE_PARTIAL;
	}

	/* Add paths based on child paths. */
	foreach(lc, child_output_rel->pathlist)
	{
		Path	   *child_path = (Path *) lfirst(lc);
		ListCell   *expr_lc;
		PathTarget *grouping_target = output_rel->reltarget;

		child_plan_info->grouped_root_local = child_root;
		child_plan_info->grouped_rel_local = child_output_rel;

		/*
		 * Memorize expr of partition key. This is refered at GetForeignPlan()
		 * for creating a scanning target list of pgspider.
		 */
		fdw_private->partkey_expr = NULL;
		foreach(expr_lc, grouping_target->exprs)
		{
			Node	   *node = (Node *) lfirst(expr_lc);

			if (IsA(node, Var) && var_is_partkey((Var *) node, fdw_private->partkey_attno))
			{
				fdw_private->partkey_expr = (Expr *) node;
				break;
			}
		}

		/* Add parent agg path and create mapping_tlist */
		add_foreign_grouping_paths(root, output_rel, child_path, path_pos);
		path_pos++;
	}
}

/*
 * spdIsForeignScanParallelSafe
 *	  Enable to scan in parallel.
 *
 * @param[in] root - not used
 * @param[in] rel - not used
 * @param[in] rte - not used
 * @return bool - always return true
 */
static bool
spdIsForeignScanParallelSafe(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	/*
	 * Plan nodes that reference a correlated SubPlan is always parallel
	 * restricted. Therefore, return false when there is lateral join.
	 */
	if (rel->lateral_relids)
		return false;

	return true;
}

/*
 * spdBuildRelationAliases
 *	  Construct the eref column name list for a relation RTE.
 *
 * @param[in] tupdesc: the physical column information
 * @param[in] alias: the user-supplied alias, or NULL if none
 * @param[in] eref: the eref Alias to store column names
 * 
 * Refer buildRelationAliases() in parse_relation.c 
 */

static void
spdBuildRelationAliases(TupleDesc tupdesc, Alias *alias, Alias *eref)
{
	int			maxattrs = tupdesc->natts;
	List	   *aliaslist;
	ListCell   *aliaslc;
	int			numaliases;
	int			varattno;
	int			numdropped = 0;

	Assert(eref->colnames == NIL);

	if (alias)
	{
		aliaslist = alias->colnames;
		aliaslc = list_head(aliaslist);
		numaliases = list_length(aliaslist);
		/* We'll rebuild the alias colname list */
		alias->colnames = NIL;
	}
	else
	{
		aliaslist = NIL;
		aliaslc = NULL;
		numaliases = 0;
	}

	for (varattno = 0; varattno < maxattrs; varattno++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, varattno);
#if PG_VERSION_NUM >= 150004
		String	   *attrname;
#else
		Value	   *attrname;
#endif

		if (attr->attisdropped)
		{
			/* Always insert an empty string for a dropped column */
			attrname = makeString(pstrdup(""));
			if (aliaslc)
				alias->colnames = lappend(alias->colnames, attrname);
			numdropped++;
		}
		else if (aliaslc)
		{
			/* Use the next user-supplied alias */
#if PG_VERSION_NUM >= 150004
			attrname = lfirst_node(String, aliaslc);
#else
			attrname = (Value *) lfirst(aliaslc);
#endif
			aliaslc = lnext(aliaslist, aliaslc);
			alias->colnames = lappend(alias->colnames, attrname);
		}
		else
		{
			attrname = makeString(pstrdup(NameStr(attr->attname)));
			/* we're done with the alias if any */
		}

		eref->colnames = lappend(eref->colnames, attrname);
	}

	/* Too many user-supplied aliases? */
	if (aliaslc)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
				 errmsg("table \"%s\" has %d columns available but %d columns specified",
						eref->aliasname, maxattrs - numdropped, numaliases)));
}
