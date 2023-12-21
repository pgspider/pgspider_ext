/*-------------------------------------------------------------------------
 *
 * pgspider_ext_deparse.c
 * contrib/pgspider_ext/pgspider_ext_deparse.c
 *
 * Portions Copyright (c) 2020, TOSHIBA CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup.h"
#include "access/htup_details.h"
#include "catalog/pg_proc.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pathnodes.h"
#include "utils/hsearch.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#include "pgspider_ext.h"

/*
 * The following aggregate functions are not pushed down to child nodes.
 * - avg, variance, stddev: these functions will give wrong result when pushing
 * down to child nodes because they requires to calculate on the whole data.
 * - array_agg, json_agg, jsonb_agg, json_object_agg, jsonb_object_agg: these functions
 * produce meaningfully different result values depending on the order of the input values.
 * It is hard to control the order of input value, so we do not push down them.
 * - string_agg, xmlagg: Logically, if there is no ORDER BY in aggregate function, these
 * functions can be pushed down to child nodes. For string_agg, it can also be pushed down
 * when the delimiter is not a constant. However, currently, to make it easier to implement,
 * we always do not push down them. This point can be improved in the future if necessary.
 */
static const char *unshippableFunctionNames[] =
{
	"avg",
	"variance",
	"stddev ",
	"array_agg",
	"json_agg",
	"jsonb_agg",
	"json_object_agg",
	"jsonb_object_agg",
	"string_agg",
	"xmlagg"
};

/* Hash key is Aggref->location. */
typedef int AggSplitHashKey;

typedef struct AggSplitHashEntry
{
	AggSplitHashKey key;		/* hash key (must be first) */
	AggSplit	aggsplit;
}			AggSplitHashEntry;

/*
 * getFunctionName
 *		Get a function name from given function oid.
 *
 * @param[in] funcid - function oid
 * @return const char* - function name
 */
static const char *
getFunctionName(Oid funcid)
{
	HeapTuple	proctup;
	Form_pg_proc procform;
	const char *proname;

	proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
	if (!HeapTupleIsValid(proctup))
		elog(ERROR, "cache lookup failed for function %u", funcid);
	procform = (Form_pg_proc) GETSTRUCT(proctup);

	proname = NameStr(procform->proname);

	ReleaseSysCache(proctup);

	return proname;
}

/*
 * isShippableFunc
 *		Jedge whether a function is safe to pushdown.
 *
 * @param[in] name - function name
 * @return const char* - true if is is safe to pushdown.
 */
static bool
isShippableFunc(const char *name)
{
	int			i;

	for (i = 0; i < sizeof(unshippableFunctionNames) / sizeof(*unshippableFunctionNames); i++)
	{
		if (strcmp(name, unshippableFunctionNames[i]) == 0)
			return false;
	}
	return true;
}

/*
 * foreign_expr_walker_agg_shippability
 *	  Check if an aggregate function can ne pushed down.
 *
 * @param[in] node - expression
 * @param[out] shippable - false will be set if it is not shippable.
 * @return bool - return true if it can stop the traverse because we detected it
 *				  Otherwise return false in order to continue traversing.
 */
bool
foreign_expr_walker_agg_shippability(Node *node, AggShippabilityContext * ctx)
{
	/* Need do nothing for empty subexpressions */
	if (node == NULL)
		return true;

	if (nodeTag(node) == T_Aggref)
	{
		Aggref	   *agg = (Aggref *) node;
		ListCell   *lc;
		const char *name;
		bool		filter_check;

		name = getFunctionName(agg->aggfnoid);
		if (!isShippableFunc(name))
		{
			ctx->shippable = false;
			return false;
		}

		/*
		 * When the aggsplit is AGGSPLIT_INITIAL_SERIAL and aggtranstype is
		 * INTERNALOID (For example, when the column is bigint and we want to
		 * get sum of that column), the PostgreSQL core will change the
		 * aggtype to bytea. When pushing down on pgspider_ext, we need to
		 * convert the data returned from fdw to bytea. However, currently,
		 * there is no way to convert it in pgspider_ext. Therefore, do not
		 * push down this case.
		 */
		if (agg->aggsplit == AGGSPLIT_INITIAL_SERIAL && agg->aggtranstype == INTERNALOID)
		{
			ctx->shippable = false;
			return false;
		}

		/* Set the flag if detected Aggref function */
		ctx->hasAggref = true;

		/* Recurse to input args. */
		foreach(lc, agg->args)
		{
			Node	   *n = (Node *) lfirst(lc);

			if (!expression_tree_walker(n, foreign_expr_walker_agg_shippability, ctx))
			{
				/* Reset the flag for next recursive check */
				ctx->hasAggref = false;
				return false;
			}
		}

		/* Check aggregate filter */
		filter_check = expression_tree_walker((Node *) agg->aggfilter, foreign_expr_walker_agg_shippability, ctx);

		/* Reset the flag for next recursive check */
		ctx->hasAggref = false;

		return filter_check;
	}
	else if (nodeTag(node) == T_Var)
	{
		Var		   *var = (Var *) node;

		/* Don't pushed down __spd_url if it is inside Aggref */
		if (ctx->hasAggref && var_is_partkey(var, ctx->partkey_attno))
		{
			ctx->shippable = false;
			return false;
		}
		else
			return true;
	}
	else
		return expression_tree_walker(node, foreign_expr_walker_agg_shippability, ctx);
}

/*
 * createVarAttrnoMapping
 *	  If columns in parent table and child table are dropped, attribute number of remianing
 *	  columns are updated. On the otherhand, attribute number of columns in child table
 *	  are not updated because a dropped column still exists with attisdropped flag. So we
 *	  need to map varattno of parent table to that of child by shifting. This function
 *	  calculate how many counts does each column need to be shifted. If there is no dropped
 *	  columns, this function returns NULL.
 *
 *	  For example, there are 5 columns: col1, col2, col3, col4 and col5 of which varrttno
 *	  are 1, 2, 3, 4 and 5. If col2 and col4 are dropped, a parent table has 3 columns:
 *	  col1, col3 and col5 of which varrttno become 1, 2, 3. But a child table has still
 *	  5 columns. col2 and col4 have attisdropped flag. So if we map columns in a parent
 *	  table to child table, each varattno for each column need to be updated as follows:
 *	  col1: 1 -> 1, col3: 2 -> 3, col5: 3 -> 5. This function creates an array of
 *	  attrno_to_child[0] = 0, attrno_to_child[1] = 1, attrno_to_child[2] = 2. Each element
 *	  corresponds to a parent column.
 *	  As same as mapping from parent to child, we also calclate information for mapping
 *	  from child to parent. This function creates an array of attrno_to_parent[0] = 0,
 *	  attrno_to_parent[2] = -1, attrno_to_parent[4] = -2. Each element corresponds to a
 *	  child column. attrno_to_parent[1] and attrno_to_parent[1] are not used because col2
 *	  and col4 are dropped.
 *
 * @param[in] tableid - child's table oid
 * @param[out] attrno_to_child - information of attrno mapping from parent to child
 * @param[out] attrno_to_parent - information of attrno mapping from child to parent
 * @return AttrNumber* - shift count for each column
 */
void
createVarAttrnoMapping(Oid parent_tableid, Oid child_tableid, AttrNumber partkey_attno,
					   AttrNumber **attrno_to_child, AttrNumber **attrno_to_parent)
{
	AttrNumber	i,
				j = 1;
	AttrNumber	i_col = 1;
	Relation	parent_rel = RelationIdGetRelation(parent_tableid);
	Relation	child_rel = RelationIdGetRelation(child_tableid);
	TupleDesc	parent_tupdesc = RelationGetDescr(parent_rel);
	TupleDesc	child_tupdesc = RelationGetDescr(child_rel);
	bool		nodropped = true;
	AttrNumber *tochild = NULL;
	AttrNumber *toparent = NULL;
	int			parent_dropped_col_num = 0;

	/* +1 means a space for partition key column. */
	tochild = (AttrNumber *) palloc0(sizeof(AttrNumber) * parent_tupdesc->natts + 1);
	toparent = (AttrNumber *) palloc0(sizeof(AttrNumber) * parent_tupdesc->natts + 1);

	for (i = 1; i < parent_tupdesc->natts; i++)
	{
		Form_pg_attribute parent_attr = TupleDescAttr(parent_tupdesc, i - 1);
		char	   *parent_colname = NameStr(parent_attr->attname);
		bool		found = false;

		if (parent_attr->attisdropped)
		{
			nodropped = false;
			parent_dropped_col_num++;
			continue;
		}

		for (; j <= child_tupdesc->natts && !found; j++)
		{
			Form_pg_attribute child_attr = TupleDescAttr(child_tupdesc, j - 1);
			char	   *child_colname;

			if (child_attr->attisdropped)
			{
				nodropped = false;
				continue;
			}

			child_colname = NameStr(child_attr->attname);

			if (strcmp(parent_colname, child_colname) != 0)
				elog(ERROR, "Column number %d \"%s\" of parent table and \"%s\" of child table are mismatched",
					 i_col, parent_colname, child_colname);

			tochild[i - 1] = j - i;
			toparent[j - 1] = i - j;
			found = true;
		}

		if (!found)
			elog(ERROR, "Column %s is not found in child table", parent_colname);

		i_col++;
	}

	if (partkey_attno != parent_tupdesc->natts - parent_dropped_col_num)
		elog(ERROR, "Partition key must be the last column");

	RelationClose(parent_rel);
	RelationClose(child_rel);

	/* We don't change attrno for the partition key column. */
	if (tochild != NULL)
		tochild[partkey_attno - 1] = 0;

	if (nodropped)
	{
		pfree(tochild);
		pfree(toparent);
		*attrno_to_child = NULL;
		*attrno_to_parent = NULL;
	}
	else
	{
		*attrno_to_child = tochild;
		*attrno_to_parent = toparent;
	}
}

/*
 * foreign_expr_walker_varattno_shifter
 *	  Update varattno in this node based on attrno mapping information.
 *
 * @param[in,out] node - expression
 * @param[in] attrno_shift - attrno mapping information
 * @return bool - always false in order to traverse all nodes recursively by
 *                expression_tree_walker().
 */
static bool
foreign_expr_walker_varattno_shifter(Node *node, AttrNumber *attrno_shift)
{
	/* Need do nothing for empty subexpressions */
	if (node == NULL)
		return false;

	if (IsA(node, Var))
	{
		Var		   *varnode = (Var *) node;

		/*
		 * The Locking Clause have attribute number of Var < 0. Ignore mapping
		 * them to avoid accessing invalid element of array.
		 */
		if (varnode->varattno - 1 >= 0)
			varnode->varattno += attrno_shift[varnode->varattno - 1];

		return false;
	}
	else
		return expression_tree_walker(node, foreign_expr_walker_varattno_shifter, attrno_shift);
}

/*
 * mapVarAttnos
 *	  Update varattno in Var node for mapping a parent table to a child table.
 *	  createVarAttrnoMapping()'s comment describes why it is necessary.
 */
Node *
mapVarAttnos(Node *node, AttrNumber *attrno_shift)
{
	if (attrno_shift == NULL)
		return node;

	foreign_expr_walker_varattno_shifter(node, attrno_shift);

	return node;
}

/*
 * mapVarAttnosInList
 *	  Update varattno in Var node for mapping a parent table to a child table.
 *	  createVarAttrnoMapping()'s comment describes why it is necessary.
 */
List *
mapVarAttnosInList(List *exprs, AttrNumber *attrno_shift)
{

	ListCell   *lc;

	if (attrno_shift == NULL)
		return exprs;

	foreach(lc, exprs)
	{
		Node	   *node = (Node *) lfirst(lc);

		foreign_expr_walker_varattno_shifter(node, attrno_shift);
	}

	return exprs;
}

/*
 * removePartkeyFromTargets
 *	  Remove a partition key from a target list 'exprs' and detect their positions in the list.
 *	  These positions are stored 'partkey_idxes' variable. It will be sorted.
 *
 * @param[in,out] exprs - target list
 * @param[in] partkey_attno - column number of partition key
 * @param[out] partkey_idxes - index of partition key if found
 * @return List* - modified target list
 */
List *
removePartkeyFromTargets(List *exprs, AttrNumber partkey_attno,
						 List **partkey_idxes)
{
	ListCell   *lc;
	int			i = 0;

	*partkey_idxes = NIL;
	foreach(lc, exprs)
	{
		Node	   *node = (Node *) lfirst(lc);
		Node	   *varnode;

		if (IsA(node, TargetEntry))
			varnode = (Node *) (((TargetEntry *) node)->expr);
		else
			varnode = node;

		if (IsA(varnode, Var))
		{
			Var		   *var = (Var *) varnode;

			/* check whole row reference */
			if (var->varattno == 0)
				continue;

			if (var->varattno == partkey_attno)
			{
				*partkey_idxes = lappend(*partkey_idxes, makeInteger(i));
				exprs = foreach_delete_current(exprs, lc);
				if (list_length(exprs) == 0)
					break;
			}
		}
		i++;
	}

	return exprs;
}

/*
 * var_is_partkey
 *	  Check whether Var is partition key or not.
 *
 * @param[in] var - Var node
 * @param[in] partkey_attno column number of partition key.
 * @return bool - true if it is a partition key.
 */
bool
var_is_partkey(Var *var, AttrNumber partkey_attno)
{
	if (var->varattno == partkey_attno)
		return true;
	else
		return false;
}

/*
 * check_partkey_walker
 *	  Check whether a partition key is used in the expression.
 *	  This function is used for being given to expression_tree_walker's argument.
 *
 * @param[in] node - node expression
 * @param[in] partkey_attno column number of partition key.
 * @return bool - true if it has a partition key. Then expression_tree_walker
 *				  calling this function will stop traversing.
 */
static bool
check_partkey_walker(Node *node, AttrNumber *partkey_attno)
{
	/* Need do nothing for empty subexpressions */
	if (node == NULL)
		return false;

	if (IsA(node, Var))
		return var_is_partkey((Var *) node, *partkey_attno);
	else
		return expression_tree_walker(node, check_partkey_walker, (void *) partkey_attno);
}

/*
 * check_partkey_walker
 *	  Check whether a partition key is used in the expression.
 *
 * @param[in] node - node expression
 * @param[in] partkey_attno column number of partition key.
 * @return bool - true if it is a partition key.
 */
bool
hasPartKeyExpr(Node *node, AttrNumber partkey_attno)
{
	return check_partkey_walker(node, (void *) &partkey_attno);
}

/*
 * extract_var_walker
 *	  Get Var expressions exceping a partition key in the expression
 *
 * @param[in] node - node expression
 * @param[in] context->partkey_attno column number of partition key.
 * @param[out] context->expers concatenated Var expressions of non partition key.
 * @return bool - always false in order to traverse all nodes recursively by
 *                expression_tree_walker().
 */
bool
extract_var_walker(Node *node, PartkeyWalkerContext * context)
{
	/* Need do nothing for empty subexpressions */
	if (node == NULL)
		return false;

	if (IsA(node, Var))
	{
		if (!var_is_partkey((Var *) node, context->partkey_attno))
			context->exprs = lappend(context->exprs, node);
		return false;
	}
	else
		return expression_tree_walker(node, extract_var_walker, (void *) context);
}

/*
 * exprlist_member
 *	  Finds the (first) member of the given tlist whose expression is
 *	  equal() to the given expression.  Result is NULL if no such member.
 *
 * @param[in] node - searching target
 * @param[in] exprs - list to be searched
 * @return Expr - found expression
 */
Expr *
exprlist_member(Expr *node, List *exprs)
{
	ListCell   *temp;

	foreach(temp, exprs)
	{
		Expr	   *expr = (Expr *) lfirst(temp);

		if (equal(node, expr))
			return expr;
	}
	return NULL;
}

/*
 * aggsplit_history_create
 *	  Create new hash table for storing addsplit values.
 *
 * @return HTAB - created hash tale
 */
HTAB *
aggsplit_history_create(void)
{
	HASHCTL		ctl;
	HTAB	   *agg_hash;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(AggSplitHashKey);
	ctl.entrysize = sizeof(AggSplitHashEntry);
#if (PG_VERSION_NUM >= 150000)
	agg_hash = hash_create("pgspider_ext aggrefs", 8, &ctl, HASH_ELEM | HASH_BLOBS);
#else
	agg_hash = hash_create("pgspider_ext aggrefs", 8, &ctl, HASH_ELEM);
#endif

	return agg_hash;
}

/*
 * aggsplit_history_add
 *	  Store aggsplit value into hash table. Hash entries are distinguished by
 *	  agg->location.
 *
 * @param[in,out] history - hash table
 * @param[in,out] agg - expr of aggregate function to be memorized
 */
static void
aggsplit_history_add(HTAB *history, Aggref *agg)
{
	bool		found;
	AggSplitHashEntry *entry;

	if (agg->location == -1)
		elog(ERROR, "Not supported: Aggref->location is unknown.");

	entry = (AggSplitHashEntry *) hash_search(history, &agg->location, HASH_ENTER, &found);

	if (found)
		elog(ERROR, "Not supported: Aggref hash alredy has entry ofwichi key is %d.", agg->location);

	entry->aggsplit = agg->aggsplit;
}

/*
 * aggsplit_history_get
 *	  Get the old aggsplit by searching the hash table
 *
 * @param[in,out] history - hash table
 * @param[in,out] agg - we seach the hash table based on agg->location
 * @return AggSplit - aggsplit value stored in the hash table.
 */
static AggSplit
aggsplit_history_get(HTAB *history, Aggref *agg)
{
	bool		found;
	AggSplitHashEntry *entry = (AggSplitHashEntry *) hash_search(history, &agg->location,
																 HASH_FIND, &found);

	if (!found)
		elog(ERROR, "Aggref is not found. key is %d.", agg->location);

	return entry->aggsplit;
}

/*
 * foreign_expr_walker_agg_mode_change
 *	  Change aggregation's operating mode.
 *	  When creating expr of Aggref for child fdw, aggsplit is changed from
 *	  AGGSPLIT_INITIAL_SERIAL or AGGSPLIT_FINAL_DESERIAL to AGGSPLIT_SIMPLE forcibly
 *	  in order to enable to pushdown aggregate function because postgres_fdw tries to
 *	  pushdown aggregate function of which aggsplit is only AGGSPLIT_SIMPLE.
 *	  On the other hand, when creating expr for pgspider_ext based on child FDW,
 *	  aggsplit is need to be reverted.
 *	  In order to revert the aggsplit to original value, we memorize it on the hash
 *	  table.
 *
 * @param[in,out] node - expression
 * @param[in,out] context - parameters. If context->walk_mode is AGG_SPLIT_WALK_CHANGE,
 *							old aggsplit value is memorized in context->history and new
 *							aggsplit is updated to context->new_aggsplit.
 *							If context->walk_mode is AGG_SPLIT_WALK_REVERT, the old
 *							aggsplit value is searhed from context->history and reset
 *							to it.
 * @return bool - always false in order to traverse all nodes recursively by
 *                expression_tree_walker().
 */
bool
foreign_expr_walker_aggsplit_change(Node *node, AggSplitChangeWalkerContext * context)
{
	/* Need do nothing for empty subexpressions */
	if (node == NULL)
		return false;

	if (nodeTag(node) == T_Aggref)
	{
		Aggref	   *agg = (Aggref *) node;
		ListCell   *lc;

		if (context->walk_mode == AGG_SPLIT_WALK_CHANGE)
		{
			aggsplit_history_add(context->history, agg);
			/* Overwrite aggsplit forcibly. */
			agg->aggsplit = context->new_aggsplit;
			elog(DEBUG1, "Aggregate flag is overwritten forcibly from %s to AGGSPLIT_SIMPLE",
				 (agg->aggsplit == AGGSPLIT_INITIAL_SERIAL) ? "AGGSPLIT_INITIAL_SERIAL" : "AGGSPLIT_FINAL_DESERIAL");
		}
		else
		{
			Assert(context->walk_mode == AGG_SPLIT_WALK_REVERT);
			agg->aggsplit = aggsplit_history_get(context->history, agg);
		}

		/* Recurse to input args. */
		foreach(lc, agg->args)
		{
			Node	   *n = (Node *) lfirst(lc);

			/* If TargetEntry, extract the expression from it */
			if (IsA(n, TargetEntry))
			{
				TargetEntry *tle = (TargetEntry *) n;

				n = (Node *) tle->expr;
			}

			expression_tree_walker(n, foreign_expr_walker_aggsplit_change, context);
		}

		/* Check aggregate filter */
		return expression_tree_walker((Node *) agg->aggfilter, foreign_expr_walker_aggsplit_change, context);
	}
	else
		return expression_tree_walker(node, foreign_expr_walker_aggsplit_change, context);
}

/*
 * Find an equivalence class member expression, all of whose Vars, come from
 * the indicated relation.
 * Copied from postgres_fdw.
 */
static Expr *
find_em_expr_for_rel(EquivalenceClass *ec, RelOptInfo *rel)
{
	ListCell   *lc_em;

	foreach(lc_em, ec->ec_members)
	{
		EquivalenceMember *em = lfirst(lc_em);

		if (bms_is_subset(em->em_relids, rel->relids) &&
			!bms_is_empty(em->em_relids))
		{
			/*
			 * If there is more than one equivalence member whose Vars are
			 * taken entirely from this relation, we'll be content to choose
			 * any one of those.
			 */
			return em->em_expr;
		}
	}

	/* We didn't find any suitable equivalence class expression */
	return NULL;
}

Expr *
getExprInPathKey(PathKey *pathkey, RelOptInfo *baserel)
{
	EquivalenceClass *pathkey_ec = pathkey->pk_eclass;

	return find_em_expr_for_rel(pathkey_ec, baserel);
}
