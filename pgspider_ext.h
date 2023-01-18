/*-------------------------------------------------------------------------
 *
 * pgspider_ext.c
 * contrib/pgspider_ext/pgspider_ext.h
 *
 * Portions Copyright (c) 2020, TOSHIBA CORPORATION
 *
 *-------------------------------------------------------------------------
 */
#ifndef SPD_FDW_H
#define SPD_FDW_H

#include "nodes/pathnodes.h"
#include "nodes/pg_list.h"

#define CODE_VERSION   10100

/*
 * Options structure to store information.
 */
typedef struct SpdPpt
{
	char	   *child_name;
}			SpdPpt;

/*
 * Argument used for extract_var_walker.
 */
typedef struct PartkeyWalkerContext
{
	AttrNumber	partkey_attno;
	List	   *exprs;
}			PartkeyWalkerContext;

typedef enum
{
	AGG_SPLIT_WALK_CHANGE,
	AGG_SPLIT_WALK_REVERT
}			AggSplitWalkMode;

/*
 * Argument used for foreign_expr_walker_aggsplit_change.
 */
typedef struct AggSplitChangeWalkerContext
{
	AggSplitWalkMode walk_mode;
	HTAB	   *history;
	AggSplit	new_aggsplit;
}			AggSplitChangeWalkerContext;

typedef struct AggShippabilityContext
{
	bool			shippable;		/* this flag determine that the expression can be shipped or not */
	bool			hasAggref;		/* this flag marks that we are checking the Aggref. It will be used
									 * to detect if partition key is inside Aggref function */
	AttrNumber		partkey_attno;	/* column number of partition key */
}			AggShippabilityContext;

/* In pgspider_ext_option.c */
extern SpdPpt * spd_get_options(Oid foreignoid);

/* In pgspider_ext_deparse.c */
extern HTAB *aggsplit_history_create(void);
extern bool foreign_expr_walker_aggsplit_change(Node *node, AggSplitChangeWalkerContext * context);
extern bool foreign_expr_walker_agg_shippability(Node *node, AggShippabilityContext *ctx);
extern void createVarAttrnoMapping(Oid parent_tableid, Oid child_tableid,
								   AttrNumber partkey_attno,
								   AttrNumber **attrno_to_child,
								   AttrNumber **attrno_to_parent);
extern Node *mapVarAttnos(Node *node, AttrNumber *attrno_shift);
extern List *mapVarAttnosInList(List *exprs, AttrNumber *attrno_shift);
extern List *removePartkeyFromTargets(List *exprs, AttrNumber partkey_attno,
									  List **partkey_idxes);
extern bool var_is_partkey(Var *var, AttrNumber partkey_attno);
extern bool hasPartKeyExpr(Node *node, AttrNumber partkey_attno);
extern bool extract_var_walker(Node *node, PartkeyWalkerContext * context);
extern Expr *exprlist_member(Expr *node, List *exprs);
extern Expr *getExprInPathKey(PathKey *pathkey, RelOptInfo *baserel);
#endif							/* SPD_FDW_H */
