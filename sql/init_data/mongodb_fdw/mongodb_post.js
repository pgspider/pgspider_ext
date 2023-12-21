use mongo_fdw_post_regress

db.T0.drop()
db.T1.drop()
db.T2.drop()
db.T3.drop()
db.T4.drop()
db.loct_empty.drop()
db.loct3.drop()

// test WITH CHECK OPTION constraints
db.base_tbl.drop()
// test WCO for partitions
//database 'mongo_fdw_post_regress', collection
db.child_tbl.drop()

// test serial columns (ie, sequence-based defaults)
db.loc1.drop()

// test generated columns
db.gloc1.drop()

// test inheritance features
db.loct.drop()

// Check SELECT FOR UPDATE/SHARE with an inherited source table
db.loct1.drop()
db.loct2.drop()

db.loct4.drop()

// Test pushing down UPDATE/DELETE joins to the remote server
db.loct11.drop()
db.loct22.drop()

db.loc2.drop()

db.fprt1_p1.drop()
db.fprt1_p2.drop()

db.fprt2_p1.drop()
db.fprt2_p2.drop()

db.pagg_tab_p1.drop()
db.pagg_tab_p2.drop()
db.pagg_tab_p3.drop()

db.ft1.drop()

db.child_local.drop()
db.tab_batch_sharded_p1_remote.drop()

