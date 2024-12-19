#!/bin/sh
export MONGO_HOST="localhost"
export MONGO_PORT="27017"
export MONGO_USER_NAME="edb"
export MONGO_PWD="edb"

# Below commands must be run in MongoDB to create mongo_fdw_post_regress databases
# used in regression tests with edb user and edb password.

# use mongo_fdw_post_regress
# db.createUser({user:"edb",pwd:"edb",roles:[{role:"dbOwner", db:"mongo_fdw_post_regress"},{role:"readWrite", db:"mongo_fdw_post_regress"}]})

# for ported_mongodb_fdw.sql test
mongosh --host=$MONGO_HOST --port=$MONGO_PORT -u $MONGO_USER_NAME -p $MONGO_PWD --authenticationDatabase "mongo_fdw_post_regress" < sql/init_data/mongodb_fdw/mongodb_post.js > /dev/null
