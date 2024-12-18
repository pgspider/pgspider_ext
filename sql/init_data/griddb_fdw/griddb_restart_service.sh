#!/bin/bash
# script restart griddb server

griddb_container_name=griddb_svr
docker restart ${griddb_container_name}
