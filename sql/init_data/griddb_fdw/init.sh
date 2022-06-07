#!/bin/bash

# User need to specified GRIDDB_HOME as environment variable before executing test
if [[ ! -d "${GRIDDB_HOME}" ]]; then
  echo "GRIDDB_HOME environment variable not set"
  exit 1
fi

# rm -rf /tmp/*.data
# find ../sql/ -name "*.data" -exec cp {} /tmp/ \;

# Start GridDB server
export GS_HOME=${GRIDDB_HOME}
export GS_LOG=${GRIDDB_HOME}/log
export no_proxy=127.0.0.1
if pgrep -x "gsserver" > /dev/null
then
  ${GRIDDB_HOME}/bin/gs_leavecluster -w -f -u admin/testadmin
  ${GRIDDB_HOME}/bin/gs_stopnode -w -u admin/testadmin
fi
sleep 1
rm -rf ${GS_HOME}/data/* ${GS_LOG}/*
echo "Starting GridDB server..."
sed -i 's/\"clusterName\":.*/\"clusterName\":\"griddbfdwTestCluster\",/' ${GRIDDB_HOME}/conf/gs_cluster.json
${GRIDDB_HOME}/bin/gs_startnode -w -u admin/testadmin
${GRIDDB_HOME}/bin/gs_joincluster -w -c griddbfdwTestCluster -u admin/testadmin

make clean && make
result="$?"
if [[ "$result" -eq 0 ]]; then
	./griddb_init host=239.0.0.1 port=31999 cluster=griddbfdwTestCluster user=admin passwd=testadmin
fi

#Update restart script
# sed -i 's|GRIDDB_HOME=.*|GRIDDB_HOME='"$GRIDDB_HOME"'|' ../griddb_restart_service.sh
# chmod +x ../griddb_restart_service.sh
