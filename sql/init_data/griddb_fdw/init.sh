#!/bin/bash

function clean_docker_img()
{
  if [ "$(docker ps -aq -f name=^/${1}$)" ]; then
    if [ "$(docker ps -aq -f status=exited -f status=created -f name=^/${1}$)" ]; then
        docker rm ${1}
    else
        docker rm $(docker stop ${1})
    fi
  fi
}

# rm -rf /tmp/*.data
# find ../sql/ -name "*.data" -exec cp {} /tmp/ \;

# Start docker
griddb_image='griddb/griddb:5.5.0-centos7'
griddb_container_name=griddb_svr
clean_docker_img ${griddb_container_name}
docker run -d --name ${griddb_container_name} --network="host" -e NOTIFICATION_ADDRESS=239.0.0.1 -e NOTIFICATION_PORT=31999 ${griddb_image}

make clean && make
result="$?"
if [[ "$result" -eq 0 ]]; then
  ./griddb_init host=239.0.0.1 port=31999 cluster=dockerGridDB user=admin passwd=admin
fi

