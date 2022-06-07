#!/bin/sh

rm -rf /tmp/data_local || true
rm -rf /tmp/data_s3 || true

mkdir -p /tmp/data_local || true
mkdir -p /tmp/data_s3 || true

cp -a sql/init_data/parquet_s3_fdw/ported_postgres /tmp/data_local
cp -a sql/init_data/parquet_s3_fdw/ported_postgres /tmp/data_s3

# start server minio/s3, by docker
# comment out these following code if do not use docker
container_name='minio_server'

if [ ! "$(docker ps -q -f name=^/${container_name}$)" ]; then
    if [ "$(docker ps -aq -f status=exited -f status=created -f name=^/${container_name}$)" ]; then
        # cleanup
        docker rm ${container_name} 
    fi
    # run minio container
   sudo docker run -d --name ${container_name} -it -p 9000:9000 -e "MINIO_ACCESS_KEY=minioadmin" -e "MINIO_SECRET_KEY=minioadmin" -v /tmp/data_s3:/data minio/minio server /data
fi

sed -i 's/REGRESS =.*/REGRESS = ported_parquet_s3_fdw_local ported_parquet_s3_fdw_server /' Makefile
sed -i 's/temp-install:.*/temp-install: EXTRA_INSTALL=contrib\/postgres_fdw contrib\/pgspider_ext contrib\/parquet_s3_fdw /' Makefile
sed -i 's/checkprep:.*/checkprep: EXTRA_INSTALL+=contrib\/postgres_fdw contrib\/pgspider_ext contrib\/parquet_s3_fdw /' Makefile

make clean
make
make check | tee make_check.out
