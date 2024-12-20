# Initializer for GridDB Foreign Data Wrapper

This tool features:
>1. Create containers on GridDB used for "make check".
>2. If a container already exists, it is re-created.
>3. Connection parameters (like host, port, clusterName, username, password) are embedded (=fixed value).

## 1. Installation
This tool requires GridDB's C client library. This library can be downloaded from the [GridDB][1] website on github[1].

### 1.1. Preapre GridDB's C client
    Download GridDB's C client and unpack it into pgspider_ext/sql/init_data/griddb_fdw directory as griddb.
    Build GridDB's C client  
    -> gridstore.h should be in pgspider_ext/sql/init_data/griddb_fdw/griddb/client/c/include.
    -> libgridstore.so should be in griddb/bin.

### 1.2. Build the tool.
Change into the pgspider_ext/sql/init_data/griddb_fdw directory.<br />
<pre>
$ make
</pre>
It creates `griddb_init` executable file.


## 2. Usage
We must use gcc 4.8.5 or 4.9.2 for building the source code of GridDB Server (https://github.com/griddb/griddb/issues/408).  
Default gcc 8.5.0 of Rocky Linux 8 cannot build the source code of GridDB Server.  
So we use GridDB Server docker container for testing.

Default informations of GridDB Server on docker container:<br />
>notification_address : 239.0.0.1<br />
>notification_port : 31999<br />
>clusterName : dockerGridDB<br />
>GridDB username : admin<br />
>GridDB password : admin<br />

### 2.1. Run automatically using init.sh script

Run the script:<br />
```
./init.sh
```
### 2.2. Run manually

Start GridDB Server on docker container
```
docker run -d --name griddb_svr --network="host" -e NOTIFICATION_ADDRESS=239.0.0.1 -e NOTIFICATION_PORT=31999 griddb/griddb:5.5.0-centos7
```

Change to griddb_fdw/make_check_initializer directory.<br />
Initialize the containers for GridDB FDW test:<br />
```
./griddb_init host=239.0.0.1 port=31999 cluster=dockerGridDB user=admin passwd=admin
```
It should display message: "Initialize all containers sucessfully."
