#!/bin/sh
DYNAMODB_ENDPOINT="http://localhost:8000"

# Below commands must be run in DynamoDB to create databases used in regression tests with `admin` user and `testadmin` password.
# aws configure
# -- AWS Access Key ID : admin
# -- AWS Secret Access Key : testadmin
# -- Default region name [None]: us-west-2


# Clean data
aws dynamodb delete-table --table-name T_1 --endpoint-url $DYNAMODB_ENDPOINT
aws dynamodb delete-table --table-name T_2 --endpoint-url $DYNAMODB_ENDPOINT
aws dynamodb delete-table --table-name T_3 --endpoint-url $DYNAMODB_ENDPOINT
aws dynamodb delete-table --table-name T_4 --endpoint-url $DYNAMODB_ENDPOINT
aws dynamodb delete-table --table-name loct_empty --endpoint-url $DYNAMODB_ENDPOINT
aws dynamodb delete-table --table-name local_tbl --endpoint-url $DYNAMODB_ENDPOINT
aws dynamodb delete-table --table-name loct3 --endpoint-url $DYNAMODB_ENDPOINT
aws dynamodb delete-table --table-name loc1 --endpoint-url $DYNAMODB_ENDPOINT
aws dynamodb delete-table --table-name gloc1 --endpoint-url $DYNAMODB_ENDPOINT
aws dynamodb delete-table --table-name loct --endpoint-url $DYNAMODB_ENDPOINT
aws dynamodb delete-table --table-name loct1 --endpoint-url $DYNAMODB_ENDPOINT
aws dynamodb delete-table --table-name loct13 --endpoint-url $DYNAMODB_ENDPOINT
aws dynamodb delete-table --table-name loct2 --endpoint-url $DYNAMODB_ENDPOINT
aws dynamodb delete-table --table-name loct4 --endpoint-url $DYNAMODB_ENDPOINT
aws dynamodb delete-table --table-name loct11 --endpoint-url $DYNAMODB_ENDPOINT
aws dynamodb delete-table --table-name loct22 --endpoint-url $DYNAMODB_ENDPOINT
aws dynamodb delete-table --table-name loc2 --endpoint-url $DYNAMODB_ENDPOINT
aws dynamodb delete-table --table-name loc3 --endpoint-url $DYNAMODB_ENDPOINT
aws dynamodb delete-table --table-name loct1_rescan --endpoint-url $DYNAMODB_ENDPOINT
aws dynamodb delete-table --table-name loct2_rescan --endpoint-url $DYNAMODB_ENDPOINT

# for ported_dynamodb_fdw.sql test
aws dynamodb --endpoint-url $DYNAMODB_ENDPOINT \
        create-table --table-name T_1 \
        --attribute-definitions AttributeName=C_1,AttributeType=N \
        --key-schema AttributeName=C_1,KeyType=HASH \
        --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1

# aws dynamodb --endpoint-url $DYNAMODB_ENDPOINT put-item --table-name connection_tbl --item $'{"artist":{"S":"No One You Know"}, "songtitle":{"S":"Call Me Today"}, "albumtitle":{"S":"Somewhat Famous"}}'

aws dynamodb --endpoint-url $DYNAMODB_ENDPOINT \
        create-table --table-name T_2 \
        --attribute-definitions AttributeName=c1,AttributeType=N \
        --key-schema AttributeName=c1,KeyType=HASH \
        --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1

aws dynamodb --endpoint-url $DYNAMODB_ENDPOINT \
        create-table --table-name T_3 \
        --attribute-definitions AttributeName=c1,AttributeType=N \
        --key-schema AttributeName=c1,KeyType=HASH \
        --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1

aws dynamodb --endpoint-url $DYNAMODB_ENDPOINT \
        create-table --table-name T_4 \
        --attribute-definitions AttributeName=c1,AttributeType=N \
        --key-schema AttributeName=c1,KeyType=HASH \
        --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1

aws dynamodb --endpoint-url $DYNAMODB_ENDPOINT \
        create-table --table-name loct_empty \
        --attribute-definitions AttributeName=c1,AttributeType=N \
        --key-schema AttributeName=c1,KeyType=HASH \
        --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1

aws dynamodb --endpoint-url $DYNAMODB_ENDPOINT \
        create-table --table-name local_tbl \
        --attribute-definitions AttributeName=c1,AttributeType=N \
        --key-schema AttributeName=c1,KeyType=HASH \
        --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1

aws dynamodb --endpoint-url $DYNAMODB_ENDPOINT \
        create-table --table-name loct3 \
        --attribute-definitions AttributeName=c1,AttributeType=N \
        --key-schema AttributeName=c1,KeyType=HASH \
        --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1

aws dynamodb --endpoint-url $DYNAMODB_ENDPOINT \
        create-table --table-name loc1 \
        --attribute-definitions AttributeName=f1,AttributeType=N \
        --key-schema AttributeName=f1,KeyType=HASH \
        --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1

aws dynamodb --endpoint-url $DYNAMODB_ENDPOINT \
        create-table --table-name gloc1 \
        --attribute-definitions AttributeName=id,AttributeType=N \
        --key-schema AttributeName=id,KeyType=HASH \
        --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1

aws dynamodb --endpoint-url $DYNAMODB_ENDPOINT \
        create-table --table-name loct \
        --attribute-definitions AttributeName=id,AttributeType=N \
        --key-schema AttributeName=id,KeyType=HASH \
        --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1

aws dynamodb --endpoint-url $DYNAMODB_ENDPOINT \
        create-table --table-name loct13 \
        --attribute-definitions AttributeName=id,AttributeType=N \
        --key-schema AttributeName=id,KeyType=HASH \
        --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1

aws dynamodb --endpoint-url $DYNAMODB_ENDPOINT \
        create-table --table-name loct1 \
        --attribute-definitions AttributeName=f1,AttributeType=N \
        --key-schema AttributeName=f1,KeyType=HASH \
        --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1

aws dynamodb --endpoint-url $DYNAMODB_ENDPOINT \
        create-table --table-name loct2 \
        --attribute-definitions AttributeName=f1,AttributeType=N \
        --key-schema AttributeName=f1,KeyType=HASH \
        --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1

aws dynamodb --endpoint-url $DYNAMODB_ENDPOINT \
        create-table --table-name loct4 \
        --attribute-definitions AttributeName=f1,AttributeType=N \
        --key-schema AttributeName=f1,KeyType=HASH \
        --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1

aws dynamodb --endpoint-url $DYNAMODB_ENDPOINT \
        create-table --table-name loct11 \
        --attribute-definitions AttributeName=a,AttributeType=N \
        --key-schema AttributeName=a,KeyType=HASH \
        --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1

aws dynamodb --endpoint-url $DYNAMODB_ENDPOINT \
        create-table --table-name loct22 \
        --attribute-definitions AttributeName=a,AttributeType=N \
        --key-schema AttributeName=a,KeyType=HASH \
        --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1

aws dynamodb --endpoint-url $DYNAMODB_ENDPOINT \
        create-table --table-name loc2 \
        --attribute-definitions AttributeName=a,AttributeType=N \
        --key-schema AttributeName=a,KeyType=HASH \
        --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1

aws dynamodb --endpoint-url $DYNAMODB_ENDPOINT \
        create-table --table-name loc3 \
        --attribute-definitions AttributeName=f1,AttributeType=N \
        --key-schema AttributeName=f1,KeyType=HASH \
        --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1

aws dynamodb --endpoint-url $DYNAMODB_ENDPOINT \
        create-table --table-name loct1_rescan \
        --attribute-definitions AttributeName=c1,AttributeType=N \
        --key-schema AttributeName=c1,KeyType=HASH \
        --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1

aws dynamodb --endpoint-url $DYNAMODB_ENDPOINT \
        create-table --table-name loct2_rescan \
        --attribute-definitions AttributeName=c1,AttributeType=N \
        --key-schema AttributeName=c1,KeyType=HASH \
        --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1
