usage="Usage: sh Create-AliSpark-2.x-APP.sh <app_name> <target_path>"

if [ $# -ne 2 ]; then
    echo $usage
    exit 1
fi

cupidRoot=$(cd `dirname $0`; cd ..; pwd)
appName=$1
targetPath=$2

if [ ! -d "$targetPath" ]; then
    echo "$targetPath is not exist, plz create before run."
    exit 1
fi

# will try to install archetypes in local mvn repo
pushd $cupidRoot/archetypes/spark-2.x/
mvn clean install -Ppublic
popd

# create the template maven app into targetPath
pushd $targetPath
mvn archetype:generate -DarchetypeGroupId=com.aliyun.odps \
                       -DarchetypeArtifactId=AliSpark-2.x-quickstart \
                       -DarchetypeVersion=1.0-SNAPSHOT \
                       -DgroupId=com.aliyun.odps \
                       -DartifactId=${appName} \
                       -DinteractiveMode=false
popd

