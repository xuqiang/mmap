#!/bin/bash

function build_index() {
    input_path=$1
    output_path=$2
    ${SPARK_HOME}/bin/spark-submit  --name "build_index@xuqiang" --master yarn-client   --class "BuildIndex" \
    --num-executors 500 --executor-memory 1G --driver-memory 1G --queue "online" --executor-cores 1 \
    --conf "spark.yarn.jar=hdfs://10.1.7.35:49000/bin/spark-assembly-1.3.1-hadoop2.4.0.jar" \
    --verbose ${JAR_FILE} ${input_path} ${output_path}
}
