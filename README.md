# SQX
SPARQL query processing on Spark GraphX

Usage: spark-submit --master spark://master:7077 --class SparqlSpark.Main --executor-memory 30G --num-executors 32 --executor-cores 12 --driver-memory 6G SQX.jar "HDFS PATH" "SPARQL BGP"
