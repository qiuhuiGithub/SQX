# SQX
SPARQL query processing on Spark GraphX <br /> 
Environment: Spark 1.6.1 + Hadoop2.6.4 <br /> 
Usage: spark-submit --master spark://master:7077 --class SparqlSpark.Main --executor-memory 30G --num-executors 32 --executor-cores 12 --driver-memory 6G SQX.jar "HDFS PATH" "SPARQL BGP"
