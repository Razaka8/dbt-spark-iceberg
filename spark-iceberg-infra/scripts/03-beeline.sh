
docker run --rm \
 -v $(pwd)/../config/spark_irc_gravitino/spark-defaults.conf:/usr/local/spark/conf/spark-defaults.conf \
 -v $(pwd)/../les_jars:/home/jovyan/.ivy2/jars \
  jupyter/pyspark-notebook:x86_64-lab-4.0.7 \
   /usr/local/spark/bin/beeline -u "jdbc:hive2://192.168.70.2:10000/" \
                                 --silent=true \
                                 -e "$1" 

