# ==========================================================================
#  Initialisation du namespace default
# ==========================================================================

echo "#=========================================#"
echo "# Initialisation du namespace default     #"
echo "#=========================================#"

docker run --rm \
 -v ./python_scripts/spark-init.py:/home/spark-intit.py \
 -v $(pwd)/../config/spark_irc_gravitino/spark-defaults.conf:/usr/local/spark/conf/spark-defaults.conf \
 -v $(pwd)/../les_jars:/home/jovyan/.ivy2/jars \
  jupyter/pyspark-notebook:x86_64-lab-4.0.7 \
   /usr/local/spark/bin/spark-submit /home/spark-intit.py

echo "#==============================================#"
echo "#         Terminé                              #"
echo "#==============================================#"