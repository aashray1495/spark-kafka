FROM nubee/spark_kubernetes_pyspark:v1

ADD file.py /opt/spark/python

ENTRYPOINT ["/opt/entrypoint.sh"]
