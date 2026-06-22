ARG SPARK_RUNTIME_IMAGE=workshop-spark-measures-spark:4.1.2
FROM ${SPARK_RUNTIME_IMAGE}

USER spark
ENTRYPOINT ["/opt/entrypoint.sh"]
CMD ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.history.HistoryServer"]
