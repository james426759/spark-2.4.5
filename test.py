from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

sparkConf = SparkConf()
sparkConf.setMaster("k8s://https://10.0.0.228:6443")
sparkConf.setAppName("KUBERNETES-IS-AWESOME")
sparkConf.set("deploy-mode", "cluster")
sparkConf.set("spark.kubernetes.container.image", "james759426/spark-py:2.4.5")
sparkConf.set("spark.kubernetes.authenticate.driver.serviceAccountName", "spark")
sparkConf.set("spark.kubernetes.container.image.pullPolicy", "Never")
sparkConf.set("spark.executor.instances", "2")
sparkConf.set("spark.dynamicAllocation.enabled", "false")
spark = SparkSession.builder.config(conf=sparkConf).getOrCreate()
sc = spark.sparkContext
rdd = sc.parallelize([0 for i in range(100)])
rdd = rdd.map(lambda x:x+1)
print(rdd.collect())
