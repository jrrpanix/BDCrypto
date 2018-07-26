//spark2-shell

def clean(pair : String, hdfsPath: String) : org.apache.spark.sql.DataFrame = {
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  val data = sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").load(hdfsPath + pair + ".csv")
  val dropped = data.drop("buy_or_sell").drop("limit_or_market").drop(data.schema.fields(5).name).na.drop
  val convert_millis = udf((v: Double) => (v * 1000).toLong )
  val converted = dropped.withColumnRenamed("timestamp", "timestamp_seconds").withColumn("timestamp", convert_millis($"timestamp_seconds")).drop("timestamp_seconds")
  val make_bucket = () => udf((timestamp: Long) => timestamp / (1000L * 60) )
  val partitioned = converted.withColumn("bucket", make_bucket()($"timestamp"))
  val weighted_price = udf[Double, Seq[Double], Seq[Double]]((prices,quantities) => {
    val data = prices.zip(quantities)
    var wsum = 0.0
    var qsum = 0.0
    for(i <- 0 until data.size) {
      var px = data(i)._1
      var qx = data(i)._2
      if (qx == 0.0) {
         qx = 0.001
      }
      wsum += px*qx
      qsum += qx
    }
    val weighted_px = wsum / qsum
    weighted_px
  })
  var bucketed = partitioned.groupBy("bucket").agg(collect_list(col("price")) as "price", collect_list(col("quantity")) as "quantity").withColumn("price", weighted_price(col("price"), col("quantity"))).drop("quantity")
  val make_partition = () => udf((timestamp: Long) => timestamp / (60*24*3) )
  bucketed = bucketed.withColumn("partition", make_partition()($"bucket"))
  val sorted = bucketed.sort("bucket")

  val lagBack = org.apache.spark.sql.expressions.Window.partitionBy("partition").orderBy("bucket").rowsBetween(-10,0)
  val lagFwd = org.apache.spark.sql.expressions.Window.partitionBy("partition").orderBy("bucket").rowsBetween(0,10)
  var ma = sorted.withColumn("ma_back", avg(sorted("price")).over(lagBack))
  ma = ma.withColumn("ma_fwd", avg(ma("price")).over(lagFwd))
  ma = ma.withColumn("dpx", ma("price") - ma("ma_back"))
  ma = ma.withColumn("fwdDelta", ma("ma_fwd") - ma("price"))
  ma = ma.drop("ma_back").drop("ma_fwd")

  val file = hdfsPath + pair + "_FINAL.csv"
  val path = new org.apache.hadoop.fs.Path(file)
  val fs=org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)
  fs.delete(path, true)
  ma.write.format("csv").option("header", "true").save(file)
  val fileList = fs.listFiles(path, true)
  val permission = new org.apache.hadoop.fs.permission.FsPermission(org.apache.hadoop.fs.permission.FsAction.ALL,org.apache.hadoop.fs.permission.FsAction.READ_EXECUTE,org.apache.hadoop.fs.permission.FsAction.READ_EXECUTE)
  while (fileList.hasNext()) {
    fs.setPermission(fileList.next().getPath(),permission)
  }
  return ma
}

val hdfsPath : String = "hdfs:///user/cypto_node/"
val currencies = Array("XXBTZUSD", "XLTCZUSD", "XXRPZUSD", "XETHZUSD")
currencies.foreach(s =>clean(s, hdfsPath))

