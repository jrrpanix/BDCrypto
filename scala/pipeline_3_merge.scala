//spark2-shell

def merge(currencies : Array[String], hdfsPath: String) : org.apache.spark.sql.DataFrame = {
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  val dataFrames: Map[String, org.apache.spark.sql.DataFrame] = currencies.map(pair => {
      var data = sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").load(hdfsPath + pair + "_FINAL.csv")
      data = data.drop("partition")
      val fields = data.schema.fields.toList
      fields.foreach((field)=> {
        if(!field.name.equals("bucket")) {
          data = data.withColumnRenamed(field.name, pair + "_" + field.name)
        }
      })
      (pair, data)
  }).toMap

  val head = dataFrames.head._1
  var range = dataFrames.head._2.select($"bucket")
  dataFrames.foreach(pair => {
    val key = pair._1
    val data = pair._2
    if(!key.equals(head)) {
      val data_range = data.select($"bucket")
      range = range.join(data_range, Seq("bucket"), "outer")
    }
  })
  range = range.sort("bucket").distinct
  var merged = range
  dataFrames.foreach(pair => {
    val data = pair._2
    merged = merged.join(data, Seq("bucket"), "left")
  })

  val make_partition = () => udf((timestamp: Long) => timestamp / (60*24*3) )
  merged = merged.withColumn("partition", make_partition()($"bucket"))

  val fields = merged.schema.fields.toList
  fields.foreach((field)=> {
    if(!field.name.equals("bucket") && !field.name.equals("partition")) {
      val forward = org.apache.spark.sql.expressions.Window.partitionBy("partition").orderBy("bucket").rowsBetween(-14400, 0)
      val fill_column = last(merged(field.name), true).over(forward)
      merged = merged.withColumn(field.name + "_filled", fill_column )
      merged = merged.drop(field.name).withColumnRenamed(field.name + "_filled", field.name)
    }
  })
  merged = merged.na.drop
  val file = hdfsPath + "MERGED_FINAL.csv"
  val path = new org.apache.hadoop.fs.Path(file)
  val fs=org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)
  fs.delete(path, true)
  merged.write.format("csv").option("header", "true").save(file)
  val fileList = fs.listFiles(path, true)
  val permission = new org.apache.hadoop.fs.permission.FsPermission(org.apache.hadoop.fs.permission.FsAction.ALL,org.apache.hadoop.fs.permission.FsAction.READ_EXECUTE,org.apache.hadoop.fs.permission.FsAction.READ_EXECUTE)
  while (fileList.hasNext()) {
    fs.setPermission(fileList.next().getPath(),permission)
  }
  return merged
}

val hdfsPath : String = "hdfs:///user/cypto_node/"
val currencies = Array("XXBTZUSD", "XLTCZUSD", "XXRPZUSD", "XETHZUSD")
val m = merge(currencies, hdfsPath)
