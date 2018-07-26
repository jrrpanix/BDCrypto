//spark2-shell

def profile(pair : String, hdfsPath: String) = {
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  println("===========================")
  println("PROFILING", pair)
  val data = sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").load(hdfsPath + pair + "_CLEAN.csv")
  println("===========================")
  data.printSchema
  println("===========================")
  println("row_count", data.count)
  println("===========================")
  println("estimated_size_bytes", org.apache.spark.util.SizeEstimator.estimate(data))
  println("===========================")
  println("fields")
  data.schema.fields.foreach((field)=> {
    println("---------------------------")
    println(field.name)
    println(field.dataType)
    if(field.dataType == org.apache.spark.sql.types.DoubleType ||
      field.dataType == org.apache.spark.sql.types.LongType ||
      field.dataType == org.apache.spark.sql.types.IntegerType ||
      field.dataType == org.apache.spark.sql.types.TimestampType) {
      println("max", data.agg(max(field.name)).first)
      println("min", data.agg(min(field.name)).first)
      println("avg", data.agg(avg(field.name)).first)
    } else {
      if(data.select(data(field.name)).distinct.count < 100) {
        println("distinct values", data.select(data("limit_or_market"))
                .distinct.collect.mkString(", "))
      }
    }
  })
}

val hdfsPath : String = "hdfs:///user/cypto_node/"
val currencies = Array("XXBTZUSD", "XLTCZUSD", "XXRPZUSD", "XETHZUSD")
currencies.foreach(s => profile(s, hdfsPath))
