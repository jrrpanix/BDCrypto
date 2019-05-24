val dataDir : String = "hdfs:///user/jr4716/bucket/"
val currencies = Array("XRP", "XBT", "LTC", "ETH")

def getrdd(dataDir: String, pair : String) = {

    val r = sc.textFile(dataDir + pair + ".csv")
    .filter(_.split(",")(0) != "price")
    .map(_.split(","))
    .map(s => (s(0).toFloat, s(1).toFloat, (s(2).toFloat*1000.0f).toLong, s(3).toString, s(4).toString, s(5).toLong))
    .keyBy(_._6)
    .groupByKey()

    def condense (s : (Long, Iterable[(Float, Float, Long, String, String, Long)])) = {
      val bucket = s._1
      val data = s._2.toArray
      var wsum = 0.0f
      var qsum = 0.0f
      for(i <- 0 until data.size) {
            var px = data(i)._1
	    var qx = data(i)._2
	    if (qx == 0.0f) {
	       qx = 0.001f
	    }
	    wsum += px*qx
	    qsum += qx
      }
      val weighted_px = wsum / qsum
      val last = data.size - 1
      (bucket, (weighted_px, qsum, data(last)._3, bucket))
     }
     r.map(s => condense(s)).sortByKey()
}

val xrp = getrdd(dataDir, "XRP")
val eth = getrdd(dataDir, "ETH")
val xbt = getrdd(dataDir, "XBT")
val ltc = getrdd(dataDir, "LTC")

val joinedData = xrp.join(eth).join(xbt).join(ltc)




