import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.sql._
import scala.collection.mutable.ArrayBuffer


case class Trade(price: Double, qty: Double, time: Long, bucket: Long)


/**
  create a org.apache.spark.sql.DataFrame
  from a file that contains a bucket index for each observation

**/
def createDf(dataDir: String, pair : String, keepFields : Array[String]) = {

    val r = sc.textFile(dataDir + pair + ".csv")
    .filter(_.split(",")(0) != "price")
    .map(_.split(","))
    .map(s => (s(0).toDouble, s(1).toDouble, (s(2).toDouble*1000.0f).toLong, s(3).toString, s(4).toString, s(5).toLong))
    .keyBy(_._6)
    .groupByKey()

    // condense row based on weighed price
    def condense (s : (Long, Iterable[(Double, Double, Long, String, String, Long)])) = {
      val bucket = s._1
      val data = s._2.toArray
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
      val last = data.size - 1
      (bucket, (weighted_px, qsum, data(last)._3, bucket))
     }
     var df = r.map(s => condense(s)).sortByKey().map(s => Trade(s._2._1, s._2._2, s._2._3, s._2._4)).toDF()

     // create backward looking moving average price     
     val lagBack = org.apache.spark.sql.expressions.Window.orderBy("bucket").rowsBetween(-10,0)
     df = df.withColumn("ma_back", avg(df("price")).over(lagBack))

     // create forward looking moving average price
     val lagFwd = org.apache.spark.sql.expressions.Window.orderBy("bucket").rowsBetween(0,10)
     df = df.withColumn("ma_fwd", avg(df("price")).over(lagFwd))

     // create column which is difference between current price and backward moving average
     df = df.withColumn("dpx", df("price") - df("ma_back"))

     // create column wihic is differente between fwd moving average and current price
     df = df.withColumn("fwdDelta", df("ma_fwd") - df("price"))

     val fields = df.schema.fields.toList
     fields.foreach((field) => {
        if(!keepFields.contains(field.name)) {
	  df = df.drop(df.col(field.name))
        }
        else if(!field.name.equals("bucket")) {
	   df = df.withColumnRenamed(field.name, pair + "_" + field.name)
        }
     })
     df	
}

def merge(dataDir: String, pairs : Array[String], keepFields : Array[String]) : org.apache.spark.sql.DataFrame = {
    val frames = pairs.map(p => (p,createDf(dataDir, p, keepFields)))
    val head = frames.head._1
    var merged = frames.head._2
    frames.foreach(pair => {
       val key = pair._1
       val data = pair._2
       if (!key.equals(head)) {
          merged = merged.join(data, "bucket")
       }
    })
    merged   
}


def printSummary(model: org.apache.spark.ml.regression.LinearRegressionModel, coeff: org.apache.spark.ml.linalg.Vector,
    dataSet: org.apache.spark.sql.DataFrame, resp: Int, features: Array[Int]) = {
    printf("Estimation = %s\n", dataSet.columns(resp))
    printf("R-squared  = %f\n", model.summary.r2)
    printf("MSE        = %f\n", model.summary.meanSquaredError)
    for(i <- 0 until coeff.size) {
       var name = dataSet.columns(features(i))
       printf("%d: (%s,tvalue) = (%f, %f)\n", i, name, coeff(i), model.summary.tValues(i))
    }
}

def fitLinear(data: org.apache.spark.sql.DataFrame) = {
    val lr = new LinearRegression().setMaxIter(200).setFeaturesCol("features")
    val model = lr.fit(data)
    val prediction = model.transform(data).select("features", "prediction", "label")
    model
}

def fitScaled(data: org.apache.spark.sql.DataFrame) = {

    val scale = new StandardScaler()
    .setInputCol("features")
    .setOutputCol("scaledFeatures")
    .setWithStd(true)
    .setWithMean(false)
    
    val smodel = scale.fit(data)
    val sdata = smodel.transform(data)

    val lr = new LinearRegression().setMaxIter(200).setFeaturesCol("scaledFeatures")
    val model = lr.fit(sdata)
    val pred = model.transform(sdata).select("features", "scaledFeatures", "prediction")

    val coeff = model.coefficients
    val std = smodel.std
    val unscaled : scala.collection.mutable.ArrayBuffer[Double] = ArrayBuffer.fill[Double](coeff.size)(0.0)
    for(i <- 0 until coeff.size) {
       unscaled(i) = coeff(i) / std(i)
    }
    val uv = org.apache.spark.ml.linalg.Vectors.dense(unscaled.toArray)

    (model, uv)
}

def featIndex(n : Int) : Array[Int] = {
  var b = ArrayBuffer.fill[Int](n)(0)
  for(i <- 0 until n) {
    b(i) = 2 + 3*(i)
  }
  b.toArray
}

def predictPrice(dataSet: org.apache.spark.sql.DataFrame, pair: String, resp: Int, features: Array[Int]) = {

   def rdata(row : org.apache.spark.sql.Row, ri: Int, fi:Array[Int]) = {
      (row(ri).asInstanceOf[Double], Vectors.dense(fi.map(i => row(i).asInstanceOf[Double])))
   }

    val fitdata = dataSet.map(s => rdata(s, resp, features)).toDF("label", "features")
    val scaled = fitScaled(fitdata)
    printSummary(scaled._1, scaled._2, dataSet, resp, features)
}




/**
  predict the change in prices

**/
val dataDir : String = "hdfs:///user/jr4716/bucket/"
val pairs : Array[String] = Array("XBT", "ETH", "XRP", "LTC")
val keepFields : Array[String] = Array("bucket", "price", "dpx", "fwdDelta")
val dataSet = merge(dataDir, pairs, keepFields)


val features = featIndex(pairs.size)
val responses = features.map(x => x + 1)
for(i <- 0 until responses.size) {
      predictPrice(dataSet, pairs(i), responses(i), features)
}









