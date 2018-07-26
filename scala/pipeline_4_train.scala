//spark2-shell

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.sql._
import scala.collection.mutable.ArrayBuffer

def calcMSE(r : org.apache.spark.sql.DataFrame) : Double = {
    r.select("label", "prediction")
.rdd.map(x => math.pow(x.get(0).asInstanceOf[Double] - x.get(1)
.asInstanceOf[Double], 2.0))
.reduce((x,y) => x + y) / r.count.toDouble
}

def getFeatures(n : Int) : Array[Int] = {
   var b = ArrayBuffer.fill[Int](n)(0)
   for(i <- 0 until n) {
    b(i) = 3 + 3*(i)
    }
    b.toArray
}


def getRegressionData(dataSet: org.apache.spark.sql.DataFrame, features:Array[Int], ri: Int) : org.apache.spark.sql.DataFrame = {
  def rdata(row : org.apache.spark.sql.Row, fi:Array[Int]) = {
     (row(ri).asInstanceOf[Double], Vectors.dense(fi.map(i => row(i).asInstanceOf[Double])))
  }
  dataSet.map(s => rdata(s, features)).toDF("label", "features")
}


def train(pair: String, hdfsPath: String) =  {
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  val dataSet = sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").load(hdfsPath + "MERGED_FINAL.csv")
  val features = getFeatures(4)
  val ri = dataSet.columns.indexOf(pair + "_fwdDelta")
  val data = getRegressionData(dataSet, features, ri)


  // Full Set Regression
  val lr = new LinearRegression().setMaxIter(200).setFeaturesCol("features").setFitIntercept(false)
  val model = lr.fit(data)
  val predict = model.transform(data).select("features", "label", "prediction")
  val columns = dataSet.columns
  printf("FullSet y=%s r2=%f mse=%f size=%d\n", pair, model.summary.r2, calcMSE(predict), predict.count)
  for(i <- 0 until features.size) {
     printf("%d %s (%f, %f)\n", i, columns(features(i)), model.coefficients(i), model.summary.tValues(i))
  }

  // Regression with Training Set
  val trainPct : Double = 0.75
  val seed : Long = 12345L
  val Array(training, test) = data.randomSplit(Array(trainPct, (1.0 - trainPct)), seed)
  val lrt = new LinearRegression().setMaxIter(200).setFeaturesCol("features").setFitIntercept(false)
  val modelTrain = lrt.fit(training)
  val trainP =  modelTrain.transform(training).select("features", "label", "prediction")
  printf("Training y=%s r2=%f mse=%f size=%d\n", pair, modelTrain.summary.r2, calcMSE(trainP), trainP.count)
  val testP =  modelTrain.transform(test).select("features", "label", "prediction")
  printf("Test     y=%s       mse=%f    size=%d\n", pair, calcMSE(testP), testP.count)
  for(i <- 0 until features.size) {
     printf("%d %s (%f, %f)\n", i, columns(features(i)), modelTrain.coefficients(i), modelTrain.summary.tValues(i))
  }

}

val hdfsPath : String = "hdfs:///user/cypto_node/"
val currencies = List("XXBTZUSD", "XLTCZUSD", "XXRPZUSD", "XETHZUSD")
currencies.foreach(x => train(x, hdfsPath))