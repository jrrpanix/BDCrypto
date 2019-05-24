// if using spark-shell use the following command line
// spark-shell --packages com.databricks:spark-csv_2.11:1.2.0 --master local

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors
import sqlContext.implicits._
import org.apache.spark.sql._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer

//
// this code is prototype code to compte the EMA of Price Series Data
//

//
// convert the Kraken csv file to a Trade Object
case class Trade(price: Float, qty: Float, time: Float, bs : String, ml: String)


//
// imperative like solution to compute the ema
//
def calcEMA(trades: Array[Trade], w: Float) : scala.collection.mutable.ArrayBuffer[Float] = {
    val emaB : scala.collection.mutable.ArrayBuffer[Float] = ArrayBuffer.fill[Float](trades.length)(0.0f)
    var e : Float = 0
    for(i <- 0 until trades.length) {
       if (i == 0) e = trades(i).price
       else e = w*trades(i).price + (1.0f - w)*e
       emaB(i) = e
    }
    return emaB
}


// Example

// read in the csv file without the header row
val xrpFile = "hdfs:///user/jr4716/small_set/XRP.csv"
val xrpData = sc.textFile(xrpFile).filter(_.split(",")(0) != "price") 

// convert it to a RDD Trade
val xrpTrade = xrpData.map(_.split(",")).map(p => Trade(p(0).toFloat, p(1).toFloat, p(2).toFloat, p(3).toString, p(4).toString))

// DATAFRAME -- NOT USED 
// included for reference, if sqlContext.implicits_ is included then can convert RDD to a Dataframe
// just in case we end up using them

val xrpDataFrame = xrpTrade.toDF()


//
//
/// convert RDD to anArray and then call calcEMA

val xrpArray = xrpTrade.toArray

//
// compute EMA
//

val emaWeight : Float = 0.05f
val emaSeries = calcEMA(xrpArray, emaWeight)




