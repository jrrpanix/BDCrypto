

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.sql._
import scala.collection.mutable.ArrayBuffer


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

def predictPrice(dataSet: org.apache.spark.sql.DataFrame, resp: Int, features: Array[Int]) = {

   def rdata(row : org.apache.spark.sql.Row, ri: Int, fi:Array[Int]) = {
      (row(ri).asInstanceOf[Double], Vectors.dense(fi.map(i => row(i).asInstanceOf[Double])))
   }

    val fitdata = dataSet.map(s => rdata(s, resp, features)).toDF("label", "features")
    val scaled = fitScaled(fitdata)
    printSummary(scaled._1, scaled._2, dataSet, resp, features)
}



def featIndex(n : Int) : Array[Int] = {
  var b = ArrayBuffer.fill[Int](n)(0)
  for(i <- 0 until n) {
    b(i) = 3 + 3*(i)
  }
  b.toArray
}


/**
  predict the change in prices

**/



val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val dataSet = sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").load("/user/pjv253/valhalla/MERGED_FINAL.csv")
val features = featIndex(4)
val responses = features.map(x => x+1)
for(i <- 0 until responses.size) {
      predictPrice(dataSet, responses(i), features)
}









