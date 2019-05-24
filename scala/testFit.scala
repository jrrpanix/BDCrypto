/**
	Sanity Check to Make Sure the Fits Work

start with the following
module load java/1.8.0_72 
module load spark/2.2.0 
spark-shell --packages com.databricks:spark-csv_2.11:1.2.0 --master local -deprecation

**/


	

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.sql._
import scala.collection.mutable.ArrayBuffer

def regdf(fileName : String) : org.apache.spark.sql.DataFrame = {
    sc.textFile(fileName).map(_.split(",")).map(s => (s(2).toDouble, Vectors.dense(s(0).toDouble, s(1).toDouble))).toDF("label", "features")
}

def printSummary(model: org.apache.spark.ml.regression.LinearRegressionModel, coeff: org.apache.spark.ml.linalg.Vector) = {
    printf("R-squared = %f\n", model.summary.r2)
    printf("MSE       = %f\n", model.summary.meanSquaredError)
    for(i <- 0 until coeff.size) {
       printf("(feature_%d,tvalue) = (%f, %f)\n", i, coeff(i), model.summary.tValues(i))
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


val fileName : String = "hdfs:///user/jr4716/gen.csv"
val regressionSet = regdf(fileName)

val fitModel = fitLinear(regressionSet)
printSummary(fitModel, fitModel.coefficients)

val scaled = fitScaled(regressionSet)
printSummary(scaled._1, scaled._2)






