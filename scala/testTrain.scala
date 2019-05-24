import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql._
import scala.collection.mutable.ArrayBuffer
import scala.util.Random


def generateData(N: Int, gen: scala.util.Random, mu: Double, sigma:Double, b1: Double, b2:Double) : org.apache.spark.sql.DataFrame = {
    var tup : scala.collection.mutable.ArrayBuffer[(Double, Double, Double)] = ArrayBuffer.fill[(Double, Double, Double)](N)((0.0, 0.0, 0.0))
    for(i <- 0 until N) {
      var e : Double = mu + gen.nextGaussian() * sigma
      var x1 = (gen.nextLong() % 250L).toDouble
      var x2 = (gen.nextLong() % 250L).toDouble
      var y = b1*x1 + b2*x2 + e
      tup(i) = (y,x1,x2)
    }
   tup.map(s => (s._1, Vectors.dense(s._2, s._3))).toDF("label", "features")
}

def calcMSE(r : org.apache.spark.sql.DataFrame) : Double = {
    r.select("label", "prediction")
.rdd.map(x => math.pow(x.get(0).asInstanceOf[Double] - x.get(1)
.asInstanceOf[Double], 2.0))
.reduce((x,y) => x + y) / r.count.toDouble
}

val seed : Long = 12345L

val data = generateData(2000, new scala.util.Random(seed), 5.0, 12.0, 5.0, -0.5)

val lr = new LinearRegression().setMaxIter(10) .setFitIntercept(false)

val trainPct : Double = 0.75
val Array(training, test) = data.randomSplit(Array(trainPct, (1.0 - trainPct)), seed)

val model = lr.fit(training)

val testTransform = model.transform(test).select("features", "label", "prediction")
val trainTransform = model.transform(training).select("features", "label", "prediction")
val outmse = calcMSE(testTransform)
val inmse = calcMSE(trainTransform)

printf("model r-sqared    = %f\n", model.summary.r2)
printf("in     sample mse = %f, sample size = %d\n", inmse, trainTransform.count)
printf("out of sample mse = %f, sample size = %d\n", outmse, testTransform.count)
for(i <- 0 until model.coefficients.size) {
  printf("(x%d, %f, %f)\n", i+1, model.coefficients(i), model.summary.tValues(i))
}


