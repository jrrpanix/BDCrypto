import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.mllib.regression.LinearRegressionWithSGD


// modified from this example
// https://www.bmc.com/blogs/sgd-linear-regression-example-apache-spark/

// model is y = 5.0 + 0.5*x

val data = sc.textFile("hdfs:///user/jr4716/data0.txt")
val parsed = data.map{ line => 
  val x : Array[String] = line.replace(", ", " ").split(" ")
  val y = x.map{ (a => a.toDouble)}
  val d = y.size - 1
  val c = Vectors.dense(y(1))
  LabeledPoint(y(0), c)
}

//
val numIter = 100
val stepSize = 0.00001

// method #1
val algo1 = new LinearRegressionWithSGD()
algo1.optimizer.setNumIterations(numIter)
algo1.optimizer.setStepSize(stepSize)
val model1 = algo1.run(parsed)
val vp1 = parsed.map{ point =>
   val pred = model1.predict(point.features)
   (point.label, pred)
}

val mse1 = vp1.map{case(v, p) => math.pow((v-p),2)}.mean()



// method #2
val model2 = LinearRegressionWithSGD.train(parsed, numIter, stepSize)
val vp2 = parsed.map{ point =>
   val pred = model2.predict(point.features)
   (point.label, pred)
}

val mse2 = vp2.map{case(v, p) => math.pow((v-p),2)}.mean()


// method #3 with intercept
val algo3 = new LinearRegressionWithSGD()
algo3.setIntercept(true)
algo3.optimizer.setNumIterations(numIter)
algo3.optimizer.setStepSize(stepSize)
val model3 = algo3.run(parsed)
val vp3 = parsed.map{ point =>
   val pred = model3.predict(point.features)
   (point.label, pred)
}

val mse3 = vp3.map{case(v, p) => math.pow((v-p),2)}.mean()
