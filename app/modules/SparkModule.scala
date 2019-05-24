package modules
import com.google.inject.AbstractModule
import com.google.inject.name.Names
import org.apache.spark.sql.SparkSession
import play.api._

class SparkModule extends AbstractModule {

  def configure() = {
    //TODO connect to dumbo spark
    val sparkSession = SparkSession.builder
      .master("local")
      .appName("Valhalla")
      .getOrCreate()
    bind(classOf[SparkSession])
      .toInstance(sparkSession);
  }
}

// ssh -Y -L 8020:babar.es.its.nyu.edu:8020 -L 8032:babar.es.its.nyu.edu:8032 dumbo
// .config("spark.hadoop.yarn.resourcemanager.hostname","localhost")
// .config("spark.hadoop.yarn.resourcemanager.address","localhost:8032")
// .config("spark.yarn.access.namenodes", "hdfs://localhost:8020")
// .config("spark.hadoop.yarn.resourcemanager.hostname","babar.es.its.nyu.edu")
// .config("spark.hadoop.yarn.resourcemanager.address","babar.es.its.nyu.edu:8032")
// .config("spark.yarn.access.namenodes", "hdfs://babar.es.its.nyu.edu:8020")
// .config("spark.yarn.stagingDir", "hdfs://babar.es.its.nyu.edu:8020/user/pjv253/")
