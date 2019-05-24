package controllers

import javax.inject._

import java.io.File
import java.io.PipedOutputStream
import java.io.PipedInputStream
import java.io.BufferedReader
import java.io.FileReader
import play.api.mvc._
import play.api.libs.iteratee.Enumerator
import org.apache.spark.sql.SparkSession
import play.api.http.HttpEntity
import scala.concurrent.ExecutionContext.Implicits.global
import akka.stream.scaladsl.{ FileIO, Source, StreamConverters }
import play.api.libs.iteratee.streams.IterateeStreams
import play.api.libs.ws._
import play.api.libs.iteratee._
import akka.util.ByteString
import java.io.{InputStream, ByteArrayInputStream, ByteArrayOutputStream, PrintWriter, OutputStreamWriter}

@Singleton
class ReportController @Inject()(cc: ControllerComponents, sparkSession: SparkSession) (implicit assetsFinder: AssetsFinder)
  extends AbstractController(cc) {

  def index = Action {
    Ok(views.html.index())
  }

  def data(pair: String, algo: String) = Action {
    val source: Source[ByteString, _] = StreamConverters.fromInputStream(()=> getData(pair, algo))
    Result(
        header = ResponseHeader(200, Map.empty),
        body =  HttpEntity.Streamed(source, None, Some("text/csv"))
    )
  }

  def getData(pair: String, algo: String) : InputStream = {
    // val df = sparkSession.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("hdfs://babar.es.its.nyu.edu:8020/user/pjv253/valhalla/" + pair + ".csv")
    val out = new PipedOutputStream()
    val in = new PipedInputStream()
    val writer = new PrintWriter(new OutputStreamWriter(out))
		in.connect(out);
		val pipeWriter = new Thread(new Runnable() {
      def run(): Unit = {
        val reader = new BufferedReader(new FileReader("results/"+pair.toUpperCase+"_" + algo + "_MERGED.csv"))
        var row : String = null
        while({row = reader.readLine(); row != null}) {
          val line = row
          writer.println(line)
        }
        reader.close()
        writer.close()
      }
    })
    pipeWriter.start()
    return in
  }
}
