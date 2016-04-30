import java.io.ObjectInputStream

import com.alibaba.analyze.harissurf.SURFInterestPoint
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.{SparkContext, SparkConf}


/**
  * Created by aaron on 16-4-21.
  */
object ScalaTest {
  //  def ppp(source:String):String={
  //  }

  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf().setAppName("test").setMaster("local")
    val sc=new SparkContext(sparkconf)

    val conf = new Configuration()
    val path = new Path("hdfs://Master:9000/user/aaron/source/rsurf/test.png.rsurf")
    val fs = path.getFileSystem(conf)
    val output_path = "hdfs://Master:9000/user/aaron/output/sparkresult.txt"



    val target = "hdfs://Master:9000/user/aaron/target/rsurf/target.png.rsurf"

    val source: String = "hdfs://Master:9000/user/aaron/source/rsurf/test.png.rsurf"
    val z=MatchSurf.surfMatch(Array(source,target))
    println("after first MatchSurf")
    println(z.toString)

    val textFiles = sc.textFile("hdfs://Master:9000/user/aaron/allFile.txt").filter(line => line.contains(".rsurf"))

    val result = textFiles.map(line => MatchSurf.surfMatch(Array(line, target)))
    val temp_path: Path = new Path(output_path)
    if (fs.exists(temp_path)) fs.delete(temp_path, true)

  }

}
