import java.io.{FileInputStream, File, ObjectInputStream}

import com.alibaba.analyze.harissurf.SURFInterestPoint
import com.alibaba.analyze.utils.MakeRootSurfPoint

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by aaron on 16-4-13.
  */
object SparkTest {
  def FILE_NAME: String = "word_count_result"

  def main(args: Array[String]) {
    val start_time=System.currentTimeMillis()

    val conf = new SparkConf().setAppName("Spark Exercise:Spark Version Program").setMaster("local")
    val sc = new SparkContext(conf)
//    sc.addJar("/home/aaron/program/workcount.jar")

//    val local_img_path=args(0) //传入本地图片路径
//    val out_rsurf_path=args(1) //传入本地图片转换为rsurf的路径

    val local_img_path="/home/aaron/图片/CBIR/source/img/" //传入本地图片路径
    val out_rsurf_path="/home/aaron/图片/CBIR/source/rsurf/" //传入本地图片转换为rsurf的路径

//    val outfile=new File(out_rsurf_path)
////    if(outfile.exists()) outfile.delete()

    val mkrsurf=new MakeRootSurfPoint(Array(local_img_path,out_rsurf_path))
    mkrsurf.run()

    println("多线程生成rsurf耗时="+(System.currentTimeMillis()-start_time)/1000)
    val start_upload_time=System.currentTimeMillis()


    val hdfs_conf = new Configuration()

    val hdfs_rsurf_path: String = "hdfs://Master:9000/user/aaron/source/rsurf/"

    val allfile_path = new Path(hdfs_rsurf_path)
    val fs = allfile_path.getFileSystem(hdfs_conf)

    val all_local_rsurf_path=new File(out_rsurf_path).listFiles()

    for(i<- 0 until all_local_rsurf_path.length){
//      println(all_local_rsurf_path(i).getName)
//      println(hdfs_rsurf_path+all_local_rsurf_path(i).getName)
      val in=new FileInputStream(all_local_rsurf_path(i))
      val outstream=fs.create(new Path(hdfs_rsurf_path+all_local_rsurf_path(i).getName))  //生成的rsurf传入hdfs
      var n=0
      while({n=in.read(); n != -1}){
        outstream.write(n)
//        println(n)
      }

      println("upload  "+all_local_rsurf_path(i).getName+"  finished")
      in.close()
      outstream.close()
    }

    println("rsurf上传完成,耗时："+(System.currentTimeMillis()-start_upload_time)/1000)

    val start_matchrsurf_time=System.currentTimeMillis()


    val files = fs.listStatus(allfile_path)

    val out = fs.create(new Path("hdfs://Master:9000/user/aaron/allFile.txt"))
    files.foreach(o => out.write((o.getPath + "\n").getBytes()))
    out.close()


    val textFiles = sc.textFile("hdfs://Master:9000/user/aaron/allFile.txt").filter(line => line.contains(".rsurf"))
//    println(textFiles.count())
    val target = "hdfs://Master:9000/user/aaron/target/rsurf/target.png.rsurf"
    //    textFiles.foreach(line=>MatchSurf.surfMatch(Array(line,target)))
    val result = textFiles.map(line => MatchSurf.surfMatch(Array(line, target))).filter(line=>{!line.equals("")})

    val output_path = "hdfs://Master:9000/user/aaron/output/sparkresult"
    val temp_path: Path = new Path(output_path)
    if (fs.exists(temp_path)) fs.delete(temp_path, true)
    result.saveAsTextFile(output_path)
    println("进行匹配共耗时："+(System.currentTimeMillis()-start_matchrsurf_time)/1000)



    /**
      * binaryFiles方法
      */

    //    val imageObject=sc.binaryFiles("/home/aaron/图片/CBIR/result/*")
    //
    //    println(imageObject.count())
    //    println(imageObject.getClass)
    //
    //    val first=imageObject.first()
    //    println(first.getClass)
    //
    //    val path:String=first._1
    //    println(path)
    //    println(path.substring(5,path.length))
    //    println(first._2.getClass)
    //    val stream=first._2
    //    val in=new ObjectInputStream(stream.open())
    //    val a=in.readInt()
    //    val list:Array[SURFInterestPoint]=new Array[SURFInterestPoint](a)
    //    for(i<- 0 until a){
    //      var sURFInterestPoint=in.readObject().asInstanceOf[SURFInterestPoint]
    //      list(i)=sURFInterestPoint
    //    }
    //    println(list.length)


  }

}
