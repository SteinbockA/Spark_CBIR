import java.awt.Color
import java.awt.image.BufferedImage
import java.io.{FileOutputStream, File, ObjectInputStream}
import java.util
import javax.imageio.ImageIO


import com.alibaba.analyze.harissurf.{SURFInterestPoint, HarrisSurf}
import com.alibaba.analyze.harris.io.InterestPointListInfo
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.apache.log4j.Logger
import scala.collection.JavaConverters._
import org.apache.spark.input.PortableDataStream

import scala.collection.mutable.ListBuffer

/**
  * Created by aaron on 16-4-16.
  */
object MatchSurf {
  val logger=Logger.getLogger(MatchSurf.getClass)
  /**
    * binaryFiles方法
    *
    * @param path
    * @param args0
    * @param args1
    */

//  def surfMatch(path:Array[String],args0:PortableDataStream,args1:PortableDataStream):Unit={
////    val stream0=new ObjectInputStream(args0.open())
////    val stream1=new ObjectInputStream(args1.open())
//
//    val source_path=path(0)
//    val target_path=path(1)
//    val output_path=path(2)
//
//    val source_surf_path=source_path+"rsurf/"
//    val target_surf_path=target_path+"rsurf/"
//
//    val source_img_path=source_path+"img/"
//    val target_img_path=target_path+"img/"
//
////    val sourcePoints=Map[String,InterestPointListInfo]
////    val sourceImgs=Map[String,BufferedImage]
//
////    val ipl=
//  }

//  /**textFiels方法  参数为数组
//    *
//    */
//
//  def surfMatch(args:Array[String]):Unit={
//    val source_path=args(0)
//    val target_path=args(1)
//    val output_path=args(2)
//
//    val source_surf_path=source_path+"rsurf/"
//    val target_surf_path=target_path+"rsurf/"
//
//    val source_img_path=source_path+"img/"
//    val target_img_path=target_path+"img/"
//
//    val fs=new File(source_surf_path).listFiles()
//
//
//    var sourcePoints:Map[String,InterestPointListInfo]=Map()
//    var sourceImgs:Map[String,BufferedImage]=Map()
//
//    for(f<-fs){
//      val logo_surf_name=f.getName
//      val ipl = InterestPointInfoReader.readComplete(f.getAbsolutePath)
//      sourcePoints+=(logo_surf_name -> ipl)
//    }
//
//    val temp=new File(target_surf_path).listFiles()
//
//    val tfs=temp(0)
//    val ipl=InterestPointInfoReader.readComplete(tfs.getAbsolutePath)
//
//    val targetPoint=ipl.getList
//
//    for((source_surf_name,sourceipl)<-sourcePoints){
//      println(source_surf_name)
//      val matchMap=HarrisSurf.`match`(sourceipl.getList,targetPoint)
//      HarrisSurf.geometricFilter(matchMap,sourceipl.getWidth,sourceipl.getHeight)
//      HarrisSurf.joinsFilter(matchMap)
//      println("matchmapsize="+matchMap.size())
//
//      var fos:FileOutputStream=null
//      println("test")
////      var source=if(sourceImgs.get(source_surf_name)==null) null else sourceImgs.get(source_surf_name).asInstanceOf[BufferedImage]
////      println("source="+source)
////      if(source==null){
//      println(source_img_path+source_surf_name.replaceFirst(".rsurf",""))
//        var source=ImageIO.read(new File(source_img_path+source_surf_name.replaceFirst(".rsurf","")))
//      println("hel")
//        sourceImgs+=(source_surf_name -> source)
////      }
//      println(target_img_path+tfs.getName.replaceFirst(".rsurf",""))
//      val target=ImageIO.read(new File(arget_img_path+tfs.getName.replaceFirst(".rsurf","")))
//
//      val source_width=source.getWidth
//      val source_height=source.getHeight
//
//      val target_width=target.getWidth
//      val target_height=target.getHeigh
//
//      val outputImage=new BufferedImagesource_width+target_width,source_height+target_height,BufferedImage.TYPE_INT_RGB)
//      val g=outputImage.createGraphics(
//      g.drawImage(source,0,0,source_width,source_height,null)
//      g.drawImage(target,source_width,source_height,target_width,target_height,null)
//
//      var x=0
//      val cs=Array(Color.RED,Color.GREEN,Color.BLUE)
//      println(matchMap.getClass)
////      for((fromPoint,toPoint)<-matchMap){
////        g.setColor(cs(x % 3))
////        x=x+1
////        g.drawLine(fromPoint.asInstanceOf getX.toInt,fromPoint.getY.toInt,toPoint.getX.toInt+source_width,toPoint.getY.toInt+source_height)
////
////      }
//      g.dispose()
//      fos=new FileOutputStream(output_path+tfs.getName+"_"+source_surf_name+"__"+matchMap.size()+".jpg")
//      ImageIO.write(outputImage,"JPEG",fos)
//      fos.close()
//
//
//    }
//
//
//
//  }
//

  /**textFiels方法  应用于HDFS
    *
    */

  def surfMatch(args:Array[String]):String={

    val f=new Path(args(0))
    val tfs=new Path(args(1))


    val output_path="hdfs:Master:9000/user/aaron/output/img"


    val source_img_path=f.getParent.toString.replace("rsurf","img/")
    val target_img_path=tfs.getParent.toString.replace("rsurf","img/")


    var sourceImgs:Map[String,BufferedImage]=Map()


    val logo_surf_name=f.getName


//    println("it brokes here")
    val ipl=readComplete(tfs.toString)
//    println("targetpath="+tfs.toString)

    val targetPoint=ipl.getList
//    println("targetPoint.size()"+targetPoint.size())


    val source_surf_name=logo_surf_name
//    println("test2")
    println(f.toString)
    val sourceipl:InterestPointListInfo=MatchSurf.readComplete(f.toString)
    println(sourceipl.getClass)
//    println("source_surf_name="+source_surf_name)
//    println("test")
//    println("sourceipl.getList.size()="+sourceipl.getList.size())
    val matchMap=HarrisSurf.`match`(sourceipl.getList,targetPoint)
    HarrisSurf.geometricFilter(matchMap,sourceipl.getWidth,sourceipl.getHeight)
    HarrisSurf.joinsFilter(matchMap)
    println("matchmapsize="+matchMap.size())

//    var fos:FileOutputStream=null
//    println("test")

    if(matchMap.size()>10){
//    println("final string="+source_img_path+source_surf_name.replaceFirst(".rsurf",""))

    return source_img_path+source_surf_name.replaceFirst(".rsurf","")

    }
    else return ""




  }

  def readComplete(filePath:String):InterestPointListInfo={
    var fis:ObjectInputStream=null
//    try {
      val path: Path = new Path(filePath)
      val conf = new Configuration()
      val fs = path.getFileSystem(conf)
//      println(path.toString)

      val hdfsInputStream: FSDataInputStream = fs.open(path)
//      println(hdfsInputStream.toString)
      fis = new ObjectInputStream(hdfsInputStream)

      val count = fis.readInt()
      println("count="+count)
      val al: java.util.ArrayList[SURFInterestPoint] = new util.ArrayList[SURFInterestPoint]()
      for (i <- 0 until count) {
        val ip:SURFInterestPoint = fis.readObject().asInstanceOf[SURFInterestPoint]
        al.add(ip)
      }

      val w = fis.readInt()
    println("w="+w)
      val h = fis.readInt()

      val ipl = new InterestPointListInfo()
      ipl.setImageFile(path.getName)
      ipl.setList(al)
      ipl.setWidth(w)
      ipl.setHeight(h)
      println("方法readComplete中="+ipl.getList.size()+"  width="+ipl.getWidth+"  height="+ipl.getHeight)
      fis.close()

      return ipl
//    }
//    catch {
//      case e => logger.error(e.getMessage)
//        return null
//    }
//    finally {
//      if(fis != null){
//        try{
//          fis.close()
//        }catch{
//          case e => logger.error(e.getMessage)
//        }
//      }
//    }

  }
}
