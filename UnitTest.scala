import java.io.{FileOutputStream, File}

/**
  * Created by aaron on 16-4-14.
  */
object UnitTest {
  def main(args: Array[String]) {
//    val files=new File("/home/aaron/图片/CBIR/result").list()
//    println(files.length)
//    for(i<- 0 until files.length){
//      if(!files(i).contains(".rsurf"))
//        println(files(i))
//    }
    val test=new Array[String](3)
    test(0)="/home/aaron/图片/CBIR/source/rsurf/test.png.rsurf"
    test(1)="/home/aaron/图片/CBIR/target/rsurf/target.png.rsurf"
    test(2)="/home/aaron/图片/CBIR/output/"

    val name=MatchSurf.surfMatch(test)
    println("test="+name)

  }
}
