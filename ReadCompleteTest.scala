/**
  * Created by aaron on 16-4-29.
  */
object ReadCompleteTest {
  def main(args: Array[String]) {

    val target = "hdfs://Master:9000/user/aaron/target/rsurf/target.png.rsurf"

    val source: String = "hdfs://Master:9000/user/aaron/source/rsurf/test.png.rsurf"

    val x=MatchSurf.readComplete(source)
    println("after source readcomplete  "+x.getClass)
    val y=MatchSurf.readComplete(target)
    println("after target readComplete")
    val z=MatchSurf.surfMatch(Array(source,target))
    println(z)
  }

}
