import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by aaron on 16-4-22.
  */
object WordCount {
  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("Wordcount").setMaster("spark://Master:7077")
    val sc=new SparkContext(conf)
    args.foreach(o=>println("args= "+o))

    val file=sc.textFile(args(0))
    val words=file.flatMap(line=>line.split(" ")).map(word=>(word,1)).reduceByKey((x,y)=>x+y)

//    words.saveAsTextFile("wordcount_result")
    println("Word Count program running results:");
//    words.collect().foreach(e => {
//    val (k,v) = e
//    println(k+"="+v)
//    })
    words.saveAsTextFile("hdfs://Master:9000/user/aaron/count_result")


    println("thr program is success")

  }

}
