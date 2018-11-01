import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.math._

object MoviePlot {
  def main(args: Array[String]): Unit = {
    //    System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
    if (args.length != 2) {
      println("Usage: MoviePlot InputFile Term")
    }
    // create Spark context with Spark configuration
    //    val sc = new SparkContext(new SparkConf().setAppName("Spark MoviePlot").setMaster("local"))
    val sc = new SparkContext(new SparkConf().setAppName("Spark MoviePlot"))

    //    val file = sc.textFile("C:\\Users\\ashis\\Documents\\Assignments\\Assignment2\\MovieSummaries\\plot_summaries.txt")// input
    val file = sc.textFile(args(0)+ "plot_summaries.txt")
    println(file)
    val term = args(1)
    println(term)
    //    val term = "marvel"

    val stopWords = sc.textFile(args(0)+"common-english-words-with-contractions.txt")

    val stopWordsSet = stopWords.flatMap(s=>s.split(",")).collect().toSet

    stopWords.cache

    val lines = file.map(l=> l.replaceAll("""[\p{Punct}]""", " ").toLowerCase.split("""\s+"""))

    lines.collect()

    val cleanLine = lines.map(l=>l.map(s=>s).filter(s=> stopWordsSet.contains(s) == false))
    cleanLine.cache

    cleanLine.collect()

    val tf = cleanLine.map(l=>(l(0),l.count( _.equals(term)).toDouble/l.size)).filter(t=>t._2!=0.0)
    tf.count

    val numberOfDocTermAppers = file.flatMap(l=>l.split("\n").filter(s=>s.contains(term))).map(l=>("s",1)).reduceByKey(_+_).collect()(0)._2


    file.count()


    val idf = log10(file.count()/tf.count)


    val tfIdf = tf.map(l=>(l._1,l._2*idf))
    tfIdf.collect


    val moviesFile = sc.textFile(args(0)+"movie.metadata.tsv")


    val movieMap = moviesFile.map(m=>(m.split("\t")(0), m.split("\t")(2)))


    movieMap.cache
    movieMap.collect


    val res = movieMap.join(tfIdf).map(l=>(l._2._1,l._2._2)).sortBy(-_._2).map(_._1).take(5)


    movieMap.join(tfIdf).map(l=>(l._2._1,l._2._2)).sortBy(-_._2).collect


    val r = movieMap.join(tfIdf).map(l=>(l._1,l._2._1,l._2._2)).sortBy(-_._3)
    //    sc.parallelize(r).coalesce(1).saveAsTextFile(args(0) + "output")
    r.coalesce(1).saveAsTextFile(args(0) + "output")
    //    r.foreach(println)

  }
}
