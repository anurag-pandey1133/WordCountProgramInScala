import org.apache.spark.sql.SparkSession
import org.apache.log4j._

object WordCountProgram extends App {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("WordCountProgram").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val book = sc.textFile("data/book.txt")
    val word = book.flatMap(x => x.split("\\W"))
    val wordcollected = word.map(x => (x,1))
    val wordcount = wordcollected.reduceByKey((x,y) => (x+y))
    //val wordcount = book.flatMap(x => x.split("\\W")).map(x => (x,1)).reduceByKey((x,y)=>(x+y)).sortBy(_._2)
    wordcount.foreach(println)


}
