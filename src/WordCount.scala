import org.apache.spark.SparkContext

object WordCount {
  def main(args: Array[String]): Unit = {
    var sc = new SparkContext("local","wordcount")
//    var file = sc.textFile("G:\\sparkjars\\spark-2.3.1-bin-hadoop2.7\\LICENSE")
//    file.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).foreach(println(_))

//    var file_test = sc.textFile("./data/*.data")
var file_test = sc.textFile("./data")
    file_test.flatMap(_.split(",")).foreach(println(_))


  }
}
