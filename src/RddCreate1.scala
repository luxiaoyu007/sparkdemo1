import org.apache.spark.SparkContext

object RddCreate1 {
  def main(args: Array[String]): Unit = {
    var sc = new SparkContext("local","wordcount")
    val data = Array(1,2,3,4,5,6,7,8,9)
    val dist_data = sc.parallelize(data,3)

//    println(dist_data.collect())
    var data2 = Array(8,9,10)
    val rddInner = data2.intersect(data)
    rddInner.foreach(println)

    val hash_a = "Aa".hashCode()
    val hash_b = "BB".hashCode()
    println(hash_a+" , "+hash_b)

    println(hash_a+"  ---  "+hash_b)
  }
}
