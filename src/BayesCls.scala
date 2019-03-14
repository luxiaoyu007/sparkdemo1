import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

object BayesCls {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("BayesCls")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    var file = spark.read.format("csv").load("iris.data")
   // file.show()

    import  spark.implicits._
    val random = new Random()
    val data = file.map(row =>{
      val label = row.getString(4) match {
        case "Iris-setosa" => 0
        case "Iris-versicolor" => 1
        case "Iris-virginica" => 2
      }

      ( row.getString(0).toDouble,
        row.getString(1).toDouble,
        row.getString(2).toDouble,row.getString(3).toDouble,
       label,
      random.nextDouble())
    }).toDF("_c0","_c1","_c2","_c3","label","rand").sort("rand")
//    data.show()

    // features列包装了 4列， 形成了一个向量Vector
    val assembler = new VectorAssembler().setInputCols(Array("_c0","_c1","_c2","_c3")).setOutputCol("features")
    val _dataset = assembler.transform(data)
//    _dataset.show()
    val Array(train, test) = _dataset.randomSplit(Array(0.8, 0.2)) //随机2-8拆分成训练和测试集
//    train.show()

    val bayes = new NaiveBayes().setFeaturesCol("features").setLabelCol("label") // 设置特征列
    val model = bayes.fit(_dataset) // 训练数据集进行训练
    model.transform(test).show() //测试下【测试数据集】, 看下效果

  }
}
