/**
  * Created by guillaume on 03/02/17.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp {

  def main(args: Array[String]) {
    val logFile = "/home/guillaume/TP/Spark/spark-2.1.0-bin-hadoop2.7/README.md" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    //pour chargement dynamique faire un SparkConf vide
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()

    // val result = logData.filter(line => line.contains("Spark")).take(10)

    val result = logData.flatMap(line => line.split(" "))
    val result1 = result.map(mot => (mot,1))
    val result2 = result1.reduceByKey((a, b) => a + b)


    //contenu RDD result1
    for ( x <- result1){
      println(s"result1 : $x")
    }

    //contenu RDD result2 , c'est un index inversé !!
    for ((k,v) <- result2){
      printf("Index => key: %s, value:%s\n",k,v)
    }

    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    sc.stop()
  }

}
