package me.lcardito

import org.apache.spark.{SparkConf, SparkContext}

object SimpleApp {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    //TRANSFORMATIONS: no data has been processed yet:

    sc
      .textFile("file:///usr/local/Cellar/apache-spark/2.1.0/README.md") //Read the file
      .flatMap(line => line.split(" ")) //Flat map the file to get all the words
      .map(word => (word, 1)) //Map all the words in a tuple with key the word and value 1
      .reduceByKey((acc, newValue) => acc + newValue) //Merge the values for each key using an associative and commutative reduce function
      .sortBy(wordToValue => wordToValue._2, ascending = false) //Sort the tuples by the value on the key (second attribute on the tuple)
      .saveAsTextFile("output/readMeWordCount") //Save as text file

    //Same as count _countByValue but I can't figure out a way to order it
    //    implicit val ordering = new Ordering[Int] {
    //      override def compare(x: Int, y: Int): Int = y.compare(x)
    //    }
    //    val something = tokenisezData.countByValue()

  }
}