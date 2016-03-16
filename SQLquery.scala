package polyu.bigdata
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object SQLquery {

  def getResults(sc:SparkContext, lower_bound:Int, upper_bound:Int, data_path:String):RDD[(Int, Int, Int)]=
 {     
    //insert your code here
 }
  
  
  def main(args: Array[String]) {     
	      
	// We need to use spark-submit command to run this program 
	val conf = new SparkConf().setAppName("Assignment 2")//.setMaster("local")  // uncomment 'setMaster' for local check
  val sc = new SparkContext(conf)  
	
  val Results = getResults(sc, args(1).toInt, args(2).toInt, args(0))
	Results.collect().foreach(println)
	println("Number of results: %s".format(Results.count()))
  }

}