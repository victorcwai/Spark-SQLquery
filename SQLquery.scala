package polyu.bigdata
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object SQLquery {
    def getResults(sc:SparkContext, lower_bound:Int, upper_bound:Int, data_path:String):RDD[(Int, Int, Int)]=
   {     
      //insert your code here
      val strRDD = sc.textFile(data_path)
      val strArrayRDD = strRDD.map(line => line.split("#")) //strArrayRDD is a matrix of n*4
      val intListRDD = strArrayRDD.map(x => List(x(1).toInt,x(2).toInt,x(3).toInt))
      val sortedIntListRDD = intListRDD.sortBy(x=>x(2),false)
      val groupedRDD = sortedIntListRDD.groupBy(x=>x(1)).map(_._2.toList)
      val filteredListRDD = groupedRDD.filter(x => x(0)(2)>=lower_bound && x(0)(2)<=upper_bound).filter(_.size>0)
      val lastRDD = filteredListRDD.map(x => x(0)).sortBy(x=>x(1)).sortBy(x=>x(0)).map(x =>{(x(0),x(1),x(2))})
      //filteredListRDD.collect().foreach(println)
            
      lastRDD
   }

  def main(args: Array[String]) {

    // We need to use spark-submit command to run this program 
    val conf = new SparkConf().setAppName("Assignment 2")//.setMaster("local") // uncomment 'setMaster' for local check
    val sc = new SparkContext(conf)

    val Results = getResults(sc, args(1).toInt, args(2).toInt, args(0))
    //val Results = getResults(sc, 60, 60,"/home/bigdata/Assignment2/Data/OrderDetails")
    Results.collect().foreach(println)
    println("Number of results: %s".format(Results.count()))
    
  }

}
