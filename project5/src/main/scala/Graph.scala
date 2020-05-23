import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Graph {
  def main(args: Array[ String ]) {
  	val conf = new SparkConf().setAppName("Graph")
    val sc = new SparkContext(conf) 
  	var graph=sc.textFile(args(0)).map(line=> {val a=line.split(",")
     var adj=List[Int]()
     var vid=a(0).toInt
     a.tail.foreach(x=>adj=x.toInt::adj)
     (vid,vid,adj)})
  	var i=0
  	for(i<-1 to 5)
  	{
  		graph=graph.map(x=>((x._1,x._2),x._3.map(y=>(y,x._2)))).map(x=>(x._1::x._2)).flatMap(x=>x).reduceByKey((x,y)=> x min y ).join(graph.map(x=>(x._1,(x._2,x._3)))).map(x=>(x._1,x._2._1,x._2._2._2))
  	}
    var ans=graph.map(x=>(x._2,1))
    ans=ans.reduceByKey(_+_) 
    ans=ans.sortByKey()
    ans.collect().map(x=>x._1+"\t"+x._2).foreach(println)
  }
}
