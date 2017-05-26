
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import scala.util.MurmurHash

object DistributionGraph {
  def main(args: Array[String]):Unit =
  {
    if(args.length < 2)
    {
      System.err.println("Usage: DistributionGraph <input file> <output file>");
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("DistributionGraph app").setMaster("local");
    val sc = new SparkContext(conf)
    val edges = sc.textFile(args(0)).map(line=>(line.split("\\s+")(0),line.split("\\s+")(1))).map{case(index,citied)=>(MurmurHash.stringHash(index).toLong,MurmurHash.stringHash(citied).toLong)}
    val graph = Graph.fromEdgeTuples(edges,null)
    val numvertices = graph.numVertices
    val degrees = graph.inDegrees
    val revdegrees = degrees.map{case(v1,v2)=>(v2,v1)}.groupByKey().map{case(indegree,vertices)=>(indegree,vertices.size)}
    val distribution = revdegrees.map{case(indegree,vsize)=>(indegree,vsize.toDouble/numvertices.toDouble)}.map(x=>x._1+" "+x._2)
    distribution.saveAsTextFile(args(1));
    sc.stop()
  }
  
}
