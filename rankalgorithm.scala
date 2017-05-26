import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.Encoder
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import scala.util.MurmurHash
import org.apache.hadoop.conf._
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.input._

case class indegreeschema(page:Long, indegree:Long)
case class outdegreeschema(page:Long, outdegree:Long)
case class edgesschema(page1:Long, page2:Long)
case class rankvalues(page:Long, rank:Double)
case class pagetitle(page:Long, title:String)

object rankalgorithm {

  def main(args: Array[String]):Unit =
  {
    if(args.length < 2)
    {
      System.err.println("Usage: DistributionGraph <input file> <output file>");
      System.exit(1)
    }
    //Creating a SparkSession
    val spark = SparkSession
    .builder()
    .master("local[3]")
    .appName("Spark Hive Example")
    .config("spark.sql.warehouse.dir","hdfs://BD-HMD02:9000/user/hive/warehouse")
    .enableHiveSupport()
    .getOrCreate()
    
    //getting the SparkContext from SparkSession
    val sc = spark.sparkContext;
    import spark.implicits._;

@transient val conf=new Configuration
 
conf.set("textinputformat.record.delimiter","#*")

val inputrdd=sc.newAPIHadoopFile(args(0),classOf[TextInputFormat],classOf[LongWritable], classOf[Text],conf).map{case(key,value)=>value.toString}.filter(value=>value.length!=0)

def indexes(line:String): ((String,String),String)={
      val pattern1 = "#index[a-zA-Z0-9]*\\n".r
      val pattern2 = "#%[a-zA-Z0-9]*\\n".r
      val pattern3 = "[a-zA-Z0-9[\\s|[^A-Za-z0-9#@!]]]+\\n".r
      val array =(pattern2.findAllIn(line)).toArray
      val string1 = ((pattern1.findFirstIn(line)).get).split("#index")(1)
      val string2 = (array.mkString(""))
      val string3 = ((pattern3.findFirstIn(line)).get)
      ((string1,string3),string2)
      }

val filterrdd=inputrdd.filter(x=>x.contains("#%")).map(line=>indexes(line)).flatMapValues(cites=>cites.split("#%")).filter{case((page,title),cites)=>cites!=""}.map{case((page,title),cites)=>((page.trim(),title.trim()),cites.trim())}.map{case((page,title),cites)=>((MurmurHash.stringHash(page).toLong,title),MurmurHash.stringHash(cites).toLong)}
val titlesDF=filterrdd.map{case((page,title),cities)=>(page,title)}.distinct.map{case(page,title)=>pagetitle(page,title)}.toDF
titlesDF.createOrReplaceTempView("titlesview")
val edges=filterrdd.map{case((page,title),cities)=>(page,cities)}

val graph = Graph.fromEdgeTuples(edges,null)
val indegreesDF = graph.inDegrees.map{case(page,degree)=>indegreeschema(page,degree)}.toDF
val outdegreesDF = graph.outDegrees.map{case(page,degree)=>outdegreeschema(page,degree)}.toDF
indegreesDF.createOrReplaceTempView("indegreesview")
outdegreesDF.createOrReplaceTempView("outdegreesview")
val removenulls2 = spark.sql("select i.page,i.indegree,o.outdegree from indegreesview i full outer join outdegreesview o on(i.page=o.page)")
val joined=removenulls2.na.fill(0)
val edgesDF = edges.map{case(page1,page2)=>edgesschema(page1,page2)}.toDF
edgesDF.createOrReplaceTempView("edgesview")
joined.createOrReplaceTempView("joinedview")
// to get all in links for pages
val joined1 = spark.sql("select e.page1,e.page2,j.indegree,j.outdegree from edgesview e join joinedview j on(e.page2=j.page)")
joined1.createOrReplaceTempView("joined1view")
val weightdenom = spark.sql("select page1,sum(indegree) as denomindegrees,sum(outdegree) as denomoutdegrees  from joined1view group by page1")
weightdenom.createOrReplaceTempView("weightdenomview")
val joined2 = spark.sql("select j.page1,j.page2,j.indegree,j.outdegree,w.denomindegrees,w.denomoutdegrees from joined1view j join weightdenomview w on(j.page1=w.page1)")
joined2.createOrReplaceTempView("joined2view")
val removenulls3 = spark.sql("select page1,page2,(indegree/denomindegrees) as weightin,(outdegree/denomoutdegrees) as weightout from joined2view")
val weightvalues=removenulls3.na.fill(0)
weightvalues.createOrReplaceTempView("weightvaluesview")

var N = graph.numVertices
var d=0.85
var const=(1-d)/N

val pages = graph.vertices.map{case(page,rank)=>rankvalues(page.toLong,1/N.toDouble)}.toDF
pages.createOrReplaceTempView("pagesview")

for(i <- 1 to 10) {
val removenulls = spark.sql("select w.page1,p.page,w.weightin,w.weightout from pagesview p left outer join weightvaluesview w on(p.page=w.page2)")
val joined3=removenulls.na.fill(0)
joined3.createOrReplaceTempView("joined3view")
val removenulls1 = spark.sql("select j.page1,j.page,j.weightin,j.weightout,p.rank from joined3view j left outer join pagesview p on(p.page=j.page1)")
val joined4=removenulls1.na.fill(0)
joined4.createOrReplaceTempView("joined4view")
val rankweights =spark.sql("select page,(weightin*weightout*rank) as totalweight from joined4view")
rankweights.createOrReplaceTempView("rankweightsview")
val finalweights =spark.sql("select page,sum(totalweight) as finalweight from rankweightsview group by page")
val finalweightsrdd=finalweights.rdd.map(row=>(row.getAs[Long]("page"),row.getAs[Double]("finalweight"))).map{case(page,finalweight)=>(page,const+(d*finalweight))}
val newranks =finalweightsrdd.map{case(page,rank)=>rankvalues(page,rank)}.toDF
val pages = newranks
pages.show
pages.createOrReplaceTempView("pagesview")
if(i==10) {
val tenranks=spark.sql("select page,rank from pagesview ORDER BY rank DESC LIMIT 10")
tenranks.createOrReplaceTempView("tenranksview")
val tenrankindegrees = spark.sql("select t.page,i.indegree,t.rank from tenranksview t join indegreesview  i on(i.page=t.page)")
tenrankindegrees.createOrReplaceTempView("tenrankindegreesview")
tenrankindegrees.show
val topten=spark.sql("select ti.title,t.indegree,t.rank from tenrankindegreesview t join titlesview  ti on(ti.page=t.page) ORDER BY rank DESC")
val toptenrdd = topten.rdd.map(row=>(row.getAs[String]("title"),row.getAs[Long]("indegree"),row.getAs[Double]("rank"))).map(x=>x._1 +"\t"+x._2+"\t"+x._3)
toptenrdd.saveAsTextFile(args(1))
}
} 
  }
}