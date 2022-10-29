import org.apache.spark.mllib.recommendation._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import org.apache.hadoop.conf._
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.input._
import org.apache.spark.sql.SparkSession


object Citation {
 
 def main(args: Array[String]):Unit={
    if(args.length<2){
      System.err.println("Citation app <input file> <input file> <output file>")
      System.exit(1); }
 
      //Creating a spark context
val conf = new SparkConf().setAppName("Citation app").setMaster("local");
@transient val hadoopConf=new Configuration
hadoopConf.set("textinputformat.record.delimiter","#*")
val sc = new SparkContext(conf) 
val inputrdd=sc.newAPIHadoopFile(args(0),classOf[TextInputFormat],classOf[LongWritable], classOf[Text],hadoopConf).map{case(key,value)=>value.toString}.filter(value=>value.length!=0)

def indexes(line:String): (String,String)={
      val pattern1 = "#index[a-zA-Z0-9]*\\n".r
      val pattern2 = "#%[a-zA-Z0-9]*\\n".r
      val array =(pattern2.findAllIn(line)).toArray
      val string1 = ((pattern1.findFirstIn(line)).get).split("#index")(1)
      val string2 = (array.mkString(""))
      val string3 = (array.mkString(""))
      (string1,string2,string3)
      }

 val filterrdd=inputrdd.filter(x=>x.contains("#%")).map(line=>indexes(line)).flatMapValues(cites=>cites.split("#%")).filter{case(index,cites)=>cites!=""}.map(x=>x._1.trim()+" "+x._2.trim())
    filterrdd.saveAsTextFile(args(1));
    sc.stop()
 }
}
