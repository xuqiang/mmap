import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.io.NullWritable
import org.apache.spark.HashPartitioner
import org.apache.spark.Partitioner
import org.apache.spark.TaskContext

class RelScoreRDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
    "item_id.%02d".format(key.asInstanceOf[Long])

  override def generateActualKey(key: Any, value: Any): Any =
    NullWritable.get()
}

object BuildIndex {
 
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("tfidf@xuqiang")
      .set("spark.akka.frameSize", "500")
      .set("spark.akka.askTimeout", "200")
      .set("spark.speculation", "true")
    val input_path = args(0)
    val output_path = args(1)
    val sc = new SparkContext(conf)
    MyUtils.delete_hdfs(output_path, sc)
   
    val rel_score_rdd = sc.newAPIHadoopFile(input_path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text])
    val pstat = rel_score_rdd.mapPartitionsWithIndex( (index, iter) => {
      var total_bytes = 0
    		while(iter.hasNext) {
    		  val (offset, line) = iter.next()
    		  total_bytes += (line.getLength() + 1 )
    		}
      Array( (index, total_bytes) ).toIterator
      
    } , preservesPartitioning = true).reduceByKey( (a, b) => a + b )
    .collectAsMap.toArray.sortWith(  _._1 < _._1 )
    pstat.map{ case (pindex, total_bytes) => println( Array(pindex, total_bytes).mkString("\t") ) }  
    
    val pstart = new scala.collection.mutable.ArrayBuffer[Long]()
    pstart.append(0L)
    for(i <- (1).to(pstat.size - 1, 1)) {
      pstart.append( pstart(i - 1) + pstat(i - 1)._2.toLong ) 
    }
    pstart.map{ case offstart => println(offstart) }
    
    val pstart_offset = pstart.zipWithIndex.map{ case (start_offset, pindex) => { (pindex, start_offset) } }.toMap
    rel_score_rdd.mapPartitionsWithIndex( (pindex, iter) => {
      val offset_start = pstart_offset(pindex)
      iter.map{ case ( offset, line ) => {
        val item_id = line.toString().split("\t")(0)
        Array(item_id, offset_start + offset.get() ).mkString("\t")
      } }
    } ).saveAsTextFile(output_path, classOf[com.hadoop.compression.lzo.LzopCodec])
  
  }
}