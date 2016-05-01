package csci.ryan_williams.masters

import csci.ryan_williams.masters._
import csci.ryan_williams.masters.coloring._
import csci.ryan_williams.masters.coloring.distributed._
import csci.ryan_williams.masters.parsing._
import java.io._
import java.util._
import scala.Console._
import scala.util._
import scala.text._
import org.json4s._
import org.json4s.jackson._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization._
import org.apache.spark._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.log4j.ConsoleAppender.SystemOutStream

/**
 * @author ${ryan_williams}
 */
object App 
{  
  implicit val formats = Serialization.formats(NoTypeHints)
  
  def main(args: Array[String])
  {
    /// NOTE: SparkConf is configured via spark-submit script and the default settings file
    var conf = new SparkConf()    
    var sc = new SparkContext(conf)  
    
    run(sc, System.in, System.out)
    
    println("App::main - complete")
  }
  
  def run(sc: SparkContext, input: InputStream, output: OutputStream) 
  {    
    /*
    var inputReader = new java.io.BufferedReader(
        new java.io.InputStreamReader(input));
    
    var inputBuilder = new StringBuilder();
    
    
    var bufferSize = 1024;
    var buffer = new Array[Char](bufferSize);
    var offset = 0;
    
    var readSize = -1;
    while(inputReader.ready())
    {
      readSize =  inputReader.read(buffer, offset, bufferSize);
      if(readSize != -1)
      {
      
        inputBuilder.appendAll(buffer, 0,readSize);
        offset += readSize;
      }
      else{
        println("End of InputReader Detected");
      }
    }
    
    var inputString = inputBuilder.result();
    
    //println("Input Stream Completed. Here are the contents: ");
    //println(inputString);    
    println("\n*** END OF INPUT STREAM CONTENTS ***");
    
    println(compact(render(parse(inputString))));
    
    println("parsing input json...");
    var inputGraph = GraphParser.parse(sc, inputString);
    
    */
    
    //val testGraph =
      //GraphUtilities.GenerateGraph[Double,Int](sc, 100, 0.5, 1)
      
    var testTime = java.util.Calendar.getInstance().getTime();
    var strTime = s"${testTime.getYear()}_${testTime.getMonth()}_${testTime.getDay()}_${testTime.getHours()}_${testTime.getMinutes()}_${testTime.getSeconds()}";
    
    var genNodeCount = 500;
    var genDensity = .001D;
    println(s"generating testGraph with |V|: $genNodeCount and density: $genDensity")
    var testGraph = GraphUtilities.GenerateGraphWithDensity(sc, genNodeCount, genDensity);
    
    println("testGraph vertex Count: " + testGraph.ops.numVertices)
    println("testGraph edge Count: " + testGraph.ops.numEdges)
    println("testGraph density: " + GraphUtilities.GetGraphDensity(testGraph))
    println("testGraph avg degree: " + GraphUtilities.GetAverageVertexDegree(testGraph))
   
    
    var inputGraphPath = s"./input/$strTime";
    GraphUtilities.saveGraph(testGraph, inputGraphPath)
    
    /*
    var inputVerticesPath = s"./input/$strTime/vertices";
    var inputEdgePath = s"./input/$strTime/edges";      
    var strVertexRDD = GraphUtilities.SerializeVertices(testGraph);
    var strEdgeRDD = GraphUtilities.SerializeEdges(testGraph);
    strVertexRDD.saveAsObjectFile(inputVerticesPath)
    strEdgeRDD.saveAsObjectFile(inputEdgePath)   
    
      
    var results = LDPO.apply(sc, testGraph);
    var uncoloredVertices = results.vertices.filter(p => p._2.color < 0);
    uncoloredVertices.foreach(v => {
      var strNeighbors = "";
      v._2.neighborIds.foreach { n => strNeighbors += n + "," }
      println(s"UNCOLORED VERTEX ${v._1}. DEGREE ${v._2.neighborIds.length}. PRIORITY ${v._2.priority}. |PATH| ${v._2.pathLength}. NIEGHBORS $strNeighbors") 
    })
    
    var longestPath = results.vertices.aggregate(0)(
        (curMax,vertex) => {Math.max(curMax, vertex._2.pathLength)}, 
        (a,b) => {Math.max(a,b)}
        )
        
    println(s"\nLongest Path: $longestPath")
    
    var outputGraph = Graph(results.vertices.map(v => (v._1, v._2.color)), results.edges)
    
    var outputPath= s"./output/$strTime";
    //GraphUtilities.saveGraph(outputGraph, outputPath)
    
    
    println(s"loading graph from $outputPath");
    var copyGraph = GraphUtilities.loadGraph(sc, outputPath)
    println("success!")
		*/
    
    
    
        
  }

}
