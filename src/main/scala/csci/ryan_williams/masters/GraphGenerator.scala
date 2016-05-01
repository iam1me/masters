package csci.ryan_williams.masters

import util.matching.Regex._;

import org.apache.spark._;
import org.apache.spark.rdd._;
import org.apache.spark.graphx._;

import org.json4s._
import org.json4s.jackson.JsonMethods._

import java.nio.file._;

import scala.collection.mutable._;
import scala.util._;
import scala.sys._;

object GraphGenerator {
  
  val usage = """
      usage: GraphGenerator --order=num --density=double [--dest=path] graphName
    """
  
  class Settings {
    var order = 0L;
    var density = 0D;
    var dest = "./";
    var name = "graph";
  }
  
  private val _numGenerator = new Random();
  this._numGenerator.setSeed(java.util.Calendar.getInstance().getTimeInMillis());
  
  def setSeed(seed: Long)
  {
    this._numGenerator.setSeed(seed)
  }
  
  def main(args: Array[String])
  {
    if(args.length == 0) 
    {
      println(usage);
      return;
    }
    
    var settings = parseSettings(args);
        
    var conf = new SparkConf()    
    var sc = new SparkContext(conf)  
        
    var edgeCount = calculateEdgeCount(settings);
    var graph = generate(sc, settings.order, edgeCount);
    println("GraphGenerator::main - graph has been generated successfully");
    
    var output_dir = Paths.get(settings.dest, settings.name);
    println(s"GraphGenerator::main - saving graph to '$output_dir'");
    
    GraphUtilities.saveGraph(graph, output_dir.toString())
  }
  
  val settingsRegex = "^--([^=\\s]*)=([^\\s]*)$".r;
  val namePattern = "^[\\w \\-]+$";
  def parseSettings(args: Array[String]): Settings = 
  {
    var result = new Settings();
    
    for(i <- 0 to args.length - 2)
    {
      var settingName = "";
      var settingValue = "";
      var temp = this.settingsRegex.findFirstMatchIn(args.apply(i));
      temp match {
        case m: Some[Match] => {
          settingName = m.get.group(1);
          settingValue= m.get.group(2);
        }
        case _ => {
          throw new Error(s"Failed to parse setting: '$temp'");
        }
      }
      
      settingName match {
        case "order" => {
          result.order = settingValue.toLong;
          if(result.order <= 0) throw new Error("order out of range");
        }
        case "density" => {
          result.density = settingValue.toDouble;
          if(result.density < 0) throw new Error("density out of range");
        }
        case "dest" => {
          result.dest = settingValue;
        }
        case _ => {
          throw new Error("Unrecognized setting: " + settingName);
        }
      }     
    }
        
    result.name = args.apply(args.length - 1);    
    if(!result.name.matches(this.namePattern))
    {
      throw new Error(s"Invalid graph name: '${result.name}'");
    }
    
    return result;
  }
  
  def calculateEdgeCount(settings:Settings):Long =
  {
    /** 
     *  An undirected graph has no loops and can have at most |V| * (|V| − 1) / 2 edges, 
     *  so the density of an undirected graph is 2 * |E| / (|V| * (|V| − 1)).
     **/
    return (settings.density * (settings.order * (settings.order - 1) / 2D)).round;
  }
  
  def generate(sc: SparkContext, nodeCount:Long, edgeCount: Long): Graph[JObject,JObject] = 
  {
    var edgeMap = new HashMap[VertexId, ArrayBuffer[VertexId]]();
    
    var curEdgeCount = 0;
    while(curEdgeCount < edgeCount)
    {
      var v1Id = (this._numGenerator.nextDouble() * (nodeCount - 1)).round;
      var v2Id = (this._numGenerator.nextDouble() * (nodeCount - 1)).round;
      
      if(v1Id != v2Id)
      {
        /// for simplification, let v1Id always be < v2Id
        /// then we don't have to check for two map entries
        if(v2Id < v1Id)
        {
          //println(s"before swap: ($v1Id, $v2Id)")
          var temp = v1Id;
          v1Id = v2Id;
          v2Id = temp;
          //println(s"after swap: ($v1Id, $v2Id)")
        }
        
        var exists = false;
        var builder: ArrayBuffer[VertexId] = null;
        if(edgeMap.contains(v1Id))
        {
          builder = edgeMap.apply(v1Id);
          exists = builder.contains(v2Id)
        }
        else
        {
          builder = ArrayBuffer[VertexId]();
          edgeMap += (v1Id -> builder);
        }
        
        if(!exists)
        {
          //if(v1Id == v2Id) throw new Error(s"wtf... ($v1Id, $v2Id)")
          builder += (v2Id);
          curEdgeCount += 1;
        }        
      }      
      else{
        //println(s"skipping self-loop on ($v1Id, $v2Id)")
      }
    }
    
    // Create an RDD for the vertices. For the Vertex Data we will supply a double
    // this is simply to meet the requirements of the GraphX API, and not because
    // the value has any meaning for this application
    var vertices: RDD[(VertexId, JObject)] = sc.parallelize(
        (0L to (nodeCount - 1L)).map(
            k => (k, JObject())
        )
        .toSeq
    );
                           
    // Create an RDD for edges. For the Edge Data we will supply a random double
    // this is simply to meet the requirements of the GraphX API, and not because
    // the value has any meaning for this application
    var edges = sc.parallelize(
        edgeMap.keys.map(k => {
          var adjacencyList = edgeMap.apply(k);
          for (v <- adjacencyList) 
            yield {
              if(v == k) throw new Error(s"Self-Loop detected for vertex $k")
              Edge[JObject](k, v, JObject()) 
            }      
        })
        .flatMap(x => x.toSeq)
        .toSeq
      );
                                 
    // construct Graph
    Graph(vertices, edges)
  }
  
}