package csci.ryan_williams.masters

import util.matching.Regex._;

import java.nio.file._

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.graphx._

import scala.collection.mutable._;

import org.json4s._
import org.json4s.JsonDSL._;
import org.json4s.jackson.JsonMethods._

import csci.ryan_williams.masters.coloring.distributed._

object UnitTest {
  val usage = """
    UnitTest --graph-order=integer --graph-density=double --sample-size=integer 
             --trials=integer [--randomize-priority=boolean] path/for/results
    """
  class Settings extends Serializable {
    var order = 0L;
    var density = .001D;
    var randomizePriority = false;
    var sampleSize = 1;
    var trials = 1;
    var destPath = "./unit_test";
  }
  
  class Results extends Serializable {
    var local_maximum_degree = 0D;
    var local_maximum_path = 0D;
    var local_maximum_color = 0D;
    var local_color_count = 0D;
    var local_rounds = 0D;
    var avg_maximum_degree = 0D;
    var avg_maximum_path = 0D;
    var avg_maximum_color = 0D;
    var avg_color_count = 0D;
    var avg_rounds = 0D;
  }
  
  def main(args: Array[String])
  {
    if(args.length == 0)
    {
      println(s"usage: $usage");
      return;
    }
    
    var settings = parseSettings(args);
    
    var conf = new SparkConf()    
    var sc = new SparkContext(conf)  
    
    var results = apply(sc, settings);
      
    println(pretty(resultsToJson(results)))  
  }
  
  val settingsRegex = "^--([^=\\s]*)=([^\\s]*)$".r;
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
        case "graph-order" => {
          result.order = settingValue.toLong;          
        }
        case "graph-density" => {
          result.density = settingValue.toDouble;          
        }
        case "sample-size" => {
          result.sampleSize = settingValue.toInt;          
        }
        case "trials" => {
          result.trials = settingValue.toInt;          
        }
        case "randomize-priority" => {
          if(settingValue.matches("(?i)^(true)|(t)|(1)|(y)|(yes)$"))
          {
            result.randomizePriority = true;
          }
          else if(settingValue.matches("(?i)^(false)|(f)|(0)|(n)|(no)$"))
          {
            result.randomizePriority = false;
          }
          else{
            throw new Error(s"Invalid randomize-priority setting value: '$settingValue'")
          }
        }
        case _ => {
          throw new Error("Unrecognized setting: " + settingName);
        }
      }     
    }
        
    result.destPath = args.apply(args.length - 1); 
    
    if(result.order <= 0) throw new Error("order must be >= 1")
    if(result.density <= 0) throw new Error("density must be > 0")
    if(result.sampleSize <= 0) throw new Error("sample-size must be > 0")
    if(result.trials <= 0) throw new Error("trials must be > 0")
    
    return result;
  }
  
  def apply(sc: SparkContext, settings: Settings): Results =
  {
    println(s"UnitTest::apply - graph-order: ${settings.order}");
    println(s"UnitTest::apply - graph-density: ${settings.density}");
    
    var results = new Results();
    
    /// Step #1. Generate the graphs to color
    for(i <- 0L to settings.sampleSize - 1)
    {
      var graph:Graph[JObject,JObject] = null
      println(s"UnitTest::apply - generating graph #$i of ${settings.sampleSize}")
      if(settings.randomizePriority) println("randomizing priorities")
      try{
      graph = GraphUtilities.GenerateGraphWithDensity(sc, 
          settings.order, settings.density)
      }catch{
        case e : Throwable => {
          println(s"UnitTest::apply - an error occurred while generating graph #$i")
          throw e
        }
      }
          
      println(s"UnitTest::apply - coloring graph #$i of ${settings.sampleSize}")
      for(j <- 0 to settings.trials - 1)
      {
        println(s"UnitTest::apply - generating coloring #$j of ${settings.trials} for graph #$i");
        var coloring:VertexRDD[LDPO.ColoringState] = null
        try{
          coloring = LDPO.apply(graph, settings.randomizePriority);
        }catch {
          case e : Throwable => {
            println(s"UnitTest::apply - an error occured while coloring graph #$i, iteration #$j")
            throw e
          }
        }
        var coloredGraph = ColorGraph.apply(graph,coloring);
        
        var localResults = getResults(coloredGraph, settings);
        
        var path = s"${settings.destPath}/graphs/$i/$j";
        println(s"UnitTest::apply - saving results for iteration #${j} for graph #${i} to ${path}");
        GraphUtilities.deleteDirectory(path)
        GraphUtilities.saveGraph(coloredGraph, path)       
        
        coloredGraph.unpersist(false)
        
        var resultsJson = resultsToJson(localResults)
        var jsonRDD = sc.parallelize[String](Seq(compact(resultsJson)))
        jsonRDD.saveAsTextFile(path + "/unittest")  
        jsonRDD.unpersist(false)
        
        println(s"UnitTest::apply - updating results after iteration #$j for graph #$i")
        results = combineResults(results, localResults)
      }
      
      graph.unpersist(false)
    }
    
    var finalResultsPath = s"${settings.destPath}/unittest";
    println(s"UnitTest::apply - saving final results to $finalResultsPath");
    
    var finalResultsJson = resultsToJson(results)
    sc.parallelize(Seq(compact(finalResultsJson))).saveAsTextFile(finalResultsPath)
    
    println("UnitTest::apply - complete")
    results
  }
  
  def getResults(graph: Graph[JObject,JObject], settings: Settings):Results = 
  {
    var max_degree = graph.ops.degrees.max()._2.toDouble
    
    var max_path = graph.vertices.map(x => {
      (x._2 \ ColorGraph.ColorPathLengthFieldName) match {
        case length: JInt => {
          length.num.toLong
        }
        case _ => {
          throw new Error("Failed to parse color_path_length")
        }
      }      
    }).max().toDouble
    
    var max_color = graph.vertices.map(x => {
      (x._2 \ ColorGraph.ColorFieldName) match {
        case value: JInt => {
          value.num.toInt
        }
        case _ => {
          throw new Error("Failed to parse color")
        }
      }
    }).max().toDouble
    
    var results = new Results();
    var total_attempts = settings.sampleSize * settings.trials;
    results.local_maximum_degree = max_degree;
    results.local_maximum_path = max_path;
    results.local_maximum_color = max_color;
    results.local_color_count = max_color + 1;
    results.local_rounds = max_path + 2; // orientation round and first color round where |path| = 0
    results.avg_maximum_degree = max_degree / total_attempts;
    results.avg_maximum_path = max_path / total_attempts;
    results.avg_maximum_color = max_color / total_attempts;
    results.avg_color_count = results.local_color_count / total_attempts;
    results.avg_rounds = results.local_rounds / total_attempts;
    results;
  }
  
  def combineResults(r1: Results, r2:Results): Results = {
    var result = new Results();
    result.avg_maximum_degree = r1.avg_maximum_degree + r2.avg_maximum_degree;
    result.avg_maximum_path = r1.avg_maximum_path + r2.avg_maximum_path;
    result.avg_maximum_color = r1.avg_maximum_color + r2.avg_maximum_color;
    result.avg_color_count = r1.avg_color_count + r2.avg_color_count;
    result.avg_rounds = r1.avg_rounds + r2.avg_rounds;
    result;
  }
  
  def resultsToJson(results: Results):JObject = {
    
    var jobj = ("local_maximum_degree", JDouble(results.local_maximum_degree)) ~ 
               ("local_maximum_path", JDouble(results.local_maximum_path)) ~
               ("local_maximum_color", JDouble(results.local_maximum_color)) ~
               ("local_color_count", JDouble(results.local_color_count)) ~
               ("local_rounds", JDouble(results.local_rounds)) ~               
               ("avg_maximum_degree", JDouble(results.avg_maximum_degree)) ~
               ("avg_maximum_path", JDouble(results.avg_maximum_path)) ~
               ("avg_maximum_color", JDouble(results.avg_maximum_color)) ~
               ("avg_color_count", JDouble(results.avg_color_count)) ~
               ("avg_rounds", JDouble(results.avg_rounds))
               
    jobj
  }
  
}