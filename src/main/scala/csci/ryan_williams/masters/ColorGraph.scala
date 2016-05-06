package csci.ryan_williams.masters

import util.matching.Regex._;

import java.nio.file._

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.graphx._

import scala.collection.mutable._;

import org.json4s._
import org.json4s.jackson.JsonMethods._

import csci.ryan_williams.masters.coloring.distributed._

/** ColorGraph
 * 	Provides a command line interface for coloring a graph in the file system
 */
object ColorGraph {
  
  val usage = """
      ColorGraph [--randomize-priority=true] [--source=/path/to/srcGraph] /path/to/destGraph
    """
  
  val ColorFieldName = "color";
  val ColorPathLengthFieldName = "color_path_length";
  
  class Settings {
    var sourcePath:String = null;
    var destPath = "./";
    var randomizePriorities = false;
  }
  
  def main(args: Array[String]) =
  {
    if(args.length == 0)
    {
      throw new Error(s"usage: $usage");
    }
    
    var settings = parseSettings(args);
    
    var conf = new SparkConf()    
    var sc = new SparkContext(conf) 
        
    var graph = GraphUtilities.loadGraph(sc, settings.sourcePath);
    var coloring = LDPO.apply(graph, settings.randomizePriorities);
    
    var coloredGraph = apply(graph, coloring);
    
    GraphUtilities.saveGraph(coloredGraph, settings.destPath)
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
        case "source" => {
          result.sourcePath = settingValue;
        }
        case "randomize-priority" => {
          if(settingValue.matches("(?i)^(true)|(t)|(1)|(y)|(yes)$"))
          {
            result.randomizePriorities = true;
          }
          else if(settingValue.matches("(?i)^(false)|(f)|(0)|(n)|(no)$"))
          {
            result.randomizePriorities = false;
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
    
    if(result.sourcePath == null) result.sourcePath = result.destPath;
    if(!Files.exists(Paths.get(result.sourcePath)))
    {
      throw new Error("sourcePath does not exist");
    } 
    
    return result;
  }

  def apply(graph: Graph[JObject,JObject], colorRDD: RDD[(VertexId,LDPO.ColoringState)]):Graph[JObject,JObject]=
  {
    return graph.joinVertices(colorRDD)((vid, jobj, coloringState) => {
      var result = jobj;

      result = GraphUtilities.setField(result, ColorFieldName, JInt(coloringState.color))
      result = GraphUtilities.setField(result, ColorPathLengthFieldName, JInt(coloringState.pathLength))
            
      (result)
    });
  }
  
}