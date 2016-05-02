package csci.ryan_williams.masters


//import java.nio.file._

import org.json4s._
import org.json4s.JsonDSL._;
import org.json4s.jackson.JsonMethods._

import org.apache.commons.io._;

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.graphx._
import org.apache.spark.graphx.util._

import scala.reflect._
//import scala.reflect.runtime.universe._

import scala.collection.mutable._;
import scala.util._;
import scala.sys._;
import java.util.UUID

/**
 * GraphHelper.scala
 * A collection of helper methods for generating, navigating, and manipulating graphs
 */
object GraphUtilities
{
  val IdFieldName="id";
  val SourceIdFieldName="srcId";
  val DestinationIdFieldName="dstId";
  
  private val _numGenerator = new Random();
  this._numGenerator.setSeed(java.util.Calendar.getInstance().getTimeInMillis());
  
  def setSeed(seed: Long)
  {
    this._numGenerator.setSeed(seed)
  }
  
  def GetGraphDensity(graph: Graph[_,_]): Double =
  {
    /** 
     *  An undirected graph has no loops and can have at most |V| * (|V| − 1) / 2 edges, 
     *  so the density of an undirected graph is 2 * |E| / (|V| * (|V| − 1)).  
     */
    
     var vertexCount = graph.ops.numVertices.toDouble;
     var edgeCount = graph.ops.numEdges.toDouble;
     
     return (2D * edgeCount / (vertexCount * (vertexCount - 1)));
  }
  
  def GetAverageVertexDegree(graph: Graph[_,_]): Double =
  {
    /**
     * The average degree of a graph G is a measure of 
     * how many edges are in set E compared to number of 
     * vertices in set V. Because each edge is incident to two 
     * vertices and counts in the degree of both vertices, 
     * the average degree of an undirected graph is 2*|E|/|V|.
     */
    var vertexCount = graph.ops.numVertices.toDouble
    var edgeCount = graph.ops.numEdges.toDouble
    return (2 *  edgeCount / vertexCount );
  }
  
  def GenerateGraphWithDensity(sc: SparkContext, nodeCount: Long, edgeDensity: Double): Graph[JObject,JObject] =
  {
    /** 
     *  An undirected graph has no loops and can have at most |V| * (|V| − 1) / 2 edges, 
     *  so the density of an undirected graph is 2 * |E| / (|V| * (|V| − 1)).
     **/
    
    var edgeCount = (edgeDensity * (nodeCount * (nodeCount - 1) / 2D)).round;
    return GraphGenerator.apply(sc, nodeCount, edgeCount);
  }
  
  def setField(obj: JObject, fieldName: String, fieldValue: JValue): JObject =
  {
    (obj \ fieldName) match {
      case JNothing => {
        obj ~ (fieldName, fieldValue);
      }
      case _ => {
        obj.replace(List(fieldName), fieldValue).asInstanceOf[JObject]        
      }
    }
  }
  
  def deleteDirectory(path:String):Unit = 
  {
    FileUtils.deleteDirectory(new java.io.File(path));
  }
  
  def saveGraph(graph: Graph[JObject,JObject], basePath: String) = 
  {
    var vertexPath= s"$basePath/vertices";
    var edgePath = s"$basePath/edges";
    
    /// if they exists, clear the destination directories
    deleteDirectory(vertexPath)
    deleteDirectory(edgePath)    
    
    /// save results to the temp directory
    graph.vertices
      .map(v => {
          var jobj = setField(v._2, IdFieldName, v._1);
          compact(jobj)
      })
      .saveAsTextFile(vertexPath);
    
    graph.edges
      .map(e => {
          var jobj = setField(e.attr, SourceIdFieldName, e.srcId);
          jobj = setField(jobj, DestinationIdFieldName, e.dstId);
          compact(jobj)
        })
      .saveAsTextFile(edgePath);
  }  
  
  def loadGraph[VD : ClassTag, ED: ClassTag](sc: SparkContext, basePath: String): Graph[JObject,JObject] =
  {  
    var vertexPath= s"$basePath/vertices";
    var edgePath = s"$basePath/edges";
    
    var vertices = sc.textFile(vertexPath)
                     .map(v => parseVertex(v));
    
    var edges = sc.textFile(edgePath)
                  .map(e => parseEdge(e));
    
    return Graph(vertices,edges)
  }
  
  def parseVertex(jsonData: String): (VertexId, JObject) =
  {
    var jobj = org.json4s.jackson.JsonMethods.parse(jsonData, true)
                .asInstanceOf[JObject];
    
    var idField = (jobj \ IdFieldName)
    idField match{
      case x: JInt => {
        return (x.num.toLong, jobj);
      }
      case _ => {
        throw new Error(s"Failed to parse id from json: '$jsonData'");
      }
    }    
  }
  
  def parseEdge(jsonData: String): Edge[JObject] =
  {
    var jobj = org.json4s.jackson.JsonMethods.parse(jsonData, true)
                .asInstanceOf[JObject];
    
    var srcId:Long = -1;
    var idField = (jobj \ SourceIdFieldName)
    idField match{
      case x: JInt => {
        srcId = x.num.toLong;
      }
      case _ => {
        throw new Error(s"Failed to parse $SourceIdFieldName from json: '$jsonData'");
      }
    }    
    
    var destId:VertexId = -1;
    idField = (jobj \ DestinationIdFieldName)
    idField match{
      case x: JInt => {
        destId = x.num.toLong;
      }
      case _ => {
        throw new Error(s"Failed to parse $DestinationIdFieldName from json: '$jsonData'");
      }
    } 
    
    return Edge[JObject](srcId,destId,jobj)
  }
}