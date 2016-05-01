package csci.ryan_williams.masters

import org.apache.spark.SparkContext

import org.json4s._
import org.json4s.JsonDSL._;
import org.json4s.jackson.JsonMethods._

import org.apache.spark.rdd.RDD
import org.apache.spark.graphx
import org.apache.spark.graphx._
import org.apache.spark.graphx.util._

import scala.reflect._
//import scala.reflect.runtime.universe._

import scala.collection.mutable._;
import scala.util._;
import scala.sys._;

/**
 * GraphHelper.scala
 * A collection of helper methods for generating, navigating, and manipulating graphs
 */
object GraphUtilities
{
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
    return GraphGenerator.generate(sc, nodeCount, edgeCount);
  }
  
  def saveGraph(graph: Graph[JObject,JObject], basePath: String) = 
  {
    var vertexPath= s"$basePath/vertices";
    var edgePath = s"$basePath/edges";
    
    graph.vertices
      .map(v => {
          
          var jobj = v._2;
          if(jobj \ "id" == JNothing)
          {
            jobj = jobj ~ JField("id", BigInt(v._1))
          }
          else{
            jobj.replace(List("id"), BigInt(v._1.toString))
          }
          
          compact(jobj)
      })
      .saveAsTextFile(vertexPath);
    
    graph.edges
      .map(e => {
          var jobj = e.attr;
                   
          if(jobj \ "srcId" == JNothing)
          {
            jobj = jobj ~ ("srcId", e.srcId)
          }
          else{
            jobj.replace(List("srcId"), BigInt(e.srcId))
          }
          
          if(jobj \ "dstId" == JNothing)
          {
            jobj = jobj ~ ("dstId", BigInt(e.dstId))
          }
          else{
            jobj.replace(List("dstId"), BigInt(e.dstId))
          }

          compact(jobj)
        })
      .saveAsTextFile(edgePath);
  }  
  
  /*def saveGraph(graph: Graph[_,_], basePath: String) =
  {
    var vertexPath= s"$basePath/vertices";
    var edgePath = s"$basePath/edges";
    graph.vertices.saveAsObjectFile(vertexPath)
    graph.edges.saveAsObjectFile(edgePath)
  }
  
  def loadGraph[VD : ClassTag, ED: ClassTag](sc: SparkContext, basePath: String): Graph[VD,ED] =
  {
    var vertexPath= s"$basePath/vertices";
    var edgePath = s"$basePath/edges";
    var vertices = sc.objectFile[(VertexId, VD)](vertexPath)
    var edges = sc.objectFile[Edge[ED]](edgePath)
    return Graph(vertices,edges)
  }*/
  
}