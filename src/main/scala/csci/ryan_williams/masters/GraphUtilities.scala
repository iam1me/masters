package csci.ryan_williams.masters

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx
import org.apache.spark.graphx._
import org.apache.spark.graphx.util._

import scala.reflect._
import scala.reflect.runtime.universe._

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
  
  def GenerateGraphWithDensity(sc: SparkContext, nodeCount: Long, edgeDensity: Double): Graph[Double,Double] =
  {
    /** 
     *  An undirected graph has no loops and can have at most |V| * (|V| − 1) / 2 edges, 
     *  so the density of an undirected graph is 2 * |E| / (|V| * (|V| − 1)).
     **/
    
    var edgeCount = (edgeDensity * (nodeCount * (nodeCount - 1) / 2D)).round;
    return GenerateGraph(sc, nodeCount, edgeCount);
  }
  
  def GenerateGraph(sc: SparkContext, nodeCount: Long, edgeCount: Long): Graph[Double,Double] = 
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
    var vertices: RDD[(VertexId, Double)] = sc.parallelize(
        (0L to (nodeCount - 1L)).map(
            k => (k, this._numGenerator.nextDouble())
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
              Edge[Double](k, v, this._numGenerator.nextDouble()) 
            }      
        })
        .flatMap(x => x.toSeq)
        .toSeq
      );
                                 
    // construct Graph
    Graph(vertices, edges)
  }
  
  def saveGraph(graph: Graph[_,_], basePath: String) =
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
  }
  
}