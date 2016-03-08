package csci.ryan_williams.masters

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx
import org.apache.spark.graphx._
import org.apache.spark.graphx.util._
import scala.reflect._
import scala.reflect.runtime.universe._

/**
 * GraphHelper.scala
 * A collection of helper methods for generating, navigating, and manipulating graphs
 */
object GraphUtilities
{
  def GenerateGraph[VD : ClassTag,ED : ClassTag](sc: SparkContext, nodeCount: Int, defVD: VD, defED: ED): Graph[(VD),ED] = 
  {
    //GraphGenerators.logNormalGraph(sc, nodeCount)
      //.mapVertices( (id, e) => map(id))    
    
    // For now, create a bi-partite graph. Replace later with algorithm from 
    // research paper
    
    // Create an RDD for the vertices
    val vertices: RDD[(VertexId, VD)] =
      sc.parallelize(Array(
          (0L, defVD), (1L, defVD),
          (2L, defVD), (3L, defVD),
          (4L, defVD)
          ))
                           
    // Create an RDD for edges
    val edges: RDD[Edge[ED]] =
      sc.parallelize(Array(Edge(3L, 7L, defED),    Edge(5L, 3L, defED),
                           Edge(2L, 5L, defED), Edge(5L, 7L, defED),
                           Edge(4L, 0L, defED),   Edge(5L, 0L, defED)
                           ))
                       
    // construct Graph
    Graph(vertices, edges)
  }
}