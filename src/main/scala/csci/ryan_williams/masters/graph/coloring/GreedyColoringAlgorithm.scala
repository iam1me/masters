package csci.ryan_williams.masters.graph.coloring

import scala.reflect._
import scala.util.control.Breaks._
import scala.util.Sorting._
import org.apache.spark.graphx
import org.apache.spark.graphx._

/**
 * GreedyColoringAlgorithm
 * The abstract base class for all non-distributed Greedy Coloring Algorithms
 * A Greedy Graph Coloring Algorithm works by:
 * 	1. Identifying some order of visiting the nodes in the Graph (can be dynamic)
 * 	2. Visiting each node in order, and assigning a visited node the lowest possible
 * 			color value available based upon the color values of its neighbors
 */
abstract class GreedyColoringAlgorithm //[VD <: Colorable : ClassTag, ED : ClassTag]
{
  /**
   * GetOrder(graph, ops)
   * Returns an Iterator that is used by the Color method
   * for iterating over all of the vertices in the Graph.
   * This is the key point over which all non-distributed 
   * Greedy Graph Coloring Algorithms differ. Depending upon
   * the order in which the nodes are visited, the Graph
   * could be given an ideal k-coloring such that k = the Chromatic Value
   * OR it could be given a highly innefficient coloring 
   */
  protected def GetOrder[VD,ED](graph: Graph[VD,ED]): Iterator[VertexId];
  
  
  /**
   * Color(graph)
   * Based upon the Iterator supplied by GetOrder(graph),
   * this method traverses all of the vertices in graph,
   * assigning each the lowest possible color value possible
   * based upon its neighboring vertices. If a vertex was 
   * assigned a color before calling Color(), that color
   * value will be left alone
   * 
   * Returns a new Graph where each vertex holds an integer
   * value representing its respective color. The ids of the
   * vertices correspond with the ids of the original graph,
   * and the edges are identical
   */
  final def Color[VD,ED](graph: Graph[VD,ED]): Graph[Int, ED] =
  { 
    var neighborsMap = graph.ops.collectNeighbors(EdgeDirection.Either)  
      .collect()
      
    var color_mapping = graph.vertices
      .map(x => (x._1, -1))
      .collect()   
    
    var it = GetOrder(graph);
    
    while(it.hasNext)
    {
      var id = it.next();      
      //var entry = vertices.apply(id.toInt)
      //var vertex = entry._2;
      
      //println("Graph Coloring Loop. Vertex Id: " + id)
      
      /// determine what color to use based upon
      /// the neighboring, colored vertices
      var neighbors = neighborsMap.apply(id.toInt)._2
      
      var neighbor_colors = neighbors.map(x => 
        color_mapping.apply(x._1.toInt)._2
        )
        
      scala.util.Sorting.quickSort(neighbor_colors)     
      
      var cur_color = 0;
      for (i <- 0 to neighbor_colors.length -1)
      {
        var check = neighbor_colors.apply(i);
        
        if(check > -1)
        {
          if(cur_color < check) { break; }        
          cur_color = check + 1;
        }
      }
      
      /// assign the color
      color_mapping.update(id.toInt, (id, cur_color))
    }
    
    /*
    color_mapping.foreach(x =>
      println("(" + x._1 + "," + x._2 + ")")
      )     
    */
    
    graph.mapVertices((id, v) => color_mapping.apply(id.toInt)._2)
    
  }
}