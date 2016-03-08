package csci.ryan_williams.masters.graph.coloring

import scala.reflect._
import org.apache.spark.graphx._
import scala.util.Sorting._
import scala.collection._
  
/**
 * LF.scala
 * An Object that will perform Largest-First Graph Coloring
 */
class LF extends GreedyColoringAlgorithm 
{
  
  class LFOrd(id: VertexId, deg: Int) extends Ordering[(VertexId, Int)]
  {
    /// Need to get degree in here instead of color
    def compare(x: (VertexId, Int), y: (VertexId, Int)) = { x._2 - y._2 }
  }
  
  implicit def convert(id: VertexId, deg: Int) = new LFOrd(id, deg)
  
  protected override def GetOrder[VD,ED](graph: Graph[VD,ED]): Iterator[Long] =
  {
    var idDegreeMap = graph.ops.degrees.collect();
    scala.util.Sorting.quickSort(idDegreeMap);    
    idDegreeMap.map(x => x._1).iterator       
  }
  

  
}