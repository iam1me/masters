package csci.ryan_williams.masters.coloring

import csci.ryan_williams.masters.coloring._;
import scala.reflect._
import org.apache.spark.graphx._
import scala.util.Sorting._
import scala.collection._
  
/**
 * LDO.scala
 * A GraphColoringAlgorithm that uses Largest Degree Ordering
 * ie, the algorithm will color nodes in non-ascending order
 * of the number of adjacent nodes
 */
class LDO extends GreedyColoringAlgorithm 
{
  protected class LargestDegreeOrdering(degrees: Array[(VertexId, Int)]) 
    extends Ordering[VertexId]
  {
    private val _degrees:Array[(VertexId,Degree)] = degrees;
    
    def compare(a: VertexId, b: VertexId): Int = {
          var degA = this._degrees.apply(a toInt)._2;
          var degB = this._degrees.apply(b toInt)._2;
          
          return degA.compare(degB);
    }  
  }
  
  protected class LDOColoringState(g:Graph[_,_]) 
    extends ColoringState(g)
  {
    private def _ordering = new LargestDegreeOrdering(g.ops.degrees.collect());
    
    private def _orderedVertexIds = this.vertices.map(v => v._1);
    scala.util.Sorting.quickSort(_orderedVertexIds)(this._ordering);
    
    final def ordering = { this._ordering; }
    final def orderedVertexIds = { this._orderedVertexIds; }    
  }
  
  protected override def initColoringState(g: Graph[_,_]): ColoringState = 
  {
    return new LDOColoringState(g);    
  }
  
  protected override def getIterator(state: ColoringState): Iterator[VertexId] =
  {    
    var lfstate:LDOColoringState = state.asInstanceOf[LDOColoringState];
    return lfstate.orderedVertexIds.iterator; //.map(v => v._1).iterator;
  }
  
}