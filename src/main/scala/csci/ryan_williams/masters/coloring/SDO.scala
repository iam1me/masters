package csci.ryan_williams.masters.coloring

import scala.language._;
import scala.collection.mutable._;
import scala.util.Sorting._
import org.apache.spark.graphx._;


/** SDO.scala
 * 	A GreedyColoringAlgorithm that uses Saturation Degree Ordering
 * 	ie, each round the node to visit is one with the greatest number
 * 	of neighbors with distinct colors
 */
class SDO extends GreedyColoringAlgorithm 
{  
  protected class SDOColoringState(g:Graph[_,_]) 
    extends ColoringState(g)
  {
    private var _saturationMap: HashMap[VertexId, HashSet[Color]] = new HashMap[VertexId,HashSet[Color]]();
    this.vertices.foreach(v => { _saturationMap += (v._1 -> new HashSet[Color]()); });
    
    /** getSaturation
     *  returns the saturation of the specified vertex
     *  where the saturation is the number of distinct colors currently used on
     *  adjacent vertices 
     */
    def getSaturation(id: VertexId):Int = 
    {
      this._saturationMap.apply(id).size;
    }
    
    /** setColor
     * 	In addition to setting the color of the specified vertex,
     * 	we will also update the saturationMap of neighboring vertices
     */
    override def setColor(id: VertexId, value: Color) = 
    {
      super.setColor(id, value);  
      
      var neighborIds = this.getNeighborIds(id)
      for(curId <- neighborIds)
      {
        var entry = this._saturationMap.apply(curId);
        if(!entry.contains(value))
        {
          entry += value;
        }        
      }
    }    
  }
  
  protected class SaturationDegreeOrdering(state: SDOColoringState) 
    extends Ordering[VertexId] 
  {
    private val _state = state;
    
    def compare(a: VertexId, b: VertexId): Int =
    {
      var satA = this._state.getSaturation(a)
      var satB = this._state.getSaturation(b)
      
      // NOTE: We want to inverse the comparison so that they are 
      // ordered from greatest to least rather than least to greatest
      var result = satB.compare(satB);
      if(result == 0)
      {
       // if they have the same saturation, attempt to break the tie
       // based upon their degree
        var degA = this._state.degreeMap.apply(a);
        var degB = this._state.degreeMap.apply(b);
        result = degB.compare(degA);
      }      
      
      return result;
    }
  }
      
  protected class SDOVertexIterator(state: SDOColoringState) extends Iterator[VertexId]
  {
    private val _state = state;
    private val _ordering = new SaturationDegreeOrdering(state);
        
    def hasNext: Boolean = 
    {
      return this._state.hasUncoloredVertices();
    }
    
    def next(): org.apache.spark.graphx.VertexId =
    {
      var candidates = this._state.getUncoloredVertexIds();
      scala.util.Sorting.quickSort(candidates)(this._ordering);
      return candidates.apply(0);
    }
  }
  
  protected override def initColoringState(g: Graph[_,_]):ColoringState = 
  { return new SDOColoringState(g); }
  
  protected override def getIterator(state: ColoringState): Iterator[VertexId] =
  {    
    return new SDOVertexIterator(state.asInstanceOf[SDOColoringState]);
  }
  
}