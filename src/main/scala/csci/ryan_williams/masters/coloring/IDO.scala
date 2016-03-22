package csci.ryan_williams.masters.coloring

import scala.collection.mutable._;
import scala.util.Sorting._;
import org.apache.spark.graphx._;

/** IDO.scala
 * 	A GreedyGraphColoringAlgorithm that uses Incidence Degree Ordering
 * 	ie, the nodes are visited in non-ascending order of the number of
 * 	colored adjacent nodes
 */
class IDO extends GreedyColoringAlgorithm {
  
  protected class IDOColoringState(g: Graph[_,_]) 
    extends ColoringState(g)
  {
    private var _incidenceMap = new HashMap[VertexId, Int]();
    
    this.vertices.foreach(v => {
      _incidenceMap += (v._1 -> 0);
    });
    
    private def incrementIncidence(id: VertexId):Unit = {
      var orig = this.getIncidence(id);
      this._incidenceMap.update(id, orig + 1);
    }
    
    def getIncidence(id: VertexId):Int = {
      this._incidenceMap.apply(id);
    }
    
    /** setColor
     * 	after setting the vertex color, we will additionally update
     * 	the incidence count for each neighboring vertex
     */
    override def setColor(id: VertexId, value: Color):Unit = {
      super.setColor(id, value);
      
      this.getNeighborIds(id)
          .iterator.foreach(nid => { incrementIncidence(nid); });
    }    
  }
  
  protected override def initColoringState(g: Graph[_,_]):ColoringState = {
    return new IDOColoringState(g);
  }
  
  protected class IncidenceDegreeOrdering(state: IDOColoringState) 
    extends Ordering[VertexId]
  {
    private val _state = state;
    
    def compare(a: VertexId, b: VertexId):Int = {
      var incA = this._state.getIncidence(a);
      var incB = this._state.getIncidence(b);
      
      var result = incB.compare(incA);
      if(result == 0)
      {
        var degA = this._state.getDegree(a)
        var degB = this._state.getDegree(b)
        result = degB.compare(degA)
      }
      return result;
    }
  }
  
  protected class IDOVertexIterator(state: IDOColoringState) 
    extends Iterator[VertexId]
  {
    private val _state = state;
    private val _ordering = new IncidenceDegreeOrdering(state);
        
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
  
  protected override def getIterator(state: ColoringState):Iterator[VertexId] =
  {
    return new IDOVertexIterator(state.asInstanceOf[IDOColoringState]);    
  }
  
}