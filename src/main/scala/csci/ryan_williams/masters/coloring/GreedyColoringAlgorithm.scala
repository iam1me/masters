package csci.ryan_williams.masters.coloring

import csci.ryan_williams.masters.coloring._;
import scala.reflect._
import scala.util.control.Breaks._
import scala.util.Sorting._
import scala.collection.mutable._;
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
abstract class GreedyColoringAlgorithm
{ 
  /** ColoringState
   *  An internal class used to track the coloring of a Graph
   *  as an algorithm runs
   */
  protected class ColoringState(g: Graph[_,_])
  {
    private val _graph:Graph[_,_] = g;
    
    private val _vertices:Array[_ <: Tuple2[VertexId,_]] 
      = g.vertices.collect();
    
    private var _uncoloredVertexIds:ArrayBuffer[VertexId] 
      = new ArrayBuffer[VertexId]();
    
    this._vertices.map(v=> v._1).copyToBuffer(this._uncoloredVertexIds)
    
    private var _coloredVertexIds:ArrayBuffer[VertexId] = new ArrayBuffer[VertexId]();
    
    private val _neighborIds:Array[(VertexId, Array[VertexId])] = 
        g.ops.collectNeighborIds(EdgeDirection.Either).collect();

    private val _degreeMap = new HashMap[VertexId, Int]();
    
    g.ops.degrees.collect().foreach(d => { 
      this._degreeMap += (d._1 -> d._2) 
    });
    
    private var _coloring: Coloring = new HashMap[VertexId, Color];    
    
    _vertices.foreach(v =>{
      _coloring += (v._1 -> -1)
    });
    
    final def graph: Graph[_,_] = 
    { this._graph; }
    
    final def vertices: Array[_ <: Tuple2[VertexId,_]] = 
    { this._vertices; }
    
    final def degreeMap: HashMap[VertexId, Int] = 
    { this._degreeMap.clone(); }
    
    final def getDegree(id: VertexId):Int = {
      this._degreeMap.apply(id);
    }
    
    final def neighborIds: Array[(VertexId, Array[VertexId])] = 
    { this._neighborIds; }
    
    final def getNeighborIds(id: VertexId): Array[VertexId] = {
      return this._neighborIds.find(v => v._1 == id)
                 .get._2;
    }
    
    final def getColor(id: VertexId):Int = 
    { this._coloring.apply(id); }    
    
    /**	setColor
     * 	sets the Color of the specified vertex. This can be overriden by
     * 	derived classes to perform additional tasks, like update the incidence
     * 	count of surrounding vertices when using Incidence Degree Ordering
     */
    def setColor(id: VertexId, value: Int):Unit = 
    {
      // we should never color a node twice with a greedy algorithm
      var orig = getColor(id);
      if(orig != -1)
      {
        throw new IllegalStateException("failed to set color of vertex " + id 
            + ": vertex already assigned a color");
      }
      
      this._coloring.update(id, value);      
      this._uncoloredVertexIds -= id;
      this._coloredVertexIds += id;
    }
    
    final def getUncoloredVertexIds(): Array[VertexId] = {
      return this._uncoloredVertexIds.toArray;      
    }
    
    final def hasUncoloredVertices(): Boolean =    {
      return (this._uncoloredVertexIds.length != 0);
    }
    
    final def getColoredVertexIds() : Array[VertexId] = {
      return this._coloredVertexIds.toArray
    }
    
    final def hasColoredVertices(): Boolean = {
      return (this._coloredVertexIds.length != 0);
    }
    
    final def getColoring(): Coloring = { this._coloring.clone(); }
  }
  
  /** initColoringState
   * 	called from apply() to initialize the ColoringState of the Algorithm
   * 	Derived classes may override this to return a derivative of the ColoringState
   * 	class with additional information
   */
  protected def initColoringState(g: Graph[_,_]): ColoringState = 
  { return new ColoringState(g); }
  
  /**
   * getIterator(graph, state)
   * Returns an Iterator that is used by the apply method
   * for iterating over all of the vertices in the Graph.
   * This is the key point over which all non-distributed 
   * Greedy Graph Coloring Algorithms differ. Depending upon
   * the order in which the nodes are visited, the Graph
   * could be given an ideal k-coloring such that k = the Chromatic Value
   * OR it could be given a highly innefficient coloring 
   */
  protected def getIterator(state: ColoringState): Iterator[VertexId];
  
  
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
  def apply(graph: Graph[_,_]): Coloring =
  { 
    println("\nGraphColoringAlgorithm::apply - init");
    
    // initialize the coloring state
    var state = initColoringState(graph);

    //var neighborsMap = graph.ops
      //.collectNeighbors(EdgeDirection.Either).collect()
    
    println("GraphColoringAlgorithm::apply - getIterator");
    var it = getIterator(state);    
    
    println("GraphColoringAlgorithm::apply - main loop");    
    while(it.hasNext)
    {
      var id = it.next();    
      println("GraphColoringAlgorithm::apply - VertedId: " + id);
      
      /// determine what color to use based upon
      /// the neighboring, colored vertices
      var neighborIds = state.getNeighborIds(id);      
      var neighbor_colors = neighborIds.map(x => state.getColor(x));        
      scala.util.Sorting.quickSort(neighbor_colors)     
      
      var cur_color = 0;
      
      /** NOTE: break is implemented in scala via an exception **/
      breakable {
        for (i <- 0 to neighbor_colors.length -1)
        {
          var check = neighbor_colors.apply(i);
          
          if(check > -1)
          {
            if(cur_color < check) { break(); }         
            cur_color = check + 1;
          }
        }
      }
      
      /// assign the color
      state.setColor(id, cur_color);
    }
        
    return state.getColoring();
  }
}