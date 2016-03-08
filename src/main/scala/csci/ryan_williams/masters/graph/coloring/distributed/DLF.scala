package csci.ryan_williams.masters.graph.coloring.distributed

import scala.reflect._
import scala.language._
import scala.util.control.Breaks._
import scala.collection.mutable._
import org.apache.spark._
import org.apache.spark.graphx
import org.apache.spark.graphx._

/** 
 *  On Greedy Graph Coloring in the Distributed Model
 *  Adrian Kosowski and  Lukasz Kuszner
 *  EuroPar 2006
 *  
 *  algorithm DLF(G): 
  	Round 0:
  		f(v) := false; d(v) = degG(v);
  	Round 3k + 1:
  		if f(v) = false
  			then while Exists N>=(v)(c(u) = c(v) ^ f(u) = true)
  				do c(v) := c(v) + 1;
  	Round 3k + 2:
  		r(v) := 0;
  		if f(v) = false ^c(v) < minu2N>(v){c(u) : f(u) = false}
  			then if 1 = rnd[1, 2 · |{u 2 N(v) : d(u) = d(v) ^ c(u) = c(v)}|]
  				then r(v) := rnd[0, d(v)4];
  	Round 3k + 3:
  		if r(v) > max{u2N(v):d(u)=d(v)^c(u)=c(v)} r(u)
  		then f(v) := true;
 */

  class ISKey(deg: Int, color: Int) extends Serializable
  {
    val d:Int = deg;
    val c:Int = color;  
  }

  class ColoringState[VD, ED](graph: Graph[VD,ED])
  {        
      // fields
      private var _graph: Graph[VD,ED] = null;
      private var _vertices: Array[(VertexId, VD)] = null;
      private var _candidates: Array[VertexId] = null;
      private var _ISMap: Map[ISKey, List[VertexId]] = null;
      private var _neighbors: Map[VertexId, Array[VertexId]] = null;
      private var _colorMap: Map[VertexId, Int] = null;
      private var _degrees: Array[(VertexId, Int)] = null;
      private var _finalizedMap: Map[VertexId, Boolean] = null;
      private var _priorityMap: Map[VertexId, Double] = null;
      
      /// constructor
      {
        this._graph = graph;
        
        this._vertices = this._graph.vertices.collect();    
        //this._graph.cache();
        
        this._neighbors = new HashMap[VertexId, Array[VertexId]]()
        
        this._graph.ops.collectNeighborIds(EdgeDirection.Either)
          .collect()
          .foreach(e => this._neighbors.put(e._1, e._2));
          
          
        //this._graph.cache();
        
        this._degrees = this._graph.ops.degrees.collect();
        this._graph.cache();
        
        
        this._colorMap = new HashMap[VertexId, Int]();         
        this._finalizedMap = new HashMap[VertexId, Boolean]();
        this._priorityMap = new HashMap[VertexId, Double](); 
        
        this._vertices.foreach((v) =>
          {
            this._colorMap.put(v._1, 0);
            this._finalizedMap.put(v._1, false);
            this._priorityMap.put(v._1, 0);
          }
        );
        
      }
      
      /// accessors & mutators
      def c(id: VertexId): Int = {
        this._colorMap.apply(id)
      }
      
      def c_(id: VertexId, value: Int) = {
        this._colorMap.update(id, value)
      }
      
      def d(id: VertexId): Int = {
        this._degrees.find(v => v._1 == id).get._2
      }
      
      def f(id: VertexId): Boolean = {
        this._finalizedMap.apply(id.toInt)
      }
      
      def f_(id: VertexId, value: Boolean) = 
      {
        if(this.c(id) < 0) 
          throw new Throwable("Cannot finalize uncolored vertex")
        
        this._finalizedMap.update(id.toInt, value)
      }
      
      def r(id: VertexId): Double = {
        this._priorityMap.apply(id.toInt)
      }
      
      def r_(id: VertexId, value: Double) = {
        this._priorityMap.update(id.toInt, value)
      }
      
      def N(id: VertexId): Array[VertexId] =
      {
        this._neighbors.apply(id)
      }
      
      def V: Array[(VertexId, VD)] = {
        this._vertices
      }
      
    
      
      //def (id: Vertexid): Array[VertexId] = {
          
      //}
      
      //def find_neighbors(id: VertexId, 
           
  }


  
  

object DLF {     
  
  def apply[VD, ED](sc: SparkContext, graph: Graph[VD,ED]): Graph[Int,ED] = 
  {
    /// initialize the coloring state for the graph
    var state = new ColoringState(graph)
        
    var candidates = state.V.filter(v => !state.f(v._1))
    /*while(candidates.count() > 0)      
    {     
      
      
      //candidates = state.V.filter(v => !state.f(v._1))
    }*/
    
    
    return null;
    
  }
  
  ///increment the color of any vertices where
  /// the vertex color has not been finalized and there exists
  /// neighbors of greater or equal degree that are finalized
  /// and which have the same color
  private def should_increment[VD,ED]
    (id: VertexId, state: ColoringState[VD,ED]): Boolean =
  {
    if(state.f(id)) return false;
    
    var deg = state.d(id)
    var color = state.c(id)
    var neighbors = state.N(id)
    var result = false    
    
    for(n <- neighbors)
    {
      if(state.f(n) && state.d(n) >= deg && state.c(n) == color)
      {
        result = true
        break
      }
    }
    
    return result
  }
  
  private def increment_color[VD,ED](id: VertexId, state: ColoringState[VD,ED]) = {
    
  }
  
  private def generate_priority[VD,ED](id: VertexId, stte: ColoringState[VD,ED]) = {
    
  }
  
  private def try_finalize[VD,ED](id: VertexId, state: ColoringState[VD,ED]) = {
    
  }
}