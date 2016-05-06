package csci.ryan_williams.masters.coloring.distributed

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.graphx._

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLImplicits

import scala.collection.mutable._


import scala.util.control.Breaks._
import scala.util.Sorting._

import scala.reflect._

import scala.util._;
import scala.sys._;


/*	LDPO.scala	
 * 	A Distributed LDO Greedy Graph Coloring Algorithm
 * 	LDPO = Largest Degree w/ Priority Ordering
 * 	Designed by Ryan Williams 2016
 * 
 *	By using LDO w/ Priority Ordering, all ambiguity is
 * 	removed such that we know precisely which vertices
 * 	can be colored at any round of the algorithm - removing
 * 	the need for 
 *  
 * */

object LDPO 
{      
  /**	NeighborOrientation
   * 	an 'enum' used to indicate the relationship between two adjacent nodes.
   * 	The algorithm will determine this relationship based first upon the degree of the nodes (larger degree first)
   * 	and then based upon priority to break any ties (priority values must be locally unique for this to work)
   */
  object NeighborOrientation
  {
    val UNKNOWN: Int = 0;
    val PARENT: Int = 1;
    val CHILD: Int = 2;
  }
  
  /**	ColoringState
   * 	holds an individual vertice's information between rounds
   * 	Immutable; a new one must be constructed between rounds for the pregel method to work properly
   */
  case class ColoringState(val id: VertexId,val priority: Long, val neighborIds: Array[VertexId], val color: Int, 
                            val neighborOrientations: Array[(VertexId, Int)], val neighborColors: Array[(VertexId, Int)],
                            val pathLength: Int) extends Serializable;
  
  /**	Message
   * 	each round, each edge-triplet is evaluated so that messages can be sent between
   * 	neighboring nodes. In this algorithm, messages are sent to keep neighbors informed of a vertices state.
   * 	The first message sent is basic orientation information. Based upon this information parent/child relationships
   * 	are established between neighboring nodes. Then any node with no parents can color themselves and send a ColorMessage
   * 	to their children. This casscades through the graph until all nodes are colored.
   */
  sealed abstract class Message extends Serializable;
  case class Initialize() extends Message;
  case class OrientationInfoMessage(id: VertexId, degree: Int, priority: Long) extends Message;
  case class ColorMessage(id: VertexId, color: Int, pathLength: Int) extends Message;

  
  /**	MessageProcessor
   * 	Takes an immutable ColoringState as input. Any number of Messages maybe passed to it
   * 	to for processing. Then the build() command can be called to retrieve a new ColoringState.
   */
  class MessageProcessor(val prevState: ColoringState)
  {
    private val _degree = prevState.neighborIds.length;
    private var _color: Int = prevState.color;
    private var _maxPathLength:Int = prevState.pathLength;
    
    private var _neighborOrientations = {
      if(prevState.neighborOrientations != null)
      {
        ArrayBuffer[(VertexId,Int)]() ++= prevState.neighborOrientations;
      }
      else{
        ArrayBuffer[(VertexId, Int)]()
      }
    }
    
    private var _neighborColors = {
      if(prevState.neighborColors != null)
      {
        ArrayBuffer[(VertexId, Int)]() ++= (prevState.neighborColors);
      }
      else{
        ArrayBuffer[(VertexId, Int)]()
      }
    }    
        
    def process(msg: Message):Unit = 
    {      
        msg match {      
          case Initialize() =>
            /* Do nothing */ 
        
          case OrientationInfoMessage(neighborId,neighborDegree,neighborPriority) =>
            this.updateOrientationInfo(neighborId, neighborDegree, neighborPriority)
          
          case ColorMessage(neighborId, neighborColor, neighborPathLength) =>
            this.updateNeighborColor(neighborId, neighborColor)
            if(this._color < 0)
            {
              this.updateMaxPathLength(neighborPathLength)
            }
            
          case _ =>
            throw new Error("Unrecognized message");
      }
        
      if(this.isReadyToColor())
      {
        this._color = this.calculateColor();
      }      
    }
    
    def build() : ColoringState =
    {
      new ColoringState(prevState.id, prevState.priority, prevState.neighborIds, 
          this._color, this._neighborOrientations.toArray, this._neighborColors.toArray, this._maxPathLength)
    }
    
    def updateMaxPathLength(neighborPathLength: Int):Unit = {
      if(neighborPathLength + 1 > this._maxPathLength)
      {
        this._maxPathLength = neighborPathLength + 1; 
      }    
    }
    
    def getNeighborOrientationOffset(neighborId: VertexId): Int =
    {
      this._neighborOrientations.indexWhere(_._1 == neighborId)
    }
    
    def getNeighborColorOffset(neighborId: VertexId): Int =
    {
      this._neighborColors.indexWhere(_._1 == neighborId)
    }
    
    def updateOrientationInfo(neighborId: VertexId, neighborDegree: Int, neighborPriority: Long): Unit = 
    {      
      var result = NeighborOrientation.UNKNOWN;
      
      if(neighborDegree < this._degree)
      {
        result = NeighborOrientation.CHILD;
      }
      else if(neighborDegree > this._degree)
      {
        result = NeighborOrientation.PARENT;
      }
      else if(neighborPriority < prevState.priority)
      {
        result = NeighborOrientation.PARENT;
      }
      else if(neighborPriority > prevState.priority)
      {
        result = NeighborOrientation.CHILD;
      }
      else if(prevState.id > neighborPriority)
      {
        result = NeighborOrientation.PARENT;
      }
      else {
        result = NeighborOrientation.CHILD;
      }
      
      
      var ndx = getNeighborOrientationOffset(neighborId);
      if(ndx < 0)
      {
        this._neighborOrientations += ((neighborId, result));  
      }
      else
      {
        this._neighborOrientations.update(ndx, (neighborId, result))  
      }
    }
    
    def updateNeighborColor(neighborId: VertexId, neighborColor: Int): Unit=
    {      
      var ndx = getNeighborColorOffset(neighborId);
      if(ndx < 0)
      {
        this._neighborColors += ((neighborId, neighborColor));
      }
      else
      {
        //println(s"updateNeigbhorColor - ${prevState.id} already has an entry for $neighborId. updating.");
        this._neighborColors.update(ndx, (neighborId, neighborColor))  
      }      
    }
        
    def isReadyToColor():Boolean = 
    {
      //println("isReadyToColor() - vertex #" + prevState.id)
      
      /// already colored?
      if(this._color >= 0)
      {
        //println("isReadyToColor() - vertex #" + prevState.id + " already colored")
        return false;
      }
           
      
      /// check to see if all parent neighbors have been colored
      var result = true;
      breakable (for(cur <- prevState.neighborIds) 
      {        
        /// if we don't know all the orientations yet, we aren't ready to color
        var orient_ndx = getNeighborOrientationOffset(cur);
        if(orient_ndx < 0)
        {
          //println(s"isReadyToColor - vertex #${prevState.id}, waiting on orientation info from #$cur")
          result = false
          break();
        }
        
        var orient = this._neighborOrientations.apply(orient_ndx)._2;
        if(orient == NeighborOrientation.UNKNOWN)
        {
          //println(s"isReadyToColor - vertex #${prevState.id}, waiting on orientation info from #$cur")
          result = false
          break();
        }
                
        /// wait for all parnts to be colored
         if(orient == NeighborOrientation.PARENT)
         {
           var color_ndx = getNeighborColorOffset(cur);
           if(color_ndx < 0)
           {
             //println(s"isReadyToColor - vertex #${prevState.id}, waiting on color info from parent #$cur")
             result = false
             break();
           }          
           
           var color = this._neighborColors.apply(color_ndx)._2;
           if(color < 0)
           {
             //println(s"isReadyToColor - vertex #${prevState.id}, waiting on color info from parent #$cur")
             result = false
             break();
           }
         }
      })
            
      return result;
    }
          
    def calculateColor(): Int =
    {   
      //println("calculateColor() for vertex #" + prevState.id)
      var neighborColors = this._neighborColors.filter(p => p._2 >= 0)
                                .map(m => m._2).toArray
                                        
      scala.util.Sorting.quickSort(neighborColors);
      //println("calculateColor() - neighbor Colors: " + neighborColors.toString)
      
      var result = 0;
      breakable(for(cur <- neighborColors)
      {
        //println("calculateColor() - comparing (" + result + ", " + cur + ")")
        if(cur < 0)
        {
          ///ignore
        }
        if(result == cur)
        {          
          result += 1;
        }
        else{
          break();
        }
      })
      
      //println(s"calculateColor() - vertex #${prevState.id}. applying color $result");
      return result;    
    }
  }
  
    

  
  
  def apply(graph: Graph[_,_], randomizePriorities: Boolean): VertexRDD[ColoringState] = 
  {    
    println("");
    
    if(graph == null) 
      throw new Exception("LDPO ERROR: graph is null")
    
    if(graph.vertices == null)
      throw new Exception("LDPO ERROR: graph.vertices == null");
    
    if(graph.vertices.count() == 0)
      throw new Exception("LDPO ERROR: graph is empty");
    
    var neighborIdRDD = graph.ops.collectNeighborIds(EdgeDirection.Either)    
    
    var stateRDD = neighborIdRDD.map(v => {
      var priority = if(randomizePriorities) Random.nextLong() else v._1.toLong;
      
      (v._1, new ColoringState(v._1, priority, v._2, -1, null, null, 0))
    })
    
    var colorGraph = Graph(stateRDD, graph.edges);
    
    var initialMessage = new Array[Message](1);
    initialMessage.update(0, Initialize());
    
    var results = colorGraph.pregel[Array[Message]](
        initialMessage, Integer.MAX_VALUE, EdgeDirection.Either)(
        vectorProgram,sendMessage, mergeMessages )
            
    
    return results.vertices;
  }
  
  /** vectorProgram
   * 	Every time a vertex recieves a message, this algorithm is used to process it
   * 	Based upon the previous vertex state and message, compute a new vertex state
   * 	NOTE: the initial msg sent to the vectorProgram will be null.
   */
  def vectorProgram(id: VertexId, vdata: ColoringState, messages: Array[Message]): ColoringState =
  {       
    //println(s"vectorProgram - processing messages to $id. count: ${messages.length}")
    
    if(messages.length > 0)
    {
      var processor = new MessageProcessor(vdata);
      
      for(curMsg <- messages)
      {
        processor.process(curMsg)
      }
      
      var result = processor.build()          
      return result;
    }
    else{
      println(s"vectorProgram for $id: no change this round")
      return vdata;
    }
  }
  
  /** sendMessage
   * 	Evaluate a pair of vertices + the edge connecting them what message(s) if any to send
   */
  def sendMessage(triplet: EdgeTriplet[ColoringState, _]) : Iterator[(VertexId, Array[Message])] =
  {
    var v1 = triplet.vertexAttr(triplet.srcId)
    var v2 = triplet.vertexAttr(triplet.dstId)
        
    var v1Messages:ArrayBuffer[Message] = null
    var v2Messages:ArrayBuffer[Message] = null
    
    var v1Orient = NeighborOrientation.UNKNOWN;
    var v2Orient = NeighborOrientation.UNKNOWN;

    if(v1.neighborOrientations == null || !v1.neighborOrientations.exists(_._1 == v2.id))
    {
      v1Messages = ArrayBuffer[Message]();
      v1Messages += OrientationInfoMessage(v2.id, v2.neighborIds.length,v2.priority);
    }
    else if(v1.neighborOrientations != null)
    {
      v1.neighborOrientations.find(_._1 ==  v2.id) match {
        case Some((id:Long, o:Int)) => v2Orient = o
        case _ => ;
      }
    }
    
    
    if(v2.neighborOrientations == null || !v2.neighborOrientations.exists(_._1 == v1.id))
    {
      v2Messages = ArrayBuffer[Message]()
      v2Messages += OrientationInfoMessage(v1.id, v1.neighborIds.length,v1.priority);
    }
    else if(v2.neighborOrientations != null)
    {
      v2.neighborOrientations.find(_._1 == v1.id) match {
        case Some((id:Long, o:Int)) => v1Orient = o
        case _ => ;
      }
    }

    if(v2Orient == NeighborOrientation.PARENT && v2.color >= 0 
        && (v1.neighborColors == null || !v1.neighborColors.exists(_._1 == v2.id)))
    {
      if(v1Messages == null) v1Messages = ArrayBuffer[Message]()
      v1Messages += ColorMessage(v2.id, v2.color, v2.pathLength);
      //println(s"sending ${v2.id} color to ${v1.id}")
    }   

    if(v1Orient == NeighborOrientation.PARENT && v1.color >= 0 
        && (v2.neighborColors == null || !v2.neighborColors.exists(_._1 == v1.id)))
    {
      if(v2Messages == null) v2Messages = ArrayBuffer[Message]() 
      v2Messages += ColorMessage(v1.id, v1.color, v1.pathLength);
      //println(s"sending ${v1.id} color to ${v2.id}")
    }    
    
     var results = ArrayBuffer[(VertexId, Array[Message])]()
     if(v1Messages != null) results += ((v1.id, v1Messages.toArray));
     if(v2Messages != null) results += ((v2.id, v2Messages.toArray));
     
     results.iterator
  }
  
  /** mergeMessages
   *  
   */
  def mergeMessages(a: Array[Message], b: Array[Message]): Array[Message] = 
  {
    if(a != null) 
    {
      if( b != null)
      {
        return a ++ b;
      }
      return a;
    }
    else if(b != null)
    {
      return b;
    }
    
    return Array[Message]();
  }  
}


