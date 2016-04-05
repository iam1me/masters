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
  object NeighborOrientation
  {
    def UNKNOWN = 0;
    def PARENT = 1;
    def CHILD = 2;
  }
  
  class VertexColoringState(vid: VertexId, vertexPriority: Int, vertexNeighborIds: Array[VertexId])
    extends Serializable
  {
    private var _id:VertexId = vid;
    private var _degree:Int = vertexNeighborIds.length;
    private var _priority:Int = vertexPriority;
    private var _neighborIds:Array[VertexId] = vertexNeighborIds;
    private var _neighborOrientation = new HashMap[VertexId,Int](); //Array[(VertexId, Int)] = vertexNeighborIds.map(x => (x,NeighborOrientation.UNKNOWN));
    private var _neighborColor = new HashMap[VertexId, Int]();
    private var _color: Int = -1;
    
    var round_count:Int = 0;
    
    def id:VertexId = { return this._id; }
    def degree:Int = { return this._degree; }
    def priority:Int = { return this._priority; }        
    def neighborIds:Array[VertexId] = { return this._neighborIds; }
    
    def color: Int = { return this._color; }
    //def color_(value: Int) = { this._color = value; }
   
    
    def getNeighborOrientation(neighborId:VertexId):Option[Int]=
    {
      this._neighborOrientation.get(neighborId)      
    }
    
    def getNeighborOrientations(): Array[(VertexId, Option[Int])]=
    {
      this._neighborIds.map(x => (x, getNeighborOrientation(x)))
    }
    
    def getNeighborColor(neighborId:VertexId):Option[Int]=
    {
      this._neighborColor.get(neighborId)   
    }
    
    def getNeighborColors(): Array[(VertexId, Option[Int])] =
    {
      this._neighborIds.map(x => (x, getNeighborColor(x)))
    }
    
    def isReadyToColor():Boolean = 
    {
      println("isReadyToColor() - vertex #" + id)
      
      /// already colored?
      if(color >= 0)
      {
        println("isReadyToColor() - vertex #" + id + " already colored")
        return false;
      }
           
      
      /// check to see if all parent neighbors have been colored
      var result = true;
      breakable (for(cur <- this.getNeighborOrientations()) {
        
        /// if we don't know all the orientations yet, we aren't ready to color
        var orient = cur._2 match {
          case Some(_) => { cur._2.get }
          case _ => 
            { 
              println("isReadyToColor - vertex #" +id + ", waiting on orientation for #" + cur._1) 
              result = false; break(); 
            }
        }
                
        /// wait for all parnts to be colored
         if(orient == NeighborOrientation.PARENT)
         {
           this.getNeighborColor(cur._1) match {
             case Some(_) => {}
             case _ => {
                 println("isReadyToColor - vertex #" + id + ", waiting on color for #" + cur._1)
                 result = false; break(); 
               }             
           }
           
         }
      })
            
      return result;
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
      else if(neighborPriority < this._priority)
      {
        result = NeighborOrientation.PARENT;
      }
      else
      {
        result = NeighborOrientation.CHILD;
      }
      
      this._neighborOrientation.update(neighborId, result)
      
      if(isReadyToColor())
      {
        this._color = this.calculateColor();
      }
    }
    
    def updateNeighborColor(neighborId: VertexId, neighborColor: Int): Unit=
    {
      this._neighborOrientation.update(neighborId, neighborColor)
      
      if(isReadyToColor())
      {
        this._color = this.calculateColor();
      }
    }
    
    def calculateColor(): Int =
    {   
      println("calculateColor() for vertex #" + id)
      var neighborColors = this._neighborColor.map(n => n._2)
                                .filter(c => c >= 0).toArray
        
      scala.util.Sorting.quickSort(neighborColors);
      
      var result = 0;
      breakable(for(cur <- neighborColors)
      {
        if(result == cur)
        {
          result += 1;
        }
        else{
          break();
        }
      })
      
      return result;    
    }
  }
    
  class VertexMessage() extends Serializable
  case class InitializeMessage() extends VertexMessage()
  case class OrientationInfoMessage(id: VertexId, degree: Int, priority: Long) extends VertexMessage()
  case class ColorMessage(id: VertexId, color: Int) extends VertexMessage()
  case class BulkMessage(messages: Array[VertexMessage]) extends VertexMessage()
  
  
  def apply(sc: SparkContext, graph: Graph[_,_]) = 
  {    
    println("");
    
    if(graph.vertices.count() == 0)
      throw new Exception("graph is empty");
    
    var neighborIdRDD = graph.ops.collectNeighborIds(EdgeDirection.Either)    
    
    var stateRDD = neighborIdRDD.map(v => {
      (v._1, new VertexColoringState(v._1, v._1.toInt, v._2))
    })
    
    var colorGraph = Graph(stateRDD, graph.edges);
    
    var results = colorGraph.pregel[VertexMessage](
        InitializeMessage(), Integer.MAX_VALUE, EdgeDirection.Both)(
        vectorProgram,sendMessage, mergeMessages )
    
    println("\n*** RESULTS ***")
        
    results.vertices.collect().map(vdata => {
      println("Vertex ID: " + vdata._1 + ", Color: " + vdata._2.color)
    })
        
    println("*** COMPLETE ***")
  }
  
  /** vectorProgram
   * 	Every time a vertex recieves a message, this algorithm is used to process it
   * 	Based upon the previous vertex state and message, compute a new vertex state
   * 	NOTE: the initial msg sent to the vectorProgram will be null.
   */
  def vectorProgram(id: VertexId, vdata: VertexColoringState, msg: VertexMessage): VertexColoringState =
  {       
        
    var messages = msg match{
      case BulkMessage(msgArray)=> { msgArray }
      case _ => { Array[VertexMessage](msg) }
    }
   
    vdata.round_count += 1
    println("vectorProgram. id: " + id + ", roundCount: " + vdata.round_count + ", messageCount: " + messages.length)
    
    for(curMsg <- messages)
    {
      curMsg match {
        case InitializeMessage() => {          
          println("vectorProgram. id: " + id + ", InitializeMessage()")
        }
        
        case OrientationInfoMessage(otherId, otherDegree, otherPriority) => {
          println("vectorProgram. id: " + id + ", OrientationInfoMessage() from " + otherId)
          vdata.updateOrientationInfo(otherId, otherDegree, otherPriority)
        }
        
        case ColorMessage(parentId, parentColor) => {
          println("vectorProgram. id: " + id + ", ColorMessage(" + parentId + ", " + parentColor + ")")
          vdata.updateNeighborColor(parentId, parentColor)
        }     
      } 
    }
    
    vdata.getNeighborOrientations().foreach(x => {
      println("vectorProgram. id: " + id + ", otherId: " + x._1 +", orientation: " + x._2)
    })
    
    
    return vdata;
  }
  
  /** sendMessage
   * 	Evaluate a pair of vertices + the edge connecting them what message(s) if any to send
   */
  def sendMessage(triplet: EdgeTriplet[VertexColoringState, _]) : Iterator[(VertexId, VertexMessage)] =
  {
    var messages = ArrayBuffer[(VertexId, VertexMessage)]()
    
    println("sendMessage. src: ( id: " + triplet.srcAttr.id + ", color: " + triplet.srcAttr.color);
    println("sendMessage. dst: ( id: " + triplet.dstAttr.id + ", color: " + triplet.dstAttr.color);
    
    var orientationInfo = triplet.dstAttr.getNeighborOrientation(triplet.srcAttr.id)   
    println("orientationInfo. dst->src: " + orientationInfo)
    
    if(!orientationInfo.isDefined)
    {
      messages += (
          ( triplet.dstAttr.id, 
            OrientationInfoMessage(triplet.srcAttr.id, triplet.srcAttr.degree,triplet.srcAttr.priority))
      )
    }
    else if(orientationInfo.get == NeighborOrientation.PARENT)
    {
      if(triplet.srcAttr.color >= 0)
      {
        var colorInfo = triplet.dstAttr.getNeighborColor(triplet.srcAttr.id)
        if(!colorInfo.isDefined)
        {      
          messages += ((triplet.dstAttr.id, ColorMessage(triplet.srcAttr.id, triplet.srcAttr.color)))          
        }
      }
    }
    
    orientationInfo = triplet.srcAttr.getNeighborOrientation(triplet.dstAttr.id)
    println("orientationInfo. src->dst: " + orientationInfo)
    
    if(!orientationInfo.isDefined)
    {
      messages += (
          ( triplet.srcAttr.id, 
            OrientationInfoMessage(triplet.dstAttr.id, triplet.dstAttr.degree,triplet.dstAttr.priority))
      )
    }
    else if(orientationInfo.get == NeighborOrientation.PARENT)
    {    
      if(triplet.srcAttr.color >= 0)
      {
        var colorInfo = triplet.dstAttr.getNeighborColor(triplet.srcAttr.id)
        if(!colorInfo.isDefined)
        {      
          messages += ((triplet.dstAttr.id, ColorMessage(triplet.srcAttr.id, triplet.srcAttr.color)))          
        }
      }
    }
    
     return messages.toIterator   
  }
  
  /** mergeMessages
   *  
   */
  def mergeMessages(a: VertexMessage, b: VertexMessage): VertexMessage = 
  {
    var messagesA = a match{
      case BulkMessage(messages) => { messages }
      case _ => { Array[VertexMessage](a) }
    }
    
    var messagesB = b match {
      case BulkMessage(messages) => { messages }
      case _ => { Array[VertexMessage](b) }
    }
    
    var buffer  = ArrayBuffer[VertexMessage]()
    buffer.appendAll(messagesA)
    buffer.appendAll(messagesB)
    
    BulkMessage(buffer.toArray)
  }
  
}


