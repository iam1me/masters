package csci.ryan_williams.masters.parsing

import scala.reflect._;
import scala.reflect.runtime.universe._;
import scala.collection.mutable._;

import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.spark._;
import org.apache.spark.rdd._;
import org.apache.spark.graphx._;

object GraphParser
{
  implicit val formats = DefaultFormats
  
  def parse(sc: SparkContext, jsonData: String): Graph[JObject,JObject] = 
  {
    var parsedData = org.json4s.jackson.JsonMethods.parse(jsonData, true);
    
    var vertexData = (parsedData \\ "vertices");
    var edgeData = (parsedData \\ "edges");
    
    println("VertexData: " );
    println(vertexData);
    
    println("EdgeData: ");
    println(edgeData);
    
    /// get vertex information
    var vertices = new scala.collection.mutable.HashMap[VertexId, JObject]();
       
    vertexData.children.foreach(v => {
      
        var vertexObj = v.asInstanceOf[JObject];
      
        var idField = vertexObj findField {      
          case JField("id", _) => true
          case _ => false        
        };
    
        idField match {
          case Some(JField("id", _)) => { 
              var fieldVal = idField.get._2.asInstanceOf[JInt];
              var idVal: VertexId = fieldVal.num.longValue();              
              vertices += (idVal -> vertexObj);
          }
          
          case _ => {
              throw new Exception("Vertex Missing ID Field");
          }
        };        
    });
    
    /// get edge information
    var edges = new scala.collection.mutable.ArrayBuffer[Edge[JObject]]();
    edgeData.children.foreach(e => {
      var edgeObject = e.asInstanceOf[JObject];
      var vertexIdsField = (edgeObject \ "vertexIds").asInstanceOf[JArray];
            
      if(vertexIdsField.values.size != 2)
      {
        throw new Exception("Edges are required to have exactly two Longs in the vertexIds array");
      }
      
      var firstId = vertexIdsField.values.apply(0).asInstanceOf[BigInt].longValue();
      var secondId = vertexIdsField.values.apply(1).asInstanceOf[BigInt].longValue();
      
      edges += new Edge(firstId, secondId, edgeObject);   
    });
    
    /// construct the graph
    return Graph(sc.parallelize(vertices.toList),
        sc.parallelize(edges.toList));
  }  
  
  
  
}

