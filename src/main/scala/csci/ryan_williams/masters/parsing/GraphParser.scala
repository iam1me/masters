package csci.ryan_williams.masters.parsing

import scala.reflect._;
import scala.collection.mutable._;

import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.graphx._;

case class VertexData[VD] (id: VertexId, data: VD);
//case class EdgeData[ED] (first: VertexId, second: VertexId, data: ED);


class GraphParser[VD : scala.reflect.ClassManifest ,ED]
{
  implicit val formats = DefaultFormats
  
  def parse(jsonData: String): Graph[VD,ED] = 
  {
    var parsedData = org.json4s.jackson.JsonMethods.parse(jsonData);
    
    var vertexData = (parsedData \ "vertices");
    var edgeData = (parsedData \ "edges");
    
    println("VertexData: " );
    println(vertexData);
    
    println("EdgeData: ");
    println(edgeData);
    
    
    /*
    var vertices = new scala.collection.mutable.ArrayBuffer[(VertexId, VD)]();
    for(vd <- vertexData.children)
    {
      vd.
    }*/
     
    return null;
  }
  
}