package csci.ryan_williams.masters

import org.apache.spark.graphx._;
import scala.collection.mutable._;

package object coloring {  
  type Degree = Int
  type Color = Int
  type Coloring = HashMap[VertexId, Color];  
}