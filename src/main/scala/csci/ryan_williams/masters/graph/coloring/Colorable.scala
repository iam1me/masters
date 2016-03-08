package csci.ryan_williams.masters.graph.coloring

/**
 * Colorable
 * Used for graph coloring, this trait provides the 
 * mechanism for marking a vertex with a color value
 * and to determine if a vertex/edge has a color.  
 * 
 * A "color" is represented as an integer >= 0. if the
 * value <= -1, then the vertex is said to be
 * uncolored
 */
trait Colorable
{  
  /**
   * color accessor 
   */
  def color : Int
  
  /**
   * color mutator
   */
  def color_(c: Int) 
  
  /**
   * is_colored accessor
   */
  final def is_colored : Boolean = (color >= 0)
  
}