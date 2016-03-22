package csci.ryan_williams.masters.coloring

import org.apache.spark.graphx._;

/** FF.scala
 * 	The First-Fit Algorithm is the simplest GraphColoringAlgorithm,
 * 	coloring them in their default ordering. O(n)
 */
class FF extends GreedyColoringAlgorithm 
{
  protected override def getIterator(state: ColoringState): Iterator[VertexId] =
  {
    return state.vertices
                .map(v => v._1)
                .iterator;
  }
}