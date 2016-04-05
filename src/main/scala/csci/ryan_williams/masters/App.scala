package csci.ryan_williams.masters

import csci.ryan_williams.masters._
import csci.ryan_williams.masters.coloring._
import csci.ryan_williams.masters.coloring.distributed._;
import csci.ryan_williams.masters.parsing._

import java.io._;

import scala.Console._;
import scala.util._;
import scala.text._;

import org.json4s._;
import org.json4s.jackson._;
import org.json4s.jackson.JsonMethods._;
import org.json4s.jackson.Serialization._;

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD

/**
 * @author ${user.name}
 */
object App {
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  implicit val formats = Serialization.formats(NoTypeHints)
  
  def main(input: InputStream, output: OutputStream) 
  {    
    
    println("creating spark conference...")
    
    var conf = new SparkConf()
      .setAppName("My Spark Test")
      //.setMaster("52.32.127.248")
      .setSparkHome(System.getenv("SPARK_HOME"))
      //.setJars(Seq(""))
      .setMaster("local[*]")
      
    var sc = new SparkContext(conf)   
    
    println("spark context created successfully!")

    var inputReader = new java.io.BufferedReader(
        new java.io.InputStreamReader(input));
    
    var inputBuilder = new StringBuilder();
    
    
    var bufferSize = 1024;
    var buffer = new Array[Char](bufferSize);
    var offset = 0;
    
    var readSize = -1;
    while(inputReader.ready())
    {
      readSize =  inputReader.read(buffer, offset, bufferSize);
      if(readSize != -1)
      {
      
        inputBuilder.appendAll(buffer, 0,readSize);
        offset += readSize;
      }
      else{
        println("End of InputReader Detected");
      }
    }
    
    var inputString = inputBuilder.result();
    
    //println("Input Stream Completed. Here are the contents: ");
    //println(inputString);    
    println("\n*** END OF INPUT STREAM CONTENTS ***");
    
    println(compact(render(parse(inputString))));
    
    println("parsing input json...");
    var inputGraph = GraphParser.parse(sc, inputString);
    
    
    LDPO.apply(sc, inputGraph);
    
    /*
    var results = new LDO().apply(inputGraph);
    
    //println("\nColoring after applying " + alg.getClass().getName())
    results.foreach(x => {
      println("\t" + x._1 + ": " + x._2);      
    });
    
    println("COLORING RESULT:");
    var serResults = write("vertices" ->
          results.map(x => 
            new JObject(
              new JField("id", new JInt(x._1))
              :: new JField("color", new JInt(x._2))
              :: Nil
            )
          )     
    )
        
        
    output.write(serResults.getBytes());
		*/
      
    //pi_example(sc, 10)
    //graph_example01(sc)
    //graph_example02(sc)
    
    //triangle_graph_coloring_LF(sc)
    
  }
  
  def pi_example(sc: SparkContext, NUM_SAMPLES: Int) {
    printf("Attempting to calculate PI with %d samples\n", NUM_SAMPLES)
    
    val count = sc.parallelize(1 to NUM_SAMPLES).map{i =>
      val x = Math.random()
      val y = Math.random()
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)
    
    val pi = 4.0 * count / NUM_SAMPLES    
    println("Pi is roughly " + pi)  
  }

  def graph_example01(sc: SparkContext) {
    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
                           (5L, ("franklin", "prof")), (2L, ("istoica", "prof")),
                           (4L, ("peter", "student"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
                           Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi"),
                           Edge(4L, 0L, "student"),   Edge(5L, 0L, "colleague")))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)
    // Notice that there is a user 0 (for which we have no information) connected to users
    // 4 (peter) and 5 (franklin).
    graph.triplets.map(
        triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
      ).collect.foreach(println(_))
    // Remove missing vertices as well as the edges to connected to them
    val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")
    // The valid subgraph will disconnect users 4 and 5 by removing user 0
    validGraph.vertices.collect.foreach(println(_))
    validGraph.triplets.map(
        triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
      ).collect.foreach(println(_))
  }



  def graph_example02(sc: SparkContext) 
  {        
    println("\ngraph_example02")
    
    // Create a graph with "age" as the vertex property.  Here we use a random graph for simplicity.
    val graph =
      GraphUtilities.GenerateGraph[Double,Int](sc, 100, 0.5, 1)
   
      
   println("graph generated successfully")
      
      //GraphGenerators.logNormalGraph(sc, numVertices = 100).mapVertices( (id, _) => id.toDouble )
      
    // Compute the number of older followers and their total age
    val olderFollowers: VertexRDD[(Int, Double)] = graph.aggregateMessages[(Int, Double)](
      triplet => { // Map Function
        if (triplet.srcAttr > triplet.dstAttr) {
          // Send message to destination vertex containing counter and age
          triplet.sendToDst(1, triplet.srcAttr)
        }
      },
      // Add counter and age
      (a, b) => (a._1 + b._1, a._2 + b._2) // Reduce Function
    )
    // Divide total age by number of older followers to get average age of older followers
    val avgAgeOfOlderFollowers: VertexRDD[Double] =
      olderFollowers.mapValues( (id, value) => value match { case (count, totalAge) => totalAge / count } )
    // Display the results
    avgAgeOfOlderFollowers.collect.foreach(println(_))
  }

  def triangle_graph_coloring_LF(sc: SparkContext)
  {
    println("\n\ntriagle_graph_coloring_LF")
    def genV():Int = { 0 }
    
    // Create an RDD for the vertices
    val vertices: RDD[(VertexId, Int)] =
      sc.parallelize(Array(
          (0L, genV()), (1L, genV()),
          (2L, genV())
          ))
          
    def genE(): Int = 0
                           
    // Create an RDD for edges
    val edges: RDD[Edge[Int]] =
      sc.parallelize(Array(
                           Edge(0L, 2L, genE()), Edge(1L, 2L, genE())
                           //Edge(1L, 2L, genE())
                           ))
                       
    // construct Graph
    var graph = Graph[Int,Int](vertices, edges)
    
    // construct LF Greedy Graph Coloring Algorithm Instance
    var alg = new IDO()
    
    // apply coloring algorithm
    var coloring = alg.apply(graph)
    
    println("\nColoring after applying " + alg.getClass().getName())
    coloring.foreach(x => {
      println("\t" + x._1 + ": " + x._2);      
    });
  }  
  



}
