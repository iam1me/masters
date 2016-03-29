package csci.ryan_williams.masters;

import csci.ryan_williams.masters.*;
import java.io.*;
import java.util.*;

/** JavaApp.java
 * 	A java wrapper for the scala application
 */
public class JavaApp {

	public static void main(String[] args) {
		
		System.out.println("JavaApp::main");
			
		
		byte[] data = testData().getBytes();		
		InputStream dataStream = new java.io.ByteArrayInputStream(data);
		
		OutputStream resultStream = System.out;
		
		/// call scala main method
		App.main(dataStream, resultStream);

	}
	
	public static String testData()
	{
		return 
			"{"
				+ "\"vertices\" : ["
					+ "{ \"id\" : 0 },"
					+ "{ \"id\" : 1 },"
					+ "{ \"id\" : 2 },"
					+ "{ \"id\" : 3 }"
				+ "],"
				+ "\"edges\" : ["
					+ "{ \"vertexIds\" : [ 0, 1] },"
					+ "{ \"vertexIds\" : [ 0, 2] },"
					+ "{ \"vertexIds\" : [ 1, 2] },"
					+ "{ \"vertexIds\" : [ 1, 3] },"
					+ "{ \"vertexIds\" : [ 2, 3] }"
				+ "]"				
			+ "}";
			
	}

}
