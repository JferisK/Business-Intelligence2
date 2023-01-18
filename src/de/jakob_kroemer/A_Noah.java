/**
 * 
 */
package de.jakob_kroemer;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * @author osboxes
 *
 */
public class A_Noah implements Serializable{

	private JavaRDD<String> logData;

	/**
	Ermittle ob und wie das Trinkgeld von der Fahrtdauer abhängt.
	*/
	public A_Noah(JavaRDD<String> logData) {
		this.logData = logData;
		System.out.println("Brechne Q9");
	}
	
	public void calcResult() {
		//create pairs with key: passengerNumber values: tipAmount
	    PairFunction<String, String, String> keyData = 
	    		new PairFunction<String, String, String>() {
	    			public Tuple2<String, String> call(String s) {
	    				String[] attributes = s.split(",");
	    								 //    key			 , 				value
	    				return new Tuple2(attributes[7], attributes[attributes.length-3]);
	    			}
	    		};        
	    		
	    JavaPairRDD<String,String> pairs = logData.mapToPair(keyData);
	    
	    //group by passengerNumber
	    JavaPairRDD<String, Iterable<String>> pTraveler = pairs.groupByKey();
	    
	    //count number of fairs passengers
	    Function<Iterable<String>,Integer> count = new Function<Iterable<String>,Integer>(){
	    	public Integer call(Iterable<String> valuesForKey) {
	    		int counter = 0;
	    		for(String i : valuesForKey)counter++;
	    		return counter;
	    	}
	    };
	    
	    JavaPairRDD<String, Integer> countOfTravelers = pTraveler.mapValues(count);
		for(int i = 0; i < countOfTravelers.collect().size(); ++i) {
			System.out.println(countOfTravelers.collect().get(i)._1 + " :: " + countOfTravelers.collect().get(i)._2);
		}
	    
		//get average of tips per passengerNumber
	    Function<Iterable<String>, Float> average = new Function<Iterable<String>, Float>() {
		      public Float call(Iterable<String> valuesForKey) {
		    	  float tips = 0;
		    	  int counter = 0;
		    	  for (String i : valuesForKey) {
		    		  tips += Float.parseFloat(i);
		    		  counter++;
		    	  }
		    	  float avg = tips/counter;
		    	  return avg;
		    }
		};
		
	    JavaPairRDD<String, Float> avgTip = pTraveler.mapValues(average);
		for(int i = 0; i < avgTip.collect().size(); ++i) {
			System.out.println(avgTip.collect().get(i)._1 + " :: " + avgTip.collect().get(i)._2);
		}	
	    //JavaRDD<String> lines = sc.parallelize(Arrays.asList("Lines with a: " + numAs + ", lines with b: " + numBs));
	    //lines.saveAsTextFile("/home/osboxes/spark_output.txt");
	}
}
