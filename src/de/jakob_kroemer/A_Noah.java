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
		System.out.println("Berechne Q9");
	}
	
	public void calcResult() {
		
		//create pairs with key: passengerNumber values: totalTips
	    PairFunction<String, Integer, Double> keyData = new PairFunction<String, Integer, Double>() {
	    	public Tuple2<Integer, Double> call(String s) {
	    		String[] attributes = s.split(",");
	    		return new Tuple2(Integer.parseInt(attributes[6]), Double.parseDouble(attributes[attributes.length-3]));
	    	}
	    };        
	    		
	    JavaPairRDD<Integer,Double> pairs = logData.mapToPair(keyData);
	    
	    //count each values per key
	    JavaPairRDD<Integer, Tuple2<Double, Integer>> valueCount = pairs.mapValues(value -> new Tuple2<Double,Integer>(value,1));

	    //add values by reduceByKey
	    JavaPairRDD<Integer, Tuple2<Double, Integer>> reducedCount = valueCount.reduceByKey((tuple1,tuple2) ->  new Tuple2<Double, Integer>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2));
	    
	    //print count
	    for(int i =0; i < reducedCount.collect().size(); ++i) {
	    	System.out.println(reducedCount.collect().get(i)._1 + " :: " +reducedCount.collect().get(i)._2._2); 
    	}
	    
	    //calculate average
	    PairFunction<Tuple2<Integer, Tuple2<Double, Integer>>,Integer,Double> getAverageByKey = (tuple) -> {
	    	Tuple2<Double, Integer> val = tuple._2;
	    	double total = val._1;
	    	double count = val._2;
	    	Tuple2<Integer, Double> avgTip = new Tuple2<Integer, Double>(tuple._1, total / count);
	    	return avgTip;
	    };
	    
	    JavaPairRDD<Integer, Double> avgTip = reducedCount.mapToPair(getAverageByKey);
	    
	    //print average
	    for(int i =0; i < avgTip.collect().size(); ++i) {
	    	System.out.println(avgTip.collect().get(i)._1 + " :: " +avgTip.collect().get(i)._2); 
	    }
	    System.out.print("Q9 Done!");
	}
}
