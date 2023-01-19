/**
 * 
 */
package de.jakob_kroemer;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import org.apache.spark.api.java.function.DoubleFunction;

/**
 * @author jferisk
 *
 */
public class Q8 implements Serializable{
	
	JavaRDD<String> logData;
	
	/**
	Ermittle ob und wie das Trinkgeld von der Uhrzeit abhängt.
	*/
	public Q8(JavaRDD<String> logData) {
		this.logData = logData;
		System.out.println("Berechne Q7");
	};
	
	public void calcResult() {	
		
	PairFunction<String, Integer, Double> pair = new PairFunction<String, Integer, Double>() {

        public Tuple2<Integer, Double> call(String s) {
	        String[] attributes = s.split(",");
	        int minutes = Math.round(Integer.parseInt(attributes[7])/60);
	       	return new Tuple2(minutes, Double.parseDouble(attributes[attributes.length-3]));               
        }
	};
	    	
    JavaPairRDD<Integer, Double> pairs = logData.mapToPair(pair);
    //count each values per key
    JavaPairRDD<Integer, Tuple2<Double, Integer>> valueCount = pairs.mapValues(value -> new Tuple2<Double,Integer>(value,1));

    //add values by reduceByKey
    JavaPairRDD<Integer, Tuple2<Double, Integer>> reducedCount = valueCount.reduceByKey((tuple1,tuple2) ->  new Tuple2<Double, Integer>(tuple1._1 + tuple2._1, tuple1._2 + tuple2._2));
    for(int i =0; i < reducedCount.collect().size(); ++i) {
	System.out.println("Anzahl Daten:"+reducedCount.collect().get(i)._1 + " :: " +reducedCount.collect().get(i)._2._2); 
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
    for(int i =0; i < avgTip.collect().size(); ++i) {
    	System.out.println("Durchschnitt Trinkgeld: "+avgTip.collect().get(i)._1 + " :: " +avgTip.collect().get(i)._2); 
    	}
		System.out.print("Q7 Done!");
	}
	
}

