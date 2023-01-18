package de.jakob_kroemer;
import org.apache.spark.api.java.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Merge {
	public static void main(String[] args) {
		
		String file1 = "/home/osboxes/data/Aufgabe2/trip_data/trip_data_1.csv"; // Should be some file on your system
		String file2 = "/home/osboxes/data/Aufgabe2/trip_fare/trip_fare_1.csv";
	    SparkConf conf = new SparkConf().setMaster("local").setAppName("Simple Application");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    JavaRDD<String> data1 = sc.textFile(file1).cache();
	    JavaRDD<String> data2 = sc.textFile(file2).cache();
	    
	    System.out.println("hey:"+data1.collect().get(1));
	    //create pairs with key: passengerNumber values: tipAmount
	    PairFunction<String, String, String> keyData = 
	    		new PairFunction<String, String, String>() {
	    			public Tuple2<String, String> call(String s) {
	    				String[] attributes = s.split(",");
	    								 //    key			 , 				value
	    				return new Tuple2(attributes[0]+":"+attributes[1], attributes[2]+","+attributes[3]+","+attributes[4]+","+attributes[5]+","
	    								 +attributes[6]+","+attributes[7]+","+attributes[8]+","+attributes[9]+","+attributes[10]+","+attributes[11]+
	    								 ","+attributes[12]+","+attributes[13]);
	    			}
	    		}; 
	    PairFunction<String, String, String> keyFare = 
	    		new PairFunction<String, String, String>() {
	    			public Tuple2<String, String> call(String s) {
	    				String[] attributes = s.split(",");
	    								 //    key			 , 				value
	    				return new Tuple2(attributes[0]+":"+attributes[1], attributes[2]+","+attributes[3]+","+attributes[4]+","+attributes[5]+","
								 +attributes[6]+","+attributes[7]+","+attributes[8]+","+attributes[9]+","+attributes[10]);
	    			}
	    		};
	    
	    		
	    JavaPairRDD<String,String> pairs1 = data1.mapToPair(keyData);
	    JavaPairRDD<String,String> pairs2 = data2.mapToPair(keyFare);
	    JavaPairRDD<String,Tuple2<String, String>>  result = pairs1.join(pairs2);
	    
	    Function flatten = new Function<Tuple2<String,Tuple2<String,String>>,String>(){
	    	public String call(Tuple2<String,Tuple2<String,String>> a) {
	    		return (a._1 +","+a._2._1+","+a._2._2);}
	    	};
	    	
	    
	    JavaRDD<String> output = result.map(flatten);
	    System.out.println(output.collect().size());
	    output.saveAsTextFile("/home/osboxes/data/spark_output_test.csv");
	    //JavaRDD<String> lines = sc.parallelize(Arrays.asList("Lines with a: " + numAs + ", lines with b: " + numBs));
	    //lines.saveAsTextFile("/home/osboxes/spark_output.txt");
	  }
}
