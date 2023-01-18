package de.jakob_kroemer;
import org.apache.spark.api.java.*;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;

public class Main {
	public static void main(String[] args) {
		
		String logFile = "/home/osboxes/data/Aufgabe2/NY_medium2_UTF8.csv"; 
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Simple Application");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> logData = sc.textFile(logFile);
		
		Q2 Q2 = new Q2(logData);
		Q2.calcResult();
		
		Q6 Q6 = new Q6(logData);
		Q6.calcResult();
		
		A_Noah Q9 = new A_Noah(logData);
		Q9.calcResult();
		
		Q4 Q4 = new Q4(logData);
		Q4.calcResult();
		
	}
}
