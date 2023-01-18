package de.jakob_kroemer;
import org.apache.spark.api.java.*;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.DoubleFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Main {
	public static void main(String[] args) {
		
		String file1 = "/home/osboxes/data/Aufgabe2/trip_data/trip_data_1.csv"; // Should be some file on your system
		String file2 = "/home/osboxes/data/Aufgabe2/trip_fare/trip_fare_1.csv";
		JavaRDD output = Merge.merge(file1,file2);
		
		Q2 Q2 = new Q2(output);
		Q2.calcResult();
		
		Q6 Q6 = new Q6(output);
		Q6.calcResult();
		
		A_Noah Q9 = new A_Noah(output);
		Q9.calcResult();
		
		Q4 Q4 = new Q4(output);
		Q4.calcResult();
		
	}
}
