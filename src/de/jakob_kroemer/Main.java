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
	
		Q6 Q6 = new Q6();
		Q6.calcResult();
	}
}