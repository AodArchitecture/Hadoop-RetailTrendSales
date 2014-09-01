package com.nielsen.hadoop.retailsales;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class RetailMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

  
  public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {
      String line = value.toString();
      String[] splits = line.split("\t");
      String store = splits[0];
      String upc = splits[1];
      String salesValue = splits[4];
      
      context.write(new Text(store+"-"+upc), new DoubleWritable(Double.parseDouble(salesValue)));

    }
  }  



