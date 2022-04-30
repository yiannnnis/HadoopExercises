/* Ioannis Papavasilopoulos 2022201800141 dit18141@go.uop.gr
Ioannis Papachristou 2022201800146 dit18146@go.uop.gr*/

package org.myorg;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class FrequencyMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {

  private final static IntWritable one = new IntWritable(1);

  @Override
  public void map(LongWritable offset, Text lineText, Context context)
      throws IOException, InterruptedException {

    String line = lineText.toString();
    String id = line.split(";")[0];
    String Income = line.split(";")[4];
    String Dt_Customer = line.split(";")[7];
    String MntWines = line.split(";")[9];
    String MntFruits = line.split(";")[10];
    String MntMeatProducts = line.split(";")[11];
    String MntFishProducts = line.split(";")[12];
    String MntSweetProducts = line.split(";")[13];
    String MntGoldProds = line.split(";")[14];
    String f = id + " " + Income + " " + Dt_Customer + " " + MntWines + " " + MntFruits + " " + MntMeatProducts + " " + MntFishProducts + " " + MntSweetProducts+ " " + MntGoldProds;
    context.write(new Text(f), one);
  }
}