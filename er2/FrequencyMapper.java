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
    String Year_Birth = line.split(";")[1];
    String Education = line.split(";")[2];
    String Marital_Status = line.split(";")[3];
    String Income = line.split(";")[4];
    int tmp_age = 2022-Integer.valueOf(Year_Birth);
    String age = Integer.toString(tmp_age);
    String MntWines = line.split(";")[9];
    String f = id + " " + Year_Birth + " " + Education + " " + Marital_Status + " " + Income + " " + age + " " + MntWines;
    context.write(new Text(f), one);
  }
}