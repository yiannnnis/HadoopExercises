/* Ioannis Papavasilopoulos 2022201800141 dit18141@go.uop.gr
Ioannis Papachristou 2022201800146 dit18146@go.uop.gr*/
package org.myorg;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FrequencyReducer extends Reducer<Text ,  IntWritable ,  Text ,  IntWritable > {
     @Override public void reduce( Text Education,  Iterable<IntWritable> counts,  Context context)
         throws IOException,  InterruptedException {

      int sum  = 0;
      for ( IntWritable count  : counts) {
        sum  += count.get();
      }
      context.write(Education,  new IntWritable(sum));
    }
}