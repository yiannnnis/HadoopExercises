/* Ioannis Papavasilopoulos 2022201800141 dit18141@go.uop.gr
Ioannis Papachristou 2022201800146 dit18146@go.uop.gr*/
package org.myorg;
import java.io.FileWriter;
import java.io.IOException;
import java.io.BufferedReader;
import java.util.StringTokenizer;
import java.io.FileNotFoundException;
import java.io.FileReader;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FrequencyReducer extends Reducer<Text ,  IntWritable ,  Text ,  IntWritable > {
     @Override public void reduce(Text f,  Iterable<IntWritable> counts,  Context context)
         throws IOException,  InterruptedException, NumberFormatException {
      
      /*int sum  = 0;
      int c=0;
      for ( IntWritable count  : counts) {
        
        sum  += Integer.parseInt(MntWines.toString());
        
      }*/

      int[] arr;
      arr=new int[2241];
      int i=0;
      BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\John\\Desktop\\DATA2\\er2\\final.txt"));
      String line = "";
      int sum=0;
      int c=0;
      int avg;
      while ((line = br.readLine()) != null) {
          StringTokenizer defaultTokenizer = new StringTokenizer(line, ";");
          for(int j=0; j<9; j++){
              defaultTokenizer.nextToken();
          }
          ++c;
          arr[i]=Integer.parseInt(defaultTokenizer.nextToken());
          sum=arr[i]+sum;
          //System.out.println(arr[i]);        
          i++;
      }
      String temp = f.toString();
      StringTokenizer Tokenizer2 = new StringTokenizer(temp, " ");
      for(int j=0; j<6; j++){
        Tokenizer2.nextToken();
      }

      String MntWines = Tokenizer2.nextToken();
        

      int am = Integer.parseInt(MntWines);
      avg=sum/c;
      if (am >((avg/2)+avg))
      {
        context.write(f, new IntWritable(avg));
      }
      
  }

    




}