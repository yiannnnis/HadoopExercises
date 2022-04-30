/* Ioannis Papavasilopoulos 2022201800141 dit18141@go.uop.gr
Ioannis Papachristou 2022201800146 dit18146@go.uop.gr*/

package org.myorg;
import java.io.FileWriter;
import java.io.IOException;
import java.io.BufferedReader;
import java.util.StringTokenizer;
import java.io.FileNotFoundException;
import java.io.FileReader;
import org.apache.hadoop.io.NullWritable;

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
          for(int j=0; j<8; j++){
              defaultTokenizer.nextToken();
          }
          ++c;
          int MntWines = Integer.parseInt(defaultTokenizer.nextToken());
          int MntFruits = Integer.parseInt(defaultTokenizer.nextToken());
          int MntMeatProducts = Integer.parseInt(defaultTokenizer.nextToken());
          int MntFishProducts = Integer.parseInt(defaultTokenizer.nextToken());
          int MntSweetProducts = Integer.parseInt(defaultTokenizer.nextToken());
          int MntGoldProds = Integer.parseInt(defaultTokenizer.nextToken());
          arr[i]= MntWines + MntFruits + MntMeatProducts + MntFishProducts + MntSweetProducts + MntGoldProds;
          sum=arr[i]+sum;
          //System.out.println(arr[i]);        
          i++;
      }
      String temp = f.toString();
      StringTokenizer Tokenizer2 = new StringTokenizer(temp, " ");
      String id = Tokenizer2.nextToken();
      String Income = Tokenizer2.nextToken();

      String Date = Tokenizer2.nextToken();
      StringTokenizer Tokenizer3 = new StringTokenizer(Date, "/");
      for(int j=0; j<2; j++){
        Tokenizer3.nextToken();
      }
      String year = Tokenizer3.nextToken();

      int total = 0;
      for(int j=0; j<6; j++){
        total = total + Integer.parseInt(Tokenizer2.nextToken());
      }

      int am = Integer.parseInt(Income);
      int yi = Integer.parseInt(year);
      avg=sum/c;
      
     
    
      if (total >((avg/2)+avg) && yi==21 && am>=69500)
      {
        context.write(new Text("gold"), new IntWritable(Integer.parseInt(id)));
      }

      if (total >((avg/2)+avg) && yi<21 && am>=69500)
      {
        context.write(new Text("silver"), new IntWritable(Integer.parseInt(id)));
      }
      
  }

    




}