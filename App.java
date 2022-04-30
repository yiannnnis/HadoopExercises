/* Ioannis Papavasilopoulos 2022201800141 dit18141@go.uop.gr
Ioannis Papachristou 2022201800146 dit18146@go.uop.gr*/
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

public class App {
    
        public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\John\\Desktop\\DATA2\\personality_analysis.csv"));
        BufferedWriter bw = new BufferedWriter(new FileWriter("C:\\Users\\John\\Desktop\\DATA2\\edited.csv"));
        String line = "";
        while ((line = br.readLine()) != null) {
            String[] values = line.split(";", -1); 
            // make an array out of line
            String writableString = ""; 
            //initial string which will be the final output for the row
            ArrayList al = new ArrayList(); 
            // use array list because can edit array and modify size easily
            for (String element : values) {
                if (element==null || element.length()==0) {
                    al.add("0");
                } 
                else {
                    al.add(element);
                }
               
        }
            String[] array = (String[]) al.toArray(new String[0]);
            for (String s : array){ // add commas between each element of arraylist
                writableString += s + ";";
            }

            writableString = writableString.substring(0, // remove last commas
            writableString.length() - 9);
            bw.write(writableString + "\n"); 
            
            //writes the line and carriage return
        }
        br.close();
        bw.close();
        }
        
}
