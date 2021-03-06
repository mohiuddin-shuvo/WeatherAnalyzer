package mapred;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class JoinReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {


       public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
	    {
    	   
    	   Map<Integer,RecordByMonth> readingByMonth= new ConcurrentHashMap<Integer,RecordByMonth>(); 
    	   
    	    //boolean found = false;
    	    String state = "";
	        while (values.hasNext())
	        {
	             String currValue = values.next().toString();
	             String valueSplitted[] = currValue.split("\\+");
	             
	             
	             if(valueSplitted[0].equals("Readings"))
	             {
	            	  
	            	 int month =Integer.parseInt(valueSplitted[1].trim());
	            	 double temp =Double.parseDouble(valueSplitted[2].trim());
	            	 double prec = Double.parseDouble(valueSplitted[3].trim());
	            	 if(!readingByMonth.containsKey(month))
	            	 {
	            		 RecordByMonth record= new RecordByMonth();
	            		 record.temp = temp;
	            		 record.prec = prec;
	            		 record.count = 1;
	            		 readingByMonth.put(month, record);
	            		 //dewpByMonth.put(month, dewp);
	            	 }
	            	 else
	            	 {
	            		 //tempByMonth.put(month, (temp+tempByMonth.get(month))/2);
	            		 //dewpByMonth.put(month, (dewp+tempByMonth.get(month))/2);
	            		 RecordByMonth record = readingByMonth.get(month);
	            		 record.count++;
	            		 record.temp = ((record.temp*(record.count-1))+ temp)/record.count;
	            		 record.prec = ((record.prec*(record.count-1)) + prec)/record.count;
	            		 readingByMonth.remove(month);
	            		 readingByMonth.put(month, record);
	            	 }
	            	 
	             }
	             else if(valueSplitted[0].equals("Station"))
	             {
	               if("".equals(state))
	            	   state = valueSplitted[1].trim();
	             
	             }
	        }
	        
	        //pump final output to file
	        String outputValue= "";
	        for(Integer month: readingByMonth.keySet())
	        {
	        	RecordByMonth record = readingByMonth.get(month);
	        	if(!outputValue.equals(""))
	        		outputValue += "+";
	        	outputValue+=month.toString()+"+"+record.temp+"+"+record.prec;
	        	
	        }
	        if(!"".equals(state) && !"".equals(outputValue))
	        		output.collect(new Text(state), new Text(outputValue));
	        
	    }
      
      
     
}