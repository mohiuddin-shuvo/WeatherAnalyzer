package mapred;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class SortStateReducer extends MapReduceBase implements Reducer<DoubleWritable, Text, Text, Text> {

	   
   
    private static Map<Integer,RecordByMonth> readingByMonth= new HashMap<Integer,RecordByMonth>();
   
    /*public void configure(JobConf job)
    {
           //To load the Delivery Codes and Messages into a hash map
           loadDeliveryStatusCodes();
          
    }*/


    public void reduce(DoubleWritable key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
    {
	    //boolean found = false;
	    String state = "";
	    while (values.hasNext())
	    {
	    	String currValue = values.next().toString();
	    	String splitarray[] = currValue.split("\\s+");
	    	 
	        if(splitarray.length==3){
	       	 
	        	output.collect( new Text(splitarray[0]), new Text(splitarray[1]+" "+splitarray[2]+" "+key.toString()));
	         //Key = splitarray[0];
	        }  
	    }
	        
	        //pump final output to file
         
        
    }
   
   
  
}
