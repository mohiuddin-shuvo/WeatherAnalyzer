package mapred;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class CalculateMinMaxTemperatureMapper  extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
{
    //variables to process Consumer Details
     
   
    /* map method that process ConsumerDetails.txt and frames the initial key value pairs
       Key(Text) – mobile number
       Value(Text) – An identifier to indicate the source of input(using ‘CD’ for the customer details file) + Customer Name
     */
	
	
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
    {
    	//String line = value.toString();
        //String splitarray[] = line.split("\\+");
         
        
        //Text state = new Text(splitarray[0]);
        //Text readings = new Text(line.substring(state.getLength()+1));
    	String currValue = value.toString();
    	String splitarray[] = currValue.split("\\s+", 2);
        //String currValue = "";
    	String Key = "";
        if(splitarray.length>1){
       	 currValue =  splitarray[1];
         Key = splitarray[0];
        }
        output.collect(new Text(Key), new Text(currValue)); 
		  
        
     }
}