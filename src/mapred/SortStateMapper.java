package mapred;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class SortStateMapper extends MapReduceBase implements Mapper<LongWritable, Text, DoubleWritable, Text>
{
    //variables to process Consumer Details
     
   
    /* map method that process ConsumerDetails.txt and frames the initial key value pairs
       Key(Text) – mobile number
       Value(Text) – An identifier to indicate the source of input(using ‘CD’ for the customer details file) + Customer Name
     */
	
	
    public void map(LongWritable key, Text value, OutputCollector<DoubleWritable, Text> output, Reporter reporter) throws IOException
    {
    	 
    	String currValue = value.toString();
    	String splitarray[] = currValue.split("\\s+");
    	String d="";
        //String currValue = "";
    	String Key = "";
        if(splitarray.length==4){
       	 d =  splitarray[3];
       	Double d1 = Double.parseDouble(d);
       	output.collect(new DoubleWritable(d1), new Text(splitarray[0]+" "+splitarray[1]+" "+splitarray[2]));
         //Key = splitarray[0];
        }
         
		  
        
     }

}
