package mapred;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class ReadingsMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
{
    //variables to process Consumer Details
    private String cellNumber,customerName,fileTag="CD~";
   
    /* map method that process ConsumerDetails.txt and frames the initial key value pairs
       Key(Text) – mobile number
       Value(Text) – An identifier to indicate the source of input(using ‘CD’ for the customer details file) + Customer Name
     */
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
    {
       //taking one line/record at a time and parsing them into key value pairs
        String line = value.toString();
        String splitarray[] = line.split("\\s+");
        //cellNumber = splitarray[0].trim();
        //customerName = splitarray[1].trim();
        String stn, month, temp, dewp; 
        if(splitarray.length>=4)
        {
        	stn = splitarray[0].trim();
        	if(!stn.equals("STN---"))
        	{
	        	
	        	Integer stnid = Integer.parseInt(stn);
	        	
	        	month = splitarray[2].trim().substring(4, 6);
	        	temp = splitarray[3].trim();
	        	dewp = splitarray[5].trim();
	        	
	        		output.collect(new Text(stnid.toString()), new Text("Readings+"+month+"+"+temp+"+"+dewp));
	    		 
	        		//customerName = splitarray[1].trim();
	        	
        	}
        }
        
      //sending the key value pair out of mapper
        //output.collect(new Text(cellNumber), new Text(fileTag+customerName));
     }
}