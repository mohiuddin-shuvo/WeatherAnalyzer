package mapred;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class StationMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
{

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
    {
    	String line = value.toString();
        String splitarray[] = line.split("\",\"");

        String stn, state; 
        if(splitarray.length>=4)
        {
        	stn = splitarray[0].trim();
        	stn = stn.substring(1);
        	if(!stn.equals("USAF"))
        	{
	        	if(Character.isLetter(stn.charAt(0)))
	        		stn = stn.substring(1);
	        	 
	        	
	        	Integer stnid = Integer.parseInt(stn);
	        	state = splitarray[4].trim();
	        	
        		if(splitarray[3].trim().equals("US"))
        		{
        		    if(!"".equals(state))
        		    	output.collect(new Text(stnid.toString()), new Text("Station+"+state));
        		    
        		}
	        }
        }
     }
}