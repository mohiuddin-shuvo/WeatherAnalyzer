package mapred;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
 

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CalculateMaxMinTemperatureReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		 
    //Variables to aid the join process
    //private String customerName,deliveryReport;
    /*Map to store Delivery Codes and Messages
    Key being the status code and vale being the status message*/
	//private static 
    /*public void configure(JobConf job)
    {
           //To load the Delivery Codes and Messages into a hash map
           loadDeliveryStatusCodes();
          
    }*/
	static double roundOff(double x, int position)
    {
		double a = x;
        double temp = Math.pow(10.0, position);
        a *= temp;
        a = Math.round(a);
        return (a / (double)temp);
    }
	public String getMonthName(int month)
	{
		switch(month)
		{
			case 1: 
				return "January";
				
			case 2: 
				return "February";
				 
			case 3: 
				return "March";
				 
			case 4: 
				return "April";
				 
			case 5: 
				return "May";
				 
			case 6: 
				return "June";
				 
			case 7: 
				return "July";
				 
			case 8: 
				return "August";
				 
			case 9: 
				return "September";
				 
			case 10: 
				return "October";
				 
			case 11: 
				return "November";
				 
			case 12: 
				return "December";
				 
			default:
				return "Invalid month"+month;
		
		}
	}
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
	{
    	Map<Integer,RecordByMonth> readingByMonth= new HashMap<Integer,RecordByMonth>(); 
    	   

        while (values.hasNext())
        {
             String currValue = values.next().toString();
             
             //String splitarray[] = currValue.split("\\s+");
             
             
             //if(splitarray.length>1)
            	 //currValue =  splitarray[1];
             
             String valueSplitted[] = currValue.split("\\+");
 	  
        	 for(int i=0;i+3<valueSplitted.length;i+=3){
             
        		 ///System.out.println("******************************** " + valueSplitted[i]);
	             int month =Integer.parseInt(valueSplitted[i]);
	        	 double temp =Double.parseDouble(valueSplitted[i+1]);
	        	 double dewp = Double.parseDouble(valueSplitted[i+2]);
	        	 if(!readingByMonth.containsKey(month))
	        	 {
	        		 RecordByMonth record= new RecordByMonth();
	        		 record.temp = temp;
	        		 record.dewp = dewp;
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
	        		 record.temp = (record.temp*(record.count-1)+ temp)/record.count;
	        		 record.dewp = (record.dewp*(record.count-1) + dewp)/record.count;
	        		 
	        		 readingByMonth.put(month, record);
	        	 }
            	 
        	 }
              
        }
        
        //pump final output to file
        //String outputValue= "";
        int Max_Month=0, Min_Month=0;
        Double Max_Month_Value = Double.MIN_VALUE, Min_Month_Value = Double.MAX_VALUE;
        for(Integer month: readingByMonth.keySet())
        {
        	RecordByMonth record = readingByMonth.get(month);
        	 if(record.temp>Max_Month_Value)
        	 {
        		 Max_Month_Value=record.temp;
        		 Max_Month = month;
        	 }
        		 
        	 if(record.temp<Min_Month_Value)
        	 {
        		 Min_Month_Value=record.temp;
        		 Min_Month = month;
        	 }
        }
         
		output.collect(key, new Text( roundOff(Max_Month_Value, 2) + ","+getMonthName(Max_Month)+" "+ roundOff(Min_Month_Value, 2)+","+getMonthName(Min_Month)+" "+(roundOff(Max_Month_Value-Min_Month_Value, 2))));
        

           
	}
   
   
  
}