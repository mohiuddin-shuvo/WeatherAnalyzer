package mapred;
import java.io.*;
//import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.io.*;
import org.json.simple.JSONObject;

public class DocSumWritable implements Writable{

	private HashSet<Text> map = new HashSet<Text>();

	public DocSumWritable() {
    }

    public DocSumWritable(HashSet<Text> map){

    	this.map = map;
    }

    
	@Override
	public void readFields(DataInput in) throws IOException {
	Iterator iter = map.iterator();
        while (iter.hasNext()) {
        	Text val = ((Text)iter.next());
            val.readFields(in);
           // val.docid.readFields(in);
           // val.freq.readFields(in);
           // val.indexType.readFields(in);
        }    	
	}

	@Override
	public void write(DataOutput out) throws IOException {
            
        Iterator iter = map.iterator();
        while (iter.hasNext()) {
        	Text val = ((Text)iter.next());
            //JSONObject obj = new JSONObject();
            //obj.put("Urlid", val.docid.toString());
        	//obj.put("TermFrequency", val.freq.get());
        	//obj.put("IndexType", val.indexType.toString());
            val.write(out);
           // val.docid.write(out);
           // val.freq.write(out);
           // val.indexType.write(out);
        }    
		

	}

	@Override
    public String toString() {

        String output = "";
        Iterator<Text> iter = map.iterator();
        while (iter.hasNext()) {
        	//Text val = ;
            /*JSONObject obj = new JSONObject();
            if(val.docid!=null)
        	obj.put("Urlid", val.docid.toString());
            if(val.freq!=null)
            	obj.put("TermFrequency", val.freq.get());
            if(val.indexType!=null)
        	obj.put("IndexType", val.indexType.toString());*/
            output += iter.next().toString()+" ";
        }    	
                

        return output;

    }

}
