package mapred;

import java.io.*;
import java.util.HashMap;

import org.apache.hadoop.io.*;


public class MapperOutputValue implements Writable {
       // Some data     
    public Text docid;
    public IntWritable freq;
    public Text indexType;
    public MapperOutputValue(){}
    public MapperOutputValue(String d, int f, String s){

    	docid = new Text(d);
        freq = new IntWritable(f);
        indexType =new Text(s);
    }

    @Override
    public void write(DataOutput out) throws IOException {
    	docid.write(out);   	
    	freq.write(out);
    	indexType.write(out);
      //out.writeInt(freq);
      //out.writeUTF(docid);
      //out.writeUTF(indexType);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
     // docid = in.readUTF();
     // freq = in.readInt(); 
     // indexType = in.readUTF();
    	if(docid!=null)
    		docid.readFields(in); 
    	if(freq!=null)
    		freq.readFields(in);
    	if(indexType!=null)
    		indexType.readFields(in);
    }

  /*  public static MapperOutputValue read(DataInput in) throws IOException {
      MapperOutputValue w = new MapperOutputValue();
      w.readFields(in);
      return w;
    }*/
}