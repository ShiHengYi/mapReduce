package Lab1;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.StringTokenizer;

public class MRmapper2  extends Mapper <LongWritable,Text,Text,Text> {
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {

	// TODO: write (key, value) pair to context (hint: need to be clever here)
		String[] iter = value.toString().split("\\s+");
		
		String name = iter[0];
		String num = iter[1];
		
		context.write(new Text("summary"), new Text(name + "_" + num));
	}
}
