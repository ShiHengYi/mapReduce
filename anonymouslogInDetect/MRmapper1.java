package Lab1;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;

public class MRmapper1  extends Mapper <LongWritable,Text,Text,IntWritable> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] values = value.toString().split("\\s+");
		if(values[0].equals("type=USER_LOGIN") && values[13].equals("res=failed'") && values[8].contains("\"")){
				context.write(new Text(values[8].substring(5, values[8].length())), new IntWritable(1));
	}
	
}


}
