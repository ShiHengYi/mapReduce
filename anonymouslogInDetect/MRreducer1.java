
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;

public class MRreducer1  extends Reducer <Text,IntWritable,Text,IntWritable> {
   public void reduce(Text key, Iterable<IntWritable> values, Context context) 
		   throws IOException, InterruptedException {
	   IntWritable result = new IntWritable();
	   
	   int sum = 0;
	   for(IntWritable val : values) {
		   sum += val.get();
	   }
	   result.set(sum);
	   context.write(key, result);
   }
}
