package Lab1;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;

public class MRreducer2  extends Reducer <Text,Text,Text,DoubleWritable> {
   public void reduce(Text key, Iterable<Text> values, Context context) 
		   throws IOException, InterruptedException {
	// TODO: parse out (key, values) (based on hint of cleverness mapper)
	   double num =0;
	   double total=0;
	   // find num elements and total fail time
	   // get sigma
	   // get each Z value
	   
	   Iterator in = values.iterator(); 
	   String result = "";
	   while(in.hasNext()) {
		   String[] single = in.next().toString().split("_");
		   total += Double.parseDouble(single[1]);
		   num++;
		   result += single[0] + "_" + single[1] + "\n";
	   }
	      
	// TODO: calculate mean_failed_login_attempts and write to context
	   double mean = total/num;
	   context.write(new Text("mean_failed_login_attempts:"), new DoubleWritable (mean));
	   
	// TODO: calculate sigma_failed_login_attempts and write to context
	   double sigma = 0;   
	   Scanner in2 = new Scanner(result);
	   while(in2.hasNextLine()) {
		   String[] single = in2.nextLine().split("_");
		   sigma += Math.pow((Double.parseDouble(single[1]) - mean),2) ;  
	   }
	   sigma = Math.sqrt(sigma/num);
	   context.write(new Text("sigma_failed_login_attempts:"),new DoubleWritable(sigma) );
	   
	// TODO: calculate num_sigmas_for:<user> and write to context
	   Scanner in3 = new Scanner(result);
	   while(in3.hasNextLine()) {
		   String[] single = in3.nextLine().split("_");
		   double zValue = (Double.parseDouble(single[1]) - mean) / sigma;
		   context.write(new Text(single[0]),new DoubleWritable(zValue));
	   }
	   
	   
   }
}

