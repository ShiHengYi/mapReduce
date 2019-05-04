

import org.apache.hadoop.conf.Configured;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class MRdriver extends Configured implements Tool {
	   
	    Job job = Job.getInstance(getConf(), "job1");
	    
	    job.setInputFormatClass(TextInputFormat.class);
	    
	    job.setJarByClass(MRdriver.class);
	    job.setMapperClass(MRmapper1.class);
	    job.setReducerClass(MRreducer1.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class); 
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.waitForCompletion(true);
	    
	    Job job2 = Job.getInstance(getConf(), "job2");
	    job2.setJarByClass(MRdriver.class);
	    job2.setMapperClass(MRmapper2.class);
	    job2.setReducerClass(MRreducer2.class);
	    
	    job2.setInputFormatClass(TextInputFormat.class);
	    
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(FloatWritable.class); 
	    job2.setMapOutputValueClass(Text.class);
	    
	    FileInputFormat.addInputPath(job2, new Path(args[1]+"/part-r-00000"));
	    FileOutputFormat.setOutputPath(job2, new Path(args[2]));
	    job2.waitForCompletion(true);
	    
	    double sigmaBound = Double.parseDouble(args[3]);
	    File out2 = new File(args[2]+"//part-r-00000");
	    FileReader fr = new FileReader(out2);
	    BufferedReader bf = new BufferedReader(fr);
	    String l1 = bf.readLine();
	    String l2 = bf.readLine();
	    
	    String cur = bf.readLine();
	    while(cur != null) {
	    	String[] a = cur.split("\\s+");
	    	Double curSigma = Double.parseDouble(a[1]);
	    	if(curSigma > sigmaBound){
	    		System.out.println("detected anomaly for user:" + a[0] + " with score: " + a[1]);
	    	}
	    	cur = bf.readLine();
	    }
	    
	    return 0;
	    
	    
   }

   public static void main(String[] args) throws Exception { 
	   if(args.length != 4) {
		   System.err.println("usage: MRdriver <input-path> <output1-path> <output2-path> <sigma_int_threshold>");
		   System.exit(1);
	   }
	   // check sigma_int_threshold is an int
	  try {
		  Integer.parseInt(args[3]);
	  }
	  catch (NumberFormatException e) {
		  System.err.println(e.getMessage());
		  System.exit(1);
	  }
      Configuration conf = new Configuration();
      System.exit(ToolRunner.run(conf, new MRdriver(), args));
   } 
}
