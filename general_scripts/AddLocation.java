
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;

public class AddLocation {
	
	public static class AddLocationMapper
	extends Mapper<LongWritable, Text, Text, Text>{
		
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
			
			String line = value.toString();
			String cleaned = line.trim();
			String [] elements = cleaned.split(",");
			int size = elements.length;
			
			String mykey = elements[0];
			
			String outputstring = elements[3] + "," + elements[4];
			if(mykey != "STATION") {
				
			
			context.write(new Text(mykey + "," + outputstring), new Text(""));
			}
		}
	}
	
	public static class AddLocationReducer extends Reducer<Text, Text, Text, Text> {
		@Override
	    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			
	    	context.write(new Text(key), new Text(""));
	    }
	 }
	
	 public static void main(String[] args) throws Exception {
	 if (args.length != 2) {
	 System.err.println("Usage: MaxTemperature <input path> <output path>");
	 System.exit(-1);
	 }

	 Job job = new Job();
	 job.setJarByClass(AddLocation.class);
	 job.setJobName("Business Statistic");
	 FileInputFormat.addInputPath(job, new Path(args[0]));
	 FileOutputFormat.setOutputPath(job, new Path(args[1]));
	 job.setNumReduceTasks(1);
	 job.setMapperClass(AddLocationMapper.class);
	 job.setReducerClass(AddLocationReducer.class);
	 job.setOutputKeyClass(Text.class);
	 job.setOutputValueClass(Text.class);

	 System.exit(job.waitForCompletion(true) ? 0 : 1);
	 }
}

