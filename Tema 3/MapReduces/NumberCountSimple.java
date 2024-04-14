import java.io.IOException;
import java.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;

public class NumberCountSimple {

	public static boolean isNumeric(String value) {
		try {
			Integer.parseInt(value);
			return true;
		} catch (NumberFormatException ex) {
		}
		return false;
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text variable, Context context)
				throws IOException, InterruptedException {
			String line = variable.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				if (isNumeric(word.toString())) {
					context.write(word, variable);
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		private Text word = new Text();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String data = "";
			for (Text val : values) {
				data += val.toString() + "\r\n";
			}
			word.set(data);
			context.write(key, word);
		}
	}

	public static void main(String[] args) throws Exception {

		if (args.length < 2) {
			System.out.println("NumberCountSimple <nameFile> <outDir>");
			ToolRunner.printGenericCommandUsage(System.out);
			return;
		}
		Job job = Job.getInstance();
		job.setJarByClass(NumberCountSimple.class);
		job.setJobName("NumberCountSimple");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
