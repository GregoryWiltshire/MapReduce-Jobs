import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.commons.lang.StringEscapeUtils;
import java.util.HashMap;

public class TweeterWordCount {

	public static String handle;
	
	public static class TweeterWordCountMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
				//Framework does the reading for you...
            
            String line = value.toString();      //a line of the csv file

            //create a list of strings delimited by space followed by commma followed by space
            List<String> lineList = new ArrayList<String>(Arrays.asList(string.split(" , ")));

                   
			//get the first element, the username
			String username = lineList.get(0);

			//if the username is not the one we are looking for or the Handle header skip
			if(username.equals("Handle")||!username.equals(inputName)){
				return; //skip
			}

			String tweet = lineList.get(1);


			//do a little bit of tweet adjustment
			String cleanedTweet = tweet.replaceAll("((?:http|https)(?::\\/{2}[\\w]+)(?:[\\/|\\.]?)(?:[^\\s\"]*))", "URLHERE");
			cleanedTweet = cleanedTweet.replaceAll("'", "");
			cleanedTweet = cleanedTweet.replaceAll("#", "");
			cleanedTweet = cleanedTweet.replaceAll("@", "");
			cleanedTweet = cleanedTweet.replaceAll("[0-9]", "NUMHERE");
			cleanedTweet = cleanedTweet.replaceAll("[^a-zA-Z]+", " ");
			

			//create some tokens and count
			StringTokenizer itr = new StringTokenizer(txt);
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}

			result.set(sum);
			context.write(key, result);

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: TweeterWordCount <input> <UserHandle>");
			System.exit(2);
		}
		Job job = new Job(conf, "Twitter Handle Word Count");
		job.setJarByClass(TweeterWordCount.class);
		job.setMapperClass(TweeterWordCountMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		//take in user handle
		String handle = (otherArgs[2]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}