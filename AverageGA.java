import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.countingJob;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.commons.lang.StringEscapeUtils;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays; 


//so far the first mapper/reducer will output a key/val of users/postcount

public class AverageGA {
	public static String handle;


//second mapper, needs to take in the profiles to map the GA users to their postcount
	public static class TweeterGAMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

            
            String line = value.toString();      //a line of the csv file

            //create a list of strings delimited by space followed by commma followed by space
            ArrayList<String> lineList = new ArrayList<String>(Arrays.asList(line.split(",")));

                   
			//get the first element, the username, trim it down
			String username = lineList.get(0);
			username = username.substring(1, username.length()-1);

			Text usernameText = new Text(username);


			//map the username to one
			context.write(usernameText, one);

		}
	}

	
	//simple first mapper to take the tweets and tally words per tweet (handle, words)
	public static class TweeterPostWordCountMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
				//Framework does the reading for you...

					//System.out.println(handle);
			String line = value.toString();
			ArrayList<String> lineList = new ArrayList<String>(Arrays.asList(line.split(",")));

			//get the first element, the username, trim it down
			String username = lineList.get(0);
			username = username.substring(1, username.length()-1);
			Text usernameText = new Text(username);


			String tweet = lineList.get(1);
			//gets the tweet string

			//do a little bit of tweet adjustment
			String cleanedTweet = tweet.replaceAll("((?:http|https)(?::\\/{2}[\\w]+)(?:[\\/|\\.]?)(?:[^\\s\"]*))", "URLHERE");
			cleanedTweet = cleanedTweet.replaceAll("'", "");
			cleanedTweet = cleanedTweet.replaceAll("#", "");
			cleanedTweet = cleanedTweet.replaceAll("@", "");
			cleanedTweet = cleanedTweet.replaceAll("[0-9]", "NUMHERE");
			cleanedTweet = cleanedTweet.replaceAll("[^a-zA-Z]+", " ");
			

			//create some tokens and send them away for the reducer to count
			StringTokenizer itr = new StringTokenizer(cleanedTweet);
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(usernameText, one);

			}

		}
	}

//simple first mapper to take the tweets and tally words per tweet (handle, words)
	public static class TweeterPostCountMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
				//Framework does the reading for you...

					//System.out.println(handle);
			String line = value.toString();
			ArrayList<String> lineList = new ArrayList<String>(Arrays.asList(line.split(",")));

			//get the first element, the username, trim it down
			String username = lineList.get(0);
			username = username.substring(1, username.length()-1);
			Text usernameText = new Text(username);

			context.write(usernameText, one);

			

		}
	}


	//need a new reducer to reduce list containing usernames and postcount, and the users in GA
	public static class avgReducer
          extends Reducer<Text,IntWritable,Text,FloatWritable> {
      private FloatWritable result = new FloatWritable();
      Float average = 0f;
      Float count = 0f;
      int sum = 0;
      public void reduce(Text key, Iterable<IntWritable> values,
                         Context context
      ) throws IOException, InterruptedException {
  
          Text sumText = new Text("average");
          for (IntWritable val : values) {
              sum += val.get();
  
          }
          count += 1;
          average = sum/count;
          result.set(average);
          System.out.println(average);
          context.write(sumText, result);
      }
  }


	//should be no changes to the sum reducer
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
		if (otherArgs.length != 3) {
			System.err.println("Usage: AverageGA <tweets> <profiles> <output>");
			System.exit(2);
		}

		Path postInput = new Path(otherArgs[0]);
		Path userInput = new Path(otherArgs[1]);
		Path outputDirIntermediate = new Path(otherArgs[2] + "_int");
		Path outputDirIntermediate2 = new Path(otherArgs[2] + "_int2");
		Path outputDir = new Path(otherArgs[2]);



		Job countingJob = new Job(conf, "Words per handle");
		countingJob.setJarByClass(AverageGA.class);
		//set the first mapper class
		countingJob.setMapperClass(TweeterPostWordCountMapper.class);
		countingJob.setCombinerClass(IntSumReducer.class);
		countingJob.setReducerClass(IntSumReducer.class);
		countingJob.setOutputKeyClass(Text.class);
		countingJob.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(countingJob, new Path(postInput);
		//FileOutputFormat.setOutputPath(countingJob, new Path(otherArgs[1]));

		//set the output of the first counter job to an intermediate text
		TextOutputFormat.setOutputPath(countingJob, outputDirIntermediate);

		

		//job to count the number of posts/user, uses the same reducer as previous
		Job countingJob2 = new Job(conf, "Words per handle");
		countingJob2.setJarByClass(AverageGA.class);
		countingJob2.setMapperClass(TweeterPostCountMapper.class);
		countingJob2.setCombinerClass(IntSumReducer.class);
		countingJob2.setReducerClass(IntSumReducer.class);
		countingJob2.setOutputKeyClass(Text.class);
		countingJob2.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(countingJob2, new Path(postInput);
		TextOutputFormat.setOutputPath(countingJob2, outputDirIntermediate2);


		Job avgJob = new Job(conf, "average words/tweet");
		avgJob.setJarByClass(AverageGA.class);
		avgJob.setMapperClass(TweeterPostCountMapper.class);
		avgJob.setCombinerClass(avgReducer.class);
		avgJob.setReducerClass(IntSumReducer.class);
		avgJob.setOutputKeyClass(Text.class);
		avgJob.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(avgJob, outputDirIntermediate;
		TextOutputFormat.setOutputPath(countingJob2, outputDir);
		//average is the output


		

		System.exit(countingJob.waitForCompletion(true) ? 0 : 1);
	}
}