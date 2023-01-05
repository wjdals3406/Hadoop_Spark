import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class YouTubeStudent20191076{
	public static class ReduceSumMapper extends Mapper<Object, Text, Text, DoubleWritable>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] itr = value.toString().split("\\|");
			Text outputKey = new Text();
			DoubleWritable outputValue = new DoubleWritable();
			
			String category = itr[3];
			double avg = Double.parseDouble(itr[6]);

			outputKey.set(category);
			outputValue.set(avg);
			context.write(outputKey,outputValue);
		}

	}

	public static class ReduceSumReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>
	{
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			DoubleWritable re_output = new DoubleWritable();
			double sum = 0;
			int count = 0;
                        for (DoubleWritable val : values) {
                                sum += val.get();
				count++;
                                }
                        re_output.set(sum/count);
                        context.write(key, re_output);
		}
	}
	public static class Rating {
		public double rating;
		public String title;
                public Rating(String _title, double _rating) {
                        this.title = _title;
			this.rating = _rating;
                }

                public String getString()
                {
                        return title + "\t" + rating;
                }
        }

	public static class RatingComparator implements Comparator<Rating> {

                public int compare(Rating x, Rating y) {
                        if ( x.rating > y.rating ) return 1;
                        if ( x.rating < y.rating ) return -1;
                        return 0;
                }
        }

        public static void insertRating(PriorityQueue q, String title, double rating, int topK) {
               
	       	Rating rating_head = (Rating) q.peek();
                if ( q.size() < topK || rating_head.rating < rating )
                {
                        Rating rat = new Rating(title, rating);
                        q.add( rat );
                        if( q.size() > topK ) q.remove();
                }
        }

	public static class TopKMapper extends Mapper<Object, Text, Text, NullWritable> {
                private PriorityQueue<Rating> queue ;
                private Comparator<Rating> comp = new RatingComparator();
                private int topK;
                public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), "\t");
			String title =  itr.nextToken().trim();
                        double rating = Double.parseDouble( itr.nextToken().trim() );
                        insertRating(queue, title,rating, topK);
		}

                protected void setup(Context context) throws IOException, InterruptedException {
                        Configuration conf = context.getConfiguration();
                        topK = conf.getInt("topK", -1);
                        queue = new PriorityQueue<Rating>( topK , comp);
                }
		protected void cleanup(Context context) throws IOException, InterruptedException {
                        while( queue.size() != 0 ) {
                                Rating rating = (Rating) queue.remove();
                                context.write( new Text( rating.getString() ), NullWritable.get() );
                        }
                }
        }
	public static class TopKReducer extends Reducer<Text,NullWritable,Text,NullWritable> {
                private PriorityQueue<Rating> queue ;
                private Comparator<Rating> comp = new RatingComparator();
                private int topK;
                public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
                        StringTokenizer itr = new StringTokenizer(key.toString(), "\t");
                        String title = itr.nextToken().trim();
                        double rating = Double.parseDouble( itr.nextToken().trim());
                        insertRating(queue, title, rating, topK);
	       	}

                protected void setup(Context context) throws IOException, InterruptedException {
                        Configuration conf = context.getConfiguration();
                        topK = conf.getInt("topK", -1);
                        queue = new PriorityQueue<Rating>( topK , comp);
                }
		protected void cleanup(Context context) throws IOException, InterruptedException {
                        while( queue.size() != 0 ) {
                                Rating rating = (Rating) queue.remove();
                                context.write( new Text( rating.getString() ), NullWritable.get() );
                        }
                }
        }

	public static void main(String[] args) throws Exception {
		String first_phase_result = "/first_phase_result";
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
                int topK = Integer.parseInt(otherArgs[2]);
		if (otherArgs.length != 3)
		{
			System.err.println("Usage: TopK <in> <out>");
			System.exit(2);
		}
		conf.setInt("topK", topK);
		Job job1 = new Job(conf, "SUM");
		job1.setJarByClass(YouTubeStudent20191076.class);
		job1.setMapperClass(ReduceSumMapper.class);
		job1.setReducerClass(ReduceSumReducer.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(DoubleWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, new Path(first_phase_result));
		FileSystem.get(job1.getConfiguration()).delete( new Path(first_phase_result), true);
		job1.waitForCompletion(true);

		Job job2 = new Job(conf, "TopK");
		job2.setJarByClass(YouTubeStudent20191076.class);
                job2.setMapperClass(TopKMapper.class);
                job2.setReducerClass(TopKReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job2, new Path(first_phase_result));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));
		FileSystem.get(job2.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}	

}


