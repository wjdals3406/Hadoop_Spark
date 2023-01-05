import java.io.IOException;
import java.util.*;
import java.text.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class UBERStudent20191076 
{

	public static class UBERMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		private Text valueinfo = new Text();
		private Text keyinfo = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
                        String line = value.toString();
                        String[] values = line.split(",");
			String strNewDateFormat = "";
			int dayNum=0;
   		        Date nDate;

                        for(int x=0; x<values.length;x++){
				if(x==1){

					try {
						SimpleDateFormat dateFormat = new SimpleDateFormat("MM/d/yyyy");
                                        	SimpleDateFormat chdFormat = new SimpleDateFormat("yyyy-MM-dd");
                                        	Date formatDate = dateFormat.parse(values[1]);
                                        	strNewDateFormat = chdFormat.format(formatDate);
                                       		nDate = chdFormat.parse(strNewDateFormat);
                                        	Calendar cal = Calendar.getInstance();
                                        	cal.setTime(nDate);
                                        	dayNum =  cal.get(Calendar.DAY_OF_WEEK)-1;
						if(dayNum == 0) dayNum = 7;
    
					}catch (ParseException e) {
						System.out.println(e.getMessage());
					}

                                String newkey = values[0] + "," + dayNum;
				keyinfo.set(newkey);
				valueinfo.set(values[3] + "," +  values[2]);
                                context.write(keyinfo, valueinfo);
				}
                        }
                }
	}

	public static class UBERReducer extends Reducer<Text,Text,Text,Text> 
	{
		private Text sumWritable = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			int sumtrip = 0;
			int sumvehicle = 0;
			String[] outputValue;
		
			String line = key.toString();
			String[] key_values = line.split(",");
			for (Text val : values) {
				outputValue = val.toString().split(",");
				sumtrip += Integer.parseInt(outputValue[0]);
				sumvehicle += Integer.parseInt(outputValue[1]);
				
			}
			switch(key_values[1]){
							case "1":
									key_values[1] = "MON";
									break;
							case "2":
									key_values[1] = "TUE";
									break;
							case "3":
									key_values[1] = "WED";
									break;
							case "4":
									key_values[1] = "THR";
									break;
							case "5":
									key_values[1] = "FRI";
									break;
							case "6":
									key_values[1] = "SAT";
									break;
							case "7":
									key_values[1] = "SUN";
									break;
					}

			key.set(key_values[0] + "," + key_values[1]);
			
			sumWritable.set(sumtrip + "," + sumvehicle);
			context.write(key, sumWritable);
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) 
		{
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "UBER");
		job.setJarByClass(UBERStudent20191076.class);
		job.setMapperClass(UBERMapper.class);
		job.setCombinerClass(UBERReducer.class);
		job.setReducerClass(UBERReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}	

}
