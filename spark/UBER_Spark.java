import scala.Tuple2;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import java.io.Serializable;
import java.util.*;
import java.text.*;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public final class UBERStudent20191076 implements Serializable {
	public static void main(String[] args) throws Exception {
		if (args.length< 2) {
			System.err.println("Usage: UBER<in-file> <out-file>");
			System.exit(1);
		}
		SparkSession spark = SparkSession
		.builder()
		.appName("UBER")
		.getOrCreate();

	JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

	JavaPairRDD<String, String> flatmapfun = new PairFlatMapFunction<String, String, String>(){
		public Iterator<Tuple2<String, String>> call(String s) {
			ArrayList<Tuple2<String, String>> elements = new ArrayList<Tuple2<String, String>>();
			String[] values = s.split(",");
			String strNewDateFormat = "";
			int dayNum=0;
   		    Date nDate;
           
			try {
				String[] week = {"MON","TUE","WED","THR","FRI","SAT","SUN"};
				SimpleDateFormat dateFormat = new SimpleDateFormat("MM/d/yyyy");
				SimpleDateFormat chdFormat = new SimpleDateFormat("yyyy-MM-dd");
				Date formatDate = dateFormat.parse(values[1]);
				strNewDateFormat = chdFormat.format(formatDate);
				nDate = chdFormat.parse(strNewDateFormat);
				Calendar cal = Calendar.getInstance();
				cal.setTime(nDate);
				dayNum =  cal.get(Calendar.DAY_OF_WEEK)-1;
				if(dayNum == 0) dayNum = 7;
				values[1] = week[dayNum-1];

			}catch (ParseException e) {
				System.out.println(e.getMessage());
			}
			
			elements.add(new Tuple2(values[0] + " " + values[1], values[3] + " " + values[2]));
			return elements.Iterator;
		}
	};

	JavaPairRDD<String, String> uelements = lines.flatMapToPair(flatmapfun);

	


	PairFunction<String, String, Integer> pf = new PairFunction<String, String, Integer>() {
		public Tuple2<String, Integer> call(String s) {
			return new Tuple2(s, 1);
		}
	};

	JavaPairRDD<String, Integer> ones = words.mapToPair(pf);

	Function2<Integer, Integer, Integer> f2 = new Function2<Integer, Integer, Integer>() {
		public Integer call(Integer x, Integer y) {
			return x + y;
		}
	};
	JavaPairRDD<String, Integer> counts = ones.reduceByKey(f2);
	FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
	counts.saveAsTextFile(args[1]);
	spark.stop();
	}
}