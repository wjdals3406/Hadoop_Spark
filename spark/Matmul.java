import scala.Tuple2;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public final class SparkMatmul implements Serializable {
	public static void main(String[] args) throws Exception {
		if (args.length< 2) {
			System.err.println("Usage: MatrixMultiplication<in-file> <out-file>");
			System.exit(1);
		}
		SparkSessionspark = SparkSession
		.builder()
		.appName("MatrixMultiplication")
		.getOrCreate();

	JavaRDD<String> mat1 = spark.read().textFile(args[0]).javaRDD();
	JavaRDD<String> mat2 = spark.read().textFile(args[1]).javaRDD();
	int m = Integer.parseInt(args[2]); //row
	int k = Integer.parseInt(args[3]); 
	int n = Integer.parseInt(args[4]); //column

	JavaPairRDD<String, Integer> m1elements = mat1.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
		public Iterator<Tuple2<String, Integer>> call(String s) {
			ArrayList<Tuple2<String, Integer>> elements = new ArrayList<Tuple2<String, Integer>>();
			
			String[] val = s.split(" ");
			for (int i= 0; i< n; i++){
				elements.add(new Tuple2(val[0] + " " + i + " " + val[1]), Integer.parseInt(val[2]));
			}
			return elements.iterator();
			}
		});

	JavaPairRDD<String, Integer> m2elements = mat2.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
		public Iterator<Tuple2<String, Integer>> call(String s) {
			ArrayList<Tuple2<String, Integer>> elements = new ArrayList<Tuple2<String, Integer>>();

			for (int i= 0; i< m; i++)
				elements.add(new Tuple2(val[0] + " " + i + " " + val[1]), Integer.parseInt(val[2]));
			return elements.iterator();

			}
		});
	JavaPairRDD<String, Integer> elements = m1elements.union(m2elements);
	JavaPairRDD<String, Integer> mul= elements.reduceByKey( new Function2<Integer, String, Integer>(){
		public Integer call(Integer x, Integer y){
			return x * y;
		}
	});

	JavaPairRDD<String, Integer> changeKey= mul.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
		public Tuple2<String, Integer> call(Tuple2<String, Integer> tp) {
			String[] idx = tp._1.split(" ");
			return new Tuple2(idx[0] + " " + idx[1], tp._2);
		}
	});

	JavaPairRDD<String, Integer> rst= changeKey.reduceByKey(new Function2<Integer, Integer, Integer>() {
		public Integer call(Integer x, Integer y){
			return x + y;
		}
	});
	rst.saveAsTextFile(args[args.length-1]);
	spark.stop();
	}
}