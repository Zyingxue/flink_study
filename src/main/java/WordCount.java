import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.util.Collector;

/**
 * @ClassName:
 * @Discription:
 */
//批处理
public class WordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String inputPath =  "D:\\i学习\\bilibili\\code\\flinkDemo\\src\\main\\resources\\hello.txt";
        DataSource<String> inputData = env.readTextFile(inputPath);
        DataSet<Tuple2<String, Integer>> result = inputData.flatMap(new MyFlatMapper())
                .groupBy(0) //按照第一个位置的word分组
                .sum(1); //按照第二个位置的num计数
        result.print();
    }
    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>>{
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = s.split(" ");
            for (String word : words) {
                collector.collect(new Tuple2<String, Integer>(word,1));
            }
        }
    }
}
