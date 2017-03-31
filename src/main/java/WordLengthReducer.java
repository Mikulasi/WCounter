import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class WordLengthReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    private ArrayList<Integer> tokenList = new ArrayList<>();

    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        for (LongWritable value : values) {
            tokenList.add((int) value.get());
        }
        Collections.sort(tokenList);
        int size = tokenList.size();
        int maxValue = tokenList.get(size - 1);
        context.write(key, new LongWritable(maxValue));
    }
}
