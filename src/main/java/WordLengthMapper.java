import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordLengthMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private String longWord;

    public enum WORD {
        WORD_LENGTH
    }

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(" ");
        for (String token : tokens) {
            longWord = token;
            Counter counter = context.getCounter(WORD.WORD_LENGTH.name(), longWord);
            counter.increment(longWord.length());
            System.out.println(counter);

        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text(longWord), new LongWritable(longWord.length()));
    }
}
