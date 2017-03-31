import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordLengthJob {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();

        if (args.length != 2) {
            System.err.println("Usage: WordLengthJob <input path> <output path>");
            System.exit(-1);
        }

        Path outputDir = new Path(args[1]);
        Job job = Job.getInstance(configuration);
        job.setJarByClass(WordLengthJob.class);
        job.setJobName("WordLengthMapper with Counter");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setMapperClass(WordLengthMapper.class);
        job.setReducerClass(WordLengthReducer.class);
        job.setNumReduceTasks(1);
        job.waitForCompletion(true);
        CounterGroup counters = job.getCounters().getGroup(WordLengthMapper.WORD.WORD_LENGTH.name());
        for (Counter counter : counters) {
            System.out.println(counter.getDisplayName() + ":" + counter.getValue());
        }
    }
}
