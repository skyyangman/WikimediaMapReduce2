import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.PriorityQueue;
import java.util.StringTokenizer;

/**
 * Top 100 item ranked by pop trend in descending order, format: Modern_art\t25\n
 */
public class MapReduce3 {
    public static class Map extends Mapper<LongWritable, Text, NullWritable, FooAndTitle> {
        private Text word = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line, " \t\n\r\f");
            //  Modern_art\t[20160601,20160604]\t[50,75]\t125\t25 -> Modern_art\t125\n
            PriorityQueue<FooAndTitle> heap = new PriorityQueue<>(new Comp());
            while (tokenizer.hasMoreTokens()) {
                String title = tokenizer.nextToken();
                String date = tokenizer.nextToken();
                String view = tokenizer.nextToken();
                String totalView = tokenizer.nextToken();
                String pop = tokenizer.nextToken();

                heap.offer(new FooAndTitle(pop, title)); ////////add pop
                if (heap.size() > 100) {
                    heap.poll();
                }
            }

            for (FooAndTitle foo : heap) {
                context.write(NullWritable.get(), foo);
            }
        }
    }

    public static class Reduce extends Reducer<NullWritable, FooAndTitle, Text, String> {

        @Override
        protected void reduce(NullWritable key, Iterable<FooAndTitle> value, Context context) throws IOException, InterruptedException {
            PriorityQueue<FooAndTitle> heap = new PriorityQueue<>(new Comp());
            for (FooAndTitle foo : value) {
                heap.offer(new FooAndTitle(foo.foo, foo.title));
                if (heap.size() > 100) {
                    heap.poll();
                }
            }
            int size = heap.size();
            for (int i = 0; i < size; i++) {
                FooAndTitle tmp = heap.poll();
                context.write(null, tmp.title + "\t" + tmp.foo);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "job333");
        job.setJarByClass(MapReduce3.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(FooAndTitle.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        System.out.println(success);
    }
}