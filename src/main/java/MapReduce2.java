import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

import static java.lang.Long.*;

public class MapReduce2 {
    public static class Map extends Mapper<LongWritable, Text, Text, Data> {
        private Text word = new Text();

        @Override
        //                   KEYIN key, VALUEIN value, Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line, " \t\n\r\f\"}");
            // * Modern_art\t[20160601,20160604]\t[50,75]\t125\t25
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken()); //set word to Modern_art
                String date = tokenizer.nextToken();
                String pageView = tokenizer.nextToken();
                Data data = new Data(date, pageView);
                context.write(word, data);
            }
        }
    }

    public static class Reduce extends Reducer<Text, Data, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Data> data, Context context) throws IOException, InterruptedException {
            StringBuilder dateSb = new StringBuilder("\\t[");
            StringBuilder pageViewSb = new StringBuilder("\\t[");
            long pageViewSum = 0L;
            long popularityTrend = 0L;

            //add all the thing for a same key up
            for (Data curData : data) {
                // Modern_art\t[20160601,20160604]\t[50,75]\t125\t25
                String date = curData.date;
                String pageView = curData.pageView;

                dateSb.append(date).append(",");
                pageViewSb.append(pageView).append(",");

                pageViewSum += parseLong(pageView);

                // 345 - 12
                if (date.endsWith("1") || date.endsWith("2")) {
                    popularityTrend -= parseLong(pageView);
                } else {
                    popularityTrend += parseLong(pageView);
                }
            }
            dateSb.setLength(dateSb.length() - 1); //remove the end comma
            dateSb.append("]");

            pageViewSb.setLength(pageViewSb.length() - 1); //remove the end comma
            pageViewSb.append("]");

            context.write(key, new Text(String.valueOf(dateSb) + pageViewSb + "\\t" + pageViewSum + "\\t" + popularityTrend));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "wordcount");
        job.setJarByClass(MapReduce2.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Data.class);
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