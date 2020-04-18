import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.*;

import static java.lang.Long.parseLong;

public class MapReduce1 {
    public static class Map extends Mapper<LongWritable, Text, Text, Data> {

        @Override
        //                   KEYIN key, VALUEIN value, Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context context
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            String[] fileNameArr = fileName.split("-");
            String date = fileNameArr[1];
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line, " \t\n\r\f");

            while (tokenizer.hasMoreTokens()) {
                //projectcode, pagename, pageviews, and bytes
                String projectName = tokenizer.nextToken();
                String title = tokenizer.nextToken();
                String numberOfRequests = tokenizer.nextToken();
                String sizeOfReturnContent = tokenizer.nextToken();
                if (!projectName.equals("en")) {
                    continue;
                }
                title = URLDecoder.decode(title, "UTF-8");
                boolean flag = false;

                List<String> startWith = new ArrayList<>(Arrays.asList(new String[]{"Media", "Special", "Talk", "User", "User_talk", "Project", "Project_talk", "File", "File_talk", "MediaWiki", "MediaWiki_talk", "Template", "Template_talk", "Help", "Help_talk", "Category", "Category_talk", "Portal", "Wikipedia", "Wikipedia_talk"}));
                for (String s : startWith) {
                    if (title.startsWith(s)) {
                        flag = true;
                    }
                }
                if (flag == true) {
                    continue;
                }

                if (Character.isLowerCase(title.charAt(0))) {
                    flag = true;
                }
                if (flag == true) {
                    continue;
                }

                List<String> endWith = new ArrayList<String>(Arrays.asList(new String[]{".jpg", ".gif", ".png", ".JPG", ".GIF", ".PNG", ".ico", ".txt"}));
                for (String s : endWith) {
                    if (title.endsWith(s)) {
                        flag = true;
                    }
                }
                if (flag == true) {
                    continue;
                }

                List<String> contain = new ArrayList<String>(Arrays.asList(new String[]{"404_error", "Main_Page", "Hypertext_Transfer_Protocol", "Favicon.ico", "Search"}));
                for (String s : contain) {
                    if (title.contains(s)) {
                        flag = true;
                    }
                }
                if (flag == true) {
                    continue;
                }

                Data data = new Data(date, numberOfRequests);
                context.write(new Text(title), data);
            }
        }
    }

    //                      Modern_art}20160601 50
    public static class Reduce extends Reducer<Text, Data, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Data> data, Context context) throws IOException, InterruptedException {
            StringBuilder dateSb = new StringBuilder("\t[");
            StringBuilder pageViewSb = new StringBuilder("\t[");
            long pageViewSum = 0L;
            long popularityTrend = 0L;

            //add all the thing for a same key up
            PriorityQueue<String> dateHeap = new PriorityQueue<String>();
            PriorityQueue<String> viewHeap = new PriorityQueue<String>();
            for (Data curData : data) {
                // Modern_art\t[20160601,20160604]\t[50,75]\t125\t25
                String date = curData.date;
                String pageView = curData.pageView;

                dateHeap.add(date);
                viewHeap.add(pageView);
                pageViewSum += parseLong(pageView);

                // 345 - 12
                if (date.endsWith("1") || date.endsWith("2")) {
                    popularityTrend -= parseLong(pageView);
                } else {
                    popularityTrend += parseLong(pageView);
                }
            }
            while (!dateHeap.isEmpty()) {
                dateSb.append(dateHeap.poll()).append(",");
            }
            while (!viewHeap.isEmpty()) {
                pageViewSb.append(viewHeap.poll()).append(",");
            }

            dateSb.setLength(dateSb.length() - 1); //remove the end comma
            dateSb.append("]");

            pageViewSb.setLength(pageViewSb.length() - 1); //remove the end comma
            pageViewSb.append("]");

            context.write(null, new Text(key.toString().trim()
                    + "\t" + dateSb.toString().trim()
                    + "\t" + pageViewSb.toString().trim()
                    + "\t" + pageViewSum
                    + "\t" + popularityTrend));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "job111");
        job.setJarByClass(MapReduce1.class);
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