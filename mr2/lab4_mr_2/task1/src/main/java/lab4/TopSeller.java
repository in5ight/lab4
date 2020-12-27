package lab4;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopSeller {
    public static class Mapper0 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String name = fileSplit.getPath().getName();

            String line = value.toString();
            if (line == null || line.equals(""))
                return;

            String[] split = line.split(",", -1);

            if (name.contains("user_log")) {
                String user = split[0];
                String item = split[1];
                String cat = split[2];
                String seller = split[3];
                String brand = split[4];
                String time = split[5];
                String action = split[6];
                context.write(new Text(user),
                        new Text("#" + item + "," + cat + "," + seller + "," + brand + "," + time + "," + action));
            } else if (name.contains("user_info")) {
                String user = split[0];
                String age = split[1];
                String gender = split[2];
                context.write(new Text(user), new Text("$" + age + "," + gender));
            }
        }
    }

    public static class Reducer0 extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<String> list1 = new LinkedList<>();
            List<String> list2 = new LinkedList<>();

            for (Text text : values) {
                String value = text.toString();
                if (value.startsWith("#")) {
                    value = value.substring(1);
                    list1.add(value);

                } else if (value.startsWith("$")) {
                    value = value.substring(1);
                    list2.add(value);
                }
            }

            for (String a : list1) {
                for (String b : list2) {
                    context.write(key, new Text(a + "," + b));
                }
            }
        }
    }

    static class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] splits = line.split("\t", -1);
            String info = splits[1];
            String[] splits2 = info.split(",", -1);
            String seller = splits2[2];
            String age = splits2[6];
            String action = splits2[5];
            if (age.equals("1") || age.equals("2") || age.equals("3")) {
                context.write(new Text(seller), new Text(action));
            }
        }
    }

    static class Reducer1 extends Reducer<Text, Text, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (Text val : values) {
                if (!"0".equals(val.toString())) {
                    sum += 1;
                }
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class Mapper2 extends Mapper<LongWritable, Text, IntWritable, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String str[] = value.toString().split("\t");
            context.write(new IntWritable(Integer.valueOf(str[1])), new Text(str[0]));
        }
    }

    public static class Reducer2 extends Reducer<IntWritable, Text, Text, IntWritable> {
        static int sortCount = 1;

        private Text result = new Text();

        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text val : values) {
                if (sortCount > 100) {
                    break;
                }
                result.set(Integer.toString(sortCount) + ": " + val + ", ");
                context.write(result, key);
                sortCount = sortCount + 1;
            }
        }
    }

    public static class DescComparator extends WritableComparator {

        protected DescComparator() {
            super(IntWritable.class, true);
        }

        @Override
        public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3, int arg4, int arg5) {
            return -super.compare(arg0, arg1, arg2, arg3, arg4, arg5);
        }

        @Override
        public int compare(Object a, Object b) {
            return -super.compare(a, b);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job0 = Job.getInstance(conf);
        job0.setJarByClass(TopSeller.class);
        job0.setMapperClass(Mapper0.class);
        job0.setReducerClass(Reducer0.class);
        job0.setOutputKeyClass(Text.class);
        job0.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job0, new Path("input"));
        FileOutputFormat.setOutputPath(job0, new Path("output_mr2_1"));

        boolean res0 = job0.waitForCompletion(true);

        Job job1 = Job.getInstance(conf);
        job1.setJarByClass(TopSeller.class);
        job1.setMapperClass(Mapper1.class);
        job1.setReducerClass(Reducer1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job1, new Path("output_mr2_1"));
        FileOutputFormat.setOutputPath(job1, new Path("output_mr2_2"));

        boolean res1 = job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf);
        job2.setJarByClass(TopSeller.class);
        job2.setMapperClass(Mapper2.class);
        job2.setReducerClass(Reducer2.class);
        job2.setSortComparatorClass(DescComparator.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path("output_mr2_2"));
        FileOutputFormat.setOutputPath(job2, new Path("output_mr2_3"));

        boolean res2 = job2.waitForCompletion(true);

        if (res0 == true && res1 == true && res2 == true) {
            System.exit(0);
        } else {
            System.exit(1);
        }
    }
}