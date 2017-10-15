import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.text.DecimalFormat;

public class UnitSum {
    public static class PassMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //input format: toPage\t unitMultiplication
            String line = value.toString().trim();
            String[] to_mult = line.split("\t");
            //target: pass to reducer
            context.write(new Text(to_mult[0].trim()), new DoubleWritable(Double.parseDouble(to_mult[1])));
        }
    }

    public static class BetaMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        float beta;
        @Override
        public void setup(Context context){
            Configuration conf = context.getConfiguration();
            beta = conf.getFloat("beta", 0.8f);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //input format: Page\t PageRank
            String line = value.toString().trim();
            String[] page_rank = line.split("\t");
            double beta_r = Double.parseDouble(page_rank[1])*(1-beta);
            //target: write to reducer
            context.write(new Text(page_rank[0]), new DoubleWritable(beta_r));
        }
    }

    public static class SumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {



        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

           //input key = toPage value = <unitMultiplication>
            double sum = 0;
            for(DoubleWritable value : values){
                sum += value.get();
            }
            //target: sum!
            DecimalFormat df = new DecimalFormat("#.0000");
            sum = Double.valueOf(df.format(sum));
            context.write(key,new DoubleWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("beta",args[3]);
        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitSum.class);

        //job.setMapperClass(PassMapper.class);
        //ChainMapper.addMapper(job,PassMapper.class,Object.class, Text.class, Text.class, DoubleWritable.class,conf);
        //ChainMapper.addMapper(job,BetaMapper.class,Text.class, DoubleWritable.class, Text.class, DoubleWritable.class,conf);

        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        //FileInputFormat.addInputPath(job, new Path(args[0]));
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PassMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, BetaMapper.class);


        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);


        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(args[0]),true);
        fs.delete(new Path(args[1]),true);

    }
}
