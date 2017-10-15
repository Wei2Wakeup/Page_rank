import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UnitMultiplication {

    public static class TransitionMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //input format: fromPage\t toPage1,toPage2,toPage3
            String line = value.toString().trim();
            String[] fromto = line.split("\t");

            if(fromto.length==1 || fromto[1].trim().equals("") ){
                return;
            }
            //target: build transition matrix unit -> fromPage\t toPage=probability
            String fromPage = fromto[0].trim();
            String[] toPages = fromto[1].trim().split(",");
            double prob = (double)1/ toPages.length;
            for(String str : toPages){
                context.write(new Text(fromPage),new Text(str+"="+prob));
            }
        }
    }

    public static class PRMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //input format: Page\t PageRank
            String line = value.toString().trim();
            String[] page_rank = line.split("\t");
            //Double pr = Double.parseDouble(page_rank[1]);
            //target: write to reducer
            context.write(new Text(page_rank[0]), new Text(page_rank[1]));
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {
        
        float beta;
        @Override
        public void setup(Context context){
            Configuration conf = context.getConfiguration();
            beta = conf.getFloat("beta",0.8f);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //input key = fromPage value=<toPage=probability..., pageRank>
            List<String> list = new ArrayList<>();
            double pr = 0;
            for( Text value : values){
                String cur =  value.toString().trim();
                if(cur.contains("=")){
                    list.add(cur);
                }else{
                    pr =  Double.parseDouble(cur);
                }
            }
            //target: get the unit multiplication

            for(String str : list){
                String[] to_prob = str.split("=");
                String outputKey  =  to_prob[0].trim();
                double mult = Double.parseDouble(to_prob[1])*pr*beta;
                context.write(new Text(outputKey),new Text(String.valueOf(mult)));

                //context.getCounter("UnitPr",String.valueOf(beta)).increment(1);
            }
            
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setFloat("beta",Float.parseFloat(args[3]));
        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitMultiplication.class);

        //how chain two mapper classes?
        //ChainMapper.addMapper(job,TransitionMapper.class,Object.class, Text.class, Text.class, Text.class,conf);
        //ChainMapper.addMapper(job,PRMapper.class,Object.class, Text.class, Text.class, Text.class,conf);


        job.setReducerClass(MultiplicationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }

}
