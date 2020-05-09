package JobThree;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobThreeDriver {

    public static void main(String[] args) throws Exception {

        //Job 1 Join
        Configuration joinConf = new Configuration();
        joinConf.set("mapreduce.output.textoutputformat.separator", ";");
        Job joinJob = Job.getInstance(joinConf, "JoinJob");
        joinJob.setJarByClass(JoinMR.class);
        joinJob.setReducerClass(JoinMR.ReducerJoin.class);
        joinJob.setOutputKeyClass(Text.class);
        joinJob.setOutputValueClass(Text.class);

        //per il join path multipli
        MultipleInputs.addInputPath(joinJob, new Path(args[0]), TextInputFormat.class, JoinMR.MapperHistoricalStockPrices.class);
        MultipleInputs.addInputPath(joinJob, new Path(args[1]), TextInputFormat.class, JoinMR.MapperHistoricalStocks.class);
        //path di output del primo job, che è poi input del secondo
        Path joinOutputPath = new Path(args[2] + "/joinresult/");

        FileOutputFormat.setOutputPath(joinJob, joinOutputPath);
        //outputPath.getFileSystem(conf).delete(outputPath);
        boolean joinSuccess = joinJob.waitForCompletion(true);
        if (!joinSuccess) {
            System.out.println("Join Job failed, exiting");
        }

        //Job 2 Mean for year and company name
        Configuration meanConf = new Configuration();
        meanConf.set("mapreduce.output.textoutputformat.separator", ";");
        Job meanJob = Job.getInstance(meanConf, "MeanJob");
        meanJob.setJarByClass(MeanByName.class);
        meanJob.setMapperClass(MeanByName.MapperMean.class);
        meanJob.setReducerClass(MeanByName.ReducerMean.class);
        meanJob.setOutputKeyClass(Text.class);
        meanJob.setOutputValueClass(Text.class);

        Path meanOutputPath = new Path(args[2] + "/meanresult/");

        FileInputFormat.addInputPath(meanJob, joinOutputPath);
        FileOutputFormat.setOutputPath(meanJob, meanOutputPath);

        boolean meanSuccess = meanJob.waitForCompletion(true);
        if (!meanSuccess) {
            System.out.println("Mean Job failed, exiting");
        }

        //Job 3 Final result - Companies with the same yearly trends for 2016-17-18

        Configuration finalConf = new Configuration();
        finalConf.set("mapreduce.output.textoutputformat.separator", " ");
        Job finalJob = Job.getInstance(finalConf, "FinalJob");
        finalJob.setJarByClass(CompaniesCombiner.class);
        finalJob.setMapperClass(CompaniesCombiner.MapperCombiner.class);
        finalJob.setReducerClass(CompaniesCombiner.ReducerCombiner.class);
        finalJob.setOutputKeyClass(Text.class);
        finalJob.setOutputValueClass(Text.class);

        Path finalOutputPath = new Path(args[2] + "/finalresult/");

        FileInputFormat.addInputPath(finalJob, meanOutputPath);
        FileOutputFormat.setOutputPath(finalJob, finalOutputPath);

        boolean finalSuccess = finalJob.waitForCompletion(true);
        if (!finalSuccess) {
            System.out.println("Final Job failed, exiting");
        }

        return;
    }
}
