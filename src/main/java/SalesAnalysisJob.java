import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import sales.TransactionAnalysis.TransactionData;
import sales.TransactionAnalysis.TransactionMapper;
import sales.TransactionAnalysis.TransactionReducer;
import sort.Data;
import sort.Mapper;
import sort.Reducer;

public class SalesAnalysisJob {
    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: hadoop jar /tmp/sales-analytics-1.0-SNAPSHOT.jar SalesAnalysisJob <input path> <final output path> <REDUCERS_COUNT=1> <DATABLOCK_SIZE_KB=1>");
            System.exit(-1);
        }

        String inputDir = args[0];
        String outputDir = args[1];
        int reducersCount = Integer.parseInt(args[2]);
        int datablockSizeMb = Integer.parseInt(args[3]) * ((int) Math.pow(2, 10)); // * 1 kb
        String intermediateResultDir = outputDir + "-intermediate";

        long startTime = System.currentTimeMillis();
        Configuration conf = new Configuration();
        conf.set("mapreduce.input.fileinputformat.split.maxsize", Integer.toString(datablockSizeMb));

        Job salesAnalysisJob = Job.getInstance(conf, "sales analysis");
        salesAnalysisJob.setNumReduceTasks(reducersCount);
        salesAnalysisJob.setJarByClass(SalesAnalysisJob.class);
        salesAnalysisJob.setMapperClass(TransactionMapper.class);
        salesAnalysisJob.setReducerClass(TransactionReducer.class);
        salesAnalysisJob.setMapOutputKeyClass(Text.class);
        salesAnalysisJob.setMapOutputValueClass(TransactionData.class);
        salesAnalysisJob.setOutputKeyClass(Text.class);
        salesAnalysisJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(salesAnalysisJob, new Path(inputDir));
        Path intermediateOutput = new Path(intermediateResultDir);
        FileOutputFormat.setOutputPath(salesAnalysisJob, intermediateOutput);

        boolean success = salesAnalysisJob.waitForCompletion(true);

        if (!success) {
            System.exit(1);
        }

        Job sortByValueJob = Job.getInstance(conf, "sorting by revenue");
        sortByValueJob.setJarByClass(SalesAnalysisJob.class);
        sortByValueJob.setMapperClass(Mapper.class);
        sortByValueJob.setReducerClass(Reducer.class);

        sortByValueJob.setMapOutputKeyClass(DoubleWritable.class);
        sortByValueJob.setMapOutputValueClass(Data.class);

        sortByValueJob.setOutputKeyClass(Data.class);
        sortByValueJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(sortByValueJob, intermediateOutput);
        FileOutputFormat.setOutputPath(sortByValueJob, new Path(outputDir));

        long endTime = System.currentTimeMillis();
        System.out.println("Jobs completed in " + (endTime - startTime) + " milliseconds.");

        System.exit(sortByValueJob.waitForCompletion(true) ? 0 : 1);
    }
}
