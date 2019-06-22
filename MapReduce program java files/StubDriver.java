import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class StubDriver {

  public static void main(String[] args) throws Exception {

    if (args.length != 3) {
      System.out.printf("Usage: StubDriver <sales-dir> <output dir> <promotion-dir>\n");
      System.exit(-1);
    }

    Job job = new Job();
    
    job.setJarByClass(StubDriver.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job,new Path(args[1]));
    
    job.setMapperClass(SalesMapper.class);
    job.setReducerClass(CommonReducer.class);
    
    DistributedCache.addCacheFile(new Path(args[2]).toUri(), job.getConfiguration());
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);

    job.setJobName("Case Study MR");

    boolean success = job.waitForCompletion(true);
    System.exit(success ? 0 : 1);
  }
}

