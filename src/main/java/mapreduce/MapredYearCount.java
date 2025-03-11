package mapreduce;

import java.io.IOException;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.avro.*;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapred.*;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import classes.avro.spotify;


public class MapredYearCount extends Configured implements Tool {

  public static class YearCountMapper extends AvroMapper<spotify, Pair<Integer, Integer>> {
    /**
     * Maps an input record to a set of intermediate key-value pairs.
     * @param spotify The input record
     * @param collector The collector to which the output key-value pairs are written
     * @param reporter Facility to report progress
     * @throws IOException
     */
    @Override
    public void map(spotify spotify_record, 
                    AvroCollector<Pair<Integer, Integer>> collector, 
                    Reporter reporter)

        throws IOException {

      Integer year_of_release = spotify_record.getYearOfRelease();

      if (year_of_release == null){
        year_of_release = 0;
      }

      collector.collect(new Pair<Integer, Integer>(year_of_release, 1));
      }
  }

  public static class YearCountReducer extends AvroReducer<Integer, Integer, Pair<Integer, Integer>> {
  
    /**
     * Reduce the values for a key to a single output value.
     * @param key The key for which the values are being passed.
     * @param values The values for the given key.
     * @param collector The collector to which the output should be written.
     * @param reporter Facility to report progress.
     * @throws IOException
     */
    @Override
    public void reduce(Integer key, Iterable<Integer> values, AvroCollector<Pair<Integer, Integer>> collector, Reporter reporter)
      throws IOException {
        int sum = 0;
        for (Integer value : values) {
          sum += value;
        }
        collector.collect(new Pair<Integer, Integer>(key, sum));
      }
  }

   /**
   * The run() method is called (indirectly) from main(), and contains all the job
   * configuration and Hadoop job submission.
   * @param args The command line arguments
   * @return 0 if the Hadoop job completes successfully, 1 if not
   */
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: MapredYearCount <input path> <output path>");
      return -1;
    }

    JobConf conf = new JobConf(getConf(), MapredYearCount.class);
    conf.setJobName("yearcount");

    // If Output directory already exists, delete it
    Path outputPath = new Path(args[1]);
    outputPath.getFileSystem(conf).delete(outputPath, true);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    AvroJob.setMapperClass(conf, YearCountMapper.class);
    AvroJob.setReducerClass(conf, YearCountReducer.class);

    // Note that AvroJob.setInputSchema and AvroJob.setOutputSchema set
    // relevant config options such as input/output format, map output
    // classes, and output key class.
    AvroJob.setInputSchema(conf, spotify.getClassSchema());
    AvroJob.setOutputSchema(conf, Pair.getPairSchema(Schema.create(Type.INT), Schema.create(Type.INT)));

    JobClient.runJob(conf);
    return 0;
  }

  /**
   * The main method specifies the Hadoop job configuration and starts the job.
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new MapredYearCount(), args);

    if (res == 0) {
      File outputDir = new File(args[1]);
      File[] outputFiles = outputDir.listFiles();
      for (File outputFile : outputFiles) {
        if (outputFile.getName().endsWith(".avro")) {
          String textName = outputFile.getName().replace(".avro", ".txt");
          List<String> records = DeserializationData.getRecords(outputFile.getAbsolutePath(), "int");
          File textFile = new File(outputFile.getParent(), textName);
          FileUtils.writeLines(textFile, records);
        }
      }
      System.out.println("Job executed successfully");
    } else {
      System.out.println("Job failed");
    }

    System.exit(res);
  }


}