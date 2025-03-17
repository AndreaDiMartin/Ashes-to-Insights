package mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import classes.avro.spotify; 

public class TrackWordCount extends Configured implements Tool {

  public static class TrackMapper extends AvroMapper<spotify, Pair<String, Integer>> {
      private static final Set<String> STOP_WORDS = new HashSet<>(Arrays.asList(
          "i", "you", "your", "she", "her", "he", "his", "they", "their", "we", "our", "it", "is", "are", "the", "a",
          "an", "and", "but", "or", "for", "to", "of", "in", "on", "at", "with", "by", "this", "that", "these", "those",
          "as", "if", "than", "then", "when", "where", "while", "who", "what", "which", "why", "how", "not", "no", "yes",
          "all", "some", "more", "most", "like", "about", "over", "here", "there", "now", "then",
          "yo", "tu", "el", "ella", "nosotros", "ellos", "ellas", "mi", "tu", "su", "nuestro","este", "ese", "aquel", "esto", 
          "eso", "a", "de", "en", "con", "por", "para", "sin", "mas", "menos", "como", "que", "cual", "quien","si", "desde",
          "hasta", "durante", "entre", "tras", "ante", "contra", "hacia", "aunque", "porque","ya"
      ));

      @Override
      public void map(spotify record, AvroCollector<Pair<String, Integer>> collector, Reporter reporter) throws IOException {
          if (record.getTrackName() != null) {
              String trackName = record.getTrackName().toString().toLowerCase().replaceAll("[^a-zA-Z ]", "");
              StringTokenizer tokenizer = new StringTokenizer(trackName);
      
              while (tokenizer.hasMoreTokens()) {
                  String token = tokenizer.nextToken();
                  if (!STOP_WORDS.contains(token)) {
                      collector.collect(new Pair<>(new String(token), 1));
                  }
              }
          } else {
              reporter.incrCounter("Map", "NullTrackName", 1);
          }
      }
  }

  public static class IntSumReducer extends AvroReducer<Object, Integer, Pair<String, Integer>> {
      @Override
      public void reduce(Object key, Iterable<Integer> values, AvroCollector<Pair<String, Integer>> collector, Reporter reporter) throws IOException {
        String keyStr = (key != null) ? key.toString() : null;
    
        if (keyStr != null) {
            int sum = 0;
            for (Integer val : values) {
                sum += val;
            }
            collector.collect(new Pair<>(keyStr, sum));
        }
    }
  }

  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: TrackWordCount <input path> <output path>");
      return -1;
    }

    JobConf conf = new JobConf(getConf(), TrackWordCount.class);
    conf.setJobName("trackwordcount");

    Path outputPath = new Path(args[1]);
    outputPath.getFileSystem(conf).delete(outputPath, true);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    AvroJob.setMapperClass(conf, TrackMapper.class);
    AvroJob.setReducerClass(conf, IntSumReducer.class);

    AvroJob.setInputSchema(conf, spotify.getClassSchema());
    AvroJob.setOutputSchema(conf, Pair.getPairSchema(Schema.create(Type.STRING), Schema.create(Type.INT)));

    JobClient.runJob(conf);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new TrackWordCount(), args);

    if (res == 0) {
      File outputDir = new File(args[1]);
      File[] outputFiles = outputDir.listFiles();
      for (File outputFile : outputFiles) {
        if (outputFile.getName().endsWith(".avro")) {
          String textName = outputFile.getName().replace(".avro", ".txt");
          List<String> records = DeserializationData.getRecords(outputFile.getAbsolutePath(), "string", "int");
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



