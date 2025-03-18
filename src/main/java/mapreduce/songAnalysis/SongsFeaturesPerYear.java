package mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.List;

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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import classes.avro.SongsFeatures;
import classes.avro.spotify;

public class SongsFeaturesPerYear extends Configured implements Tool {

    public static class SongsFeaturesMapper extends AvroMapper<spotify, Pair<Integer, SongsFeatures>> 
    {
        @Override
        public void map(spotify track, AvroCollector<Pair<Integer, SongsFeatures>> collector, Reporter reporter) throws IOException {

            Integer year = track.getYearOfRelease();
            CharSequence name = track.getTrackName();
            Integer popularity = (int)track.getPopularity();

            if (year != null && year != 1 && name != null && popularity > 70) {
                SongsFeatures SF = new SongsFeatures(
                    (int) track.getExplicit(),
                    (float) track.getAcousticness(),
                    (float) track.getDanceability(),
                    (float) track.getEnergy(),
                    (float) track.getInstrumentalness(),
                    (int) track.getKey(),
                    (float) track.getLiveness(),
                    (float) track.getLoudness(),
                    (float) track.getSpeechiness(),
                    (float) track.getTempo(),
                    (int) track.getTimeSignature(),
                    (float) track.getValence()
                );

                collector.collect(new Pair<>(year, SF));
            }
        }


    }

    public static class AverageFeaturesReducer extends AvroReducer<Integer, SongsFeatures, Pair<Integer, SongsFeatures>> {
        @Override
        public void reduce(Integer year, Iterable<SongsFeatures> SFs, AvroCollector<Pair<Integer, SongsFeatures>> collector, Reporter reporter) throws IOException {
            
            int count = 0;
            int explicitSum = 0;
            float acousticnessSum = 0, danceabilitySum = 0, energySum = 0;
            float instrumentalnessSum = 0, livenessSum = 0, loudnessSum = 0;
            float speechinessSum = 0, tempoSum = 0, valenceSum = 0;
            int key = 0, timeSignature = 0;
    
            for (SongsFeatures SF : SFs) {
                explicitSum += SF.getExplicit();
                acousticnessSum += SF.getAcousticness();
                danceabilitySum += SF.getDanceability();
                energySum += SF.getEnergy();
                instrumentalnessSum += SF.getInstrumentalness();
                livenessSum += SF.getLiveness();
                loudnessSum += SF.getLoudness();
                speechinessSum += SF.getSpeechiness();
                tempoSum += SF.getTempo();
                valenceSum += SF.getValence();
                key = SF.getKey();
                timeSignature = SF.getTimeSignature();
                
                count++;
            }
    
            if (count > 0) {
                SongsFeatures avgSF = new SongsFeatures(
                    explicitSum / count,
                    acousticnessSum / count,
                    danceabilitySum / count,
                    energySum / count,
                    instrumentalnessSum / count,
                    key,
                    livenessSum / count,
                    loudnessSum / count,
                    speechinessSum / count,
                    tempoSum / count,
                    timeSignature, 
                    valenceSum / count
                );
    
                collector.collect(new Pair<>(year, avgSF));
            }
        }
    }
    

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: SongsFeaturesPerYear <input path> <output path>");
            return -1;
        }

        JobConf conf = new JobConf(getConf(), SongsFeaturesPerYear.class);
        conf.setJobName("SongsFeaturesPerYear");

        Path outputPath = new Path(args[1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);

        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        AvroJob.setInputSchema(conf, spotify.getClassSchema());
        AvroJob.setMapOutputSchema(conf, Pair.getPairSchema(Schema.create(Type.INT), SongsFeatures.getClassSchema()));
        AvroJob.setOutputSchema(conf, Pair.getPairSchema(Schema.create(Type.INT), SongsFeatures.getClassSchema()));

        AvroJob.setMapperClass(conf, SongsFeaturesMapper.class);
        AvroJob.setReducerClass(conf, AverageFeaturesReducer.class);

        JobClient.runJob(conf);
        return 0;
    }

    public static void main(String[] args) throws Exception 
    {

        // Ejecuta el trabajo de MapReduce utilizando ToolRunner
        int res = ToolRunner.run(new Configuration(), new SongsFeaturesPerYear(), args);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);


        // Procesa la salida si el trabajo fue exitoso
        if (res == 0) 
        {

            System.out.println("Trabajo terminado con éxito");

            //Comienzo del proceso de deserialización
            File outputDir = new File(args[1]);
            File[] outputFiles = outputDir.listFiles();

            for (File outputFile : outputFiles) 
            {
                if (outputFile.getName().endsWith(".avro")) 
                {
                    String textName = outputFile.getName().replace(".avro", ".txt");
                    List<String> records = DeserializationData.getPairIntSongsFeatures(outputFile.getAbsolutePath());
                    File textFile = new File(outputFile.getParent(), textName);
                    FileUtils.writeLines(textFile, records);
                }
            }

        } else 
        {
            System.out.println("El trabajo falló");
        }
        
        System.exit(res);
    }
}