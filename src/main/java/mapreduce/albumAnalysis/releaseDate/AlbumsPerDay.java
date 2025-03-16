package mapreduce;

import java.io.IOException;

// Necesario para contar de forma unica los albumes
import java.util.HashSet;   
import java.util.Set;       

// Necesario para la deserializacion
import java.util.List;      
import java.io.File;        
import org.apache.commons.io.FileUtils;

// Necesario para el acceso a AvroMapper, AvroReducer y funciones para 
// obtener esquemas de avro
import org.apache.avro.Schema.Type;
import org.apache.avro.mapred.*;
import org.apache.avro.*;

// Necesario para la configuraciones del job
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

// Esquema spotify para la entrada
import classes.avro.spotify;
import classes.avro.DayValue;

public class AlbumsPerDay extends Configured implements Tool 
{

    // Mapper: extrae el año, día de lanzamiento y el nombre del álbum
    public static class AlbumsPerDayMapper 
    extends AvroMapper<spotify, Pair<Integer, DayValue>> 
    {
        @Override
        public void map(spotify track, 
                        AvroCollector<Pair<Integer, DayValue>> 
                        collector, 
                        Reporter reporter)
        throws IOException 
        {
            // Obtenemos el nombre del album, su año y día de lanzamiento
            Integer year = track.getYearOfRelease();
            Integer day = track.getDayOfRelease();
            CharSequence albumName = track.getAlbumName();

            if (year != null && year != 1 && albumName != null && day != null) 
            {
                DayValue dayValue = new DayValue(day, albumName.toString());
                collector.collect(new Pair<Integer, DayValue>(year, dayValue));
            } 

            if ((year == null || year == 1) && albumName != null && day != null)  
            {
                DayValue dayValue = new DayValue(day, albumName.toString());
                collector.collect(new Pair<Integer, DayValue>(0, dayValue));
            } 

        }
    }

    // Reducer: cuenta la cantidad de álbumes únicos que salen por día
    public static class AlbumsPerDayReducer 
    extends AvroReducer<Integer, DayValue, Pair<Integer, DayValue>> 
    {
        @Override
        public void reduce( Integer key, 
                            Iterable<DayValue> values, 
                            AvroCollector<Pair<Integer, DayValue>> collector,
                            Reporter reporter) 
        throws IOException {

            // Array para llevar la cantidad de albumes unicos por dia
            Set<CharSequence>[] uniqueAlbumsPerDay = new HashSet[31];

             // Inicializamos los sets
            for (int i = 0; i < uniqueAlbumsPerDay.length; i++) {
                uniqueAlbumsPerDay[i] = new HashSet<>();
            }

            for (DayValue dayValue : values) 
            {
                Integer day = dayValue.getDay();
                CharSequence album = dayValue.getAlbums();
                uniqueAlbumsPerDay[day - 1].add(album);
            }

            Integer numAlbums;
            for (int i = 0; i < uniqueAlbumsPerDay.length; i++) {

                numAlbums = uniqueAlbumsPerDay[i].size();
                DayValue dayValue = new DayValue(i + 1, String.valueOf(numAlbums));

                collector.collect(new Pair<>( key , dayValue));
            }          
        }
    }


    // Configura y ejecuta el trabajo de MapReduce.
    // args:  Argumentos de la línea de comandos: [ruta de entrada] [ruta de salida]
    // retorna  0 si el trabajo se completa con éxito, un valor distinto de cero si falla.
    public int run(String[] args) throws Exception 
    {
        // Indicaciones de uso
        if (args.length != 2) 
        {
            System.err.println("Uso: AlbumsPerDay <input path> <output path>");
            return -1;
        }

        // Configuración del trabajo
        JobConf conf = new JobConf(getConf(), AlbumsPerDay.class);
        conf.setJobName("Contar el número de albums publicados por día de cada año");

        // Si el path de salida provisto ya existe, se elimina
        Path outputPath = new Path(args[1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);

        // Establecemos los paths de entrada y salida
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        // Establecemos el mappper y reducer
        AvroJob.setMapperClass(conf, AlbumsPerDayMapper.class);
        AvroJob.setReducerClass(conf, AlbumsPerDayReducer.class);

        // Especificamos el esquema Avro de entrada y salida
        AvroJob.setInputSchema(conf, spotify.getClassSchema());
        AvroJob.setOutputSchema(conf,Pair.getPairSchema(Schema.create(Type.INT),DayValue.getClassSchema()));

        // Ejecuta el trabajo de MapReduce
        JobClient.runJob(conf);
        return 0;
    }

    // Main para iniciar el trabajo de MapReduce.
    public static void main(String[] args) throws Exception 
    {

        // Ejecuta el trabajo de MapReduce utilizando ToolRunner
        int res = ToolRunner.run(new Configuration(), new AlbumsPerDay(), args);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);


        // Procesa la salida si el trabajo fue exitoso
        if (res == 0) 
        {

            System.out.println("Trabajo terminado con éxito");

            // Comienzo del proceso de deserialización
            File outputDir = new File(args[1]);
            File[] outputFiles = outputDir.listFiles();

            for (File outputFile : outputFiles) 
            {
                if (outputFile.getName().endsWith(".avro")) 
                {
                    String textName = outputFile.getName().replace(".avro", ".txt");

                    List<String> records = DeserializationData.getPairIntDayValueRecords(outputFile.getAbsolutePath());
                    
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