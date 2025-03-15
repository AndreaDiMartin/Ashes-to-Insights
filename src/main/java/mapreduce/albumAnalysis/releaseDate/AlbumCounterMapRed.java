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

// Codigo generaro para el esquema spotify
import classes.avro.spotify;

public class AlbumCounterMapRed extends Configured implements Tool 
{

    // Mapper: extrae el año de lanzamiento y el nombre del álbum
    public static class AlbumMapper 
    extends AvroMapper<spotify, Pair<Integer, CharSequence>> 
    {
        @Override
        public void map(spotify track, 
                        AvroCollector<Pair<Integer, CharSequence>> 
                        collector, 
                        Reporter reporter)
        throws IOException 
        {
            // Obtenemos el nombre del album y su año de lanzamiento
            Integer year = track.getYearOfRelease();
            CharSequence albumName = track.getAlbumName();

            // Si el nombre y el año son distintos a nulo, 
            // agregamos el par con su información
            if (year != null && year != 1 && albumName != null) 
            {
                collector.collect(new Pair<Integer, CharSequence>(year, albumName));
            } 

            // Si el año es null o tiene el error del 1, pero su nombre
            // sigue sin ser nulo, se agrega el par especial con año cero
            // para llevar un seguimiento
            if ((year == null || year == 1) && albumName != null) 
            {
                collector.collect(new Pair<Integer, CharSequence>(0, albumName));
            } 

        }
    }

    // Reducer: cuenta la cantidad de álbumes únicos por año
    public static class AlbumReducer 
    extends AvroReducer<Integer, CharSequence, Pair<Integer, CharSequence>> 
    {
        @Override
        public void reduce( Integer key, 
                            Iterable<CharSequence> values, 
                            AvroCollector<Pair<Integer, CharSequence>> collector,
                            Reporter reporter) 
        throws IOException {

            // Dado que en el CSV cada linea corresponde a una cancion
            // los albumes salen repetidos, por esto se usa el siguiente set
            // para contar de forma unica cada album
            Set<CharSequence> uniqueAlbums = new HashSet<>();
            for (CharSequence album : values) 
            {
                uniqueAlbums.add(album.toString());
            }

            // Dado que el par que se estaba usando estaba compuesto de una
            // key Integer y un value CharSequence, no podemos cambiar el esquema
            // para ahora dar un valor Integer, como en este MapReduce solo nos interesa
            // saber la cantidad de albumes unicos por año convertimos el tam del
            // set en string
            collector.collect(new Pair<Integer, CharSequence>(key, String.valueOf(uniqueAlbums.size()) ));
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
            System.err.println("Uso: AlbumCounterMapRed <input path> <output path>");
            return -1;
        }

        // Configuración del trabajo
        JobConf conf = new JobConf(getConf(), AlbumCounterMapRed.class);
        conf.setJobName("Contar el número de albums publicados por año");

        // Si el path de salida provisto ya existe, se elimina
        Path outputPath = new Path(args[1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);

        // Establecemos los paths de entrada y salida
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        // Establecemos el mappper y reducer
        AvroJob.setMapperClass(conf, AlbumMapper.class);
        AvroJob.setReducerClass(conf, AlbumReducer.class);

        // Especificamos el esquema Avro de entrada y salida
        AvroJob.setInputSchema(conf, spotify.getClassSchema());
        AvroJob.setOutputSchema(conf,Pair.getPairSchema(Schema.create(Type.INT),Schema.create(Type.STRING)));

        // Ejecuta el trabajo de MapReduce
        JobClient.runJob(conf);
        return 0;
    }

    // Main para iniciar el trabajo de MapReduce.
    public static void main(String[] args) throws Exception 
    {

        // Ejecuta el trabajo de MapReduce utilizando ToolRunner
        int res = ToolRunner.run(new Configuration(), new AlbumCounterMapRed(), args);
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

                    List<String> records = DeserializationData.getRecords(outputFile.getAbsolutePath(), "int", "string");
                    
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