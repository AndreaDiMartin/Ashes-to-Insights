package mapreduce;

import java.io.IOException;

// Necesario para contar de forma unica los albumes
import java.util.HashSet;   
import java.util.HashMap;   
import java.util.Map;   
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

// Esquema usados
import classes.avro.spotify;
import classes.avro.TypeValue;

public class AlbumTypes extends Configured implements Tool 
{

    // Mapper: extrae el año de lanzamiento, el nombre y tipo del álbum
    public static class AlbumTypesMapper 
    extends AvroMapper<spotify, Pair<Integer, TypeValue>> 
    {
        @Override
        public void map(spotify track, 
                        AvroCollector<Pair<Integer, TypeValue>> 
                        collector, 
                        Reporter reporter)
        throws IOException 
        {
            // Obtenemos el nombre del album, su tipo y su año de lanzamiento
            Integer year = track.getYearOfRelease();
            CharSequence albumName = track.getAlbumName();
            CharSequence albumType = track.getAlbumType();

            // Si el nombre, el tipo  el año son distintos a nulo, 
            // agregamos el par con su información
            if (year != null && year != 1 && albumType != null && albumName != null) 
            {
                TypeValue typeValue = new TypeValue(albumType.toString(), albumName.toString());
                collector.collect(new Pair<Integer, TypeValue>(year, typeValue));
            } 

            
            if ((year == null || year == 1) && albumName != null && albumType != null) 
            {
                TypeValue typeValue = new TypeValue(albumType.toString(), albumName.toString());
                collector.collect(new Pair<Integer, TypeValue>(0, typeValue));
            } 

        }
    }

    // Reducer: cuenta la cantidad de álbumes únicos por año
    public static class AlbumTypesReducer 
    extends AvroReducer<Integer, TypeValue, Pair<Integer, TypeValue>> 
    {
        @Override
        public void reduce( Integer key, 
                            Iterable<TypeValue> values, 
                            AvroCollector<Pair<Integer, TypeValue>> collector,
                            Reporter reporter) 
        throws IOException {

            
            Set<CharSequence> uniqueAlbums = new HashSet<>();
            Map<CharSequence, Integer> typeCounter =  new HashMap<>();
            for (TypeValue typeValue : values) 
            {
                CharSequence type = typeValue.getType();
                CharSequence album = typeValue.getAlbums();

                if (uniqueAlbums.add(album.toString()))
                {
                    typeCounter.putIfAbsent(type.toString(), 0);
                    Integer count = typeCounter.get(type.toString());
                    typeCounter.put(type.toString(), count + 1 );
                }
                
            }

            Set<CharSequence> types = typeCounter.keySet();
            for (CharSequence type : types) 
            {
                TypeValue typeValue = new TypeValue(type.toString(), typeCounter.get(type).toString());
                collector.collect(new Pair<Integer, TypeValue>(key, typeValue));
              
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
            System.err.println("Uso: AlbumTypes <input path> <output path>");
            return -1;
        }

        // Configuración del trabajo
        JobConf conf = new JobConf(getConf(), AlbumTypes.class);
        conf.setJobName("Contar los diferentes tipos de albums publicados por año");

        // Si el path de salida provisto ya existe, se elimina
        Path outputPath = new Path(args[1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);

        // Establecemos los paths de entrada y salida
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        // Establecemos el mappper y reducer
        AvroJob.setMapperClass(conf, AlbumTypesMapper.class);
        AvroJob.setReducerClass(conf, AlbumTypesReducer.class);

        // Especificamos el esquema Avro de entrada y salida
        AvroJob.setInputSchema(conf, spotify.getClassSchema());
        AvroJob.setOutputSchema(conf,Pair.getPairSchema(Schema.create(Type.INT), TypeValue.getClassSchema() ));

        // Ejecuta el trabajo de MapReduce
        JobClient.runJob(conf);
        return 0;
    }

    // Main para iniciar el trabajo de MapReduce.
    public static void main(String[] args) throws Exception 
    {

        // Ejecuta el trabajo de MapReduce utilizando ToolRunner
        int res = ToolRunner.run(new Configuration(), new AlbumTypes(), args);
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

                    List<String> records = DeserializationData.getPairIntTypeValueRecords(outputFile.getAbsolutePath());
                    
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