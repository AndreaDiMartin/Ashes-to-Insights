package mapreduce;

import java.io.IOException;
import java.util.List;
import java.io.File;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.avro.*;
import org.apache.avro.Schema.Type;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.mapred.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

// Esquemas usados
import classes.avro.spotify;
import classes.avro.WeeklyAlbumReleases;
import classes.avro.DayAlbumData;

// MapReduce para contar el número de álbumes únicos 
// publicados por día de la semana para cada año
public class AlbumsPerWeekdayPerYear extends Configured implements Tool {


    // Mapper:  Extrae el año, el día de la semana de lanzamiento y 
    //          el nombre del álbum de cada registro de Spotify.
    //          - La salida del mapper es un par (Año, WeeklyAlbumReleases), 
    //            donde WeeklyAlbumReleases contiene una lista de DayAlbumData
    //            con el día de la semana y el nombre del álbum.
    public static class AlbumsPerWeekdayPerYearMapper 
    extends AvroMapper<spotify, Pair<Integer, WeeklyAlbumReleases>> 
    {

        @Override
        public void map(spotify track, 
                        AvroCollector<Pair<Integer, WeeklyAlbumReleases>> collector, 
                        Reporter reporter)
        throws IOException 
        {

            // Accedemos al año, día de la semana y nombre del álbum
            Integer year = track.getYearOfRelease();
            CharSequence weekday = track.getWeekdayOfRelease();
            CharSequence albumName = track.getAlbumName();

            Schema stringSchema = Schema.create(Schema.Type.STRING);
            Schema arrayStringSchema = Schema.createArray(stringSchema);
            Schema arrayDADSchema = Schema.createArray(DayAlbumData.getClassSchema());

            if (year != null && weekday != null &&  albumName != null) 
            {
                GenericArray<CharSequence> albumList = new GenericData.Array<>(0, arrayStringSchema);
                albumList.add(albumName);

                DayAlbumData dayAlbumData = new DayAlbumData(weekday, 1, albumList);

                GenericArray<DayAlbumData> dayAlbumDataArray = new GenericData.Array<>(0, arrayDADSchema);
                dayAlbumDataArray.add(dayAlbumData);

                WeeklyAlbumReleases WAR = new WeeklyAlbumReleases(dayAlbumDataArray);

                collector.collect(new Pair<Integer, WeeklyAlbumReleases>(year, WAR));
            } 

        }
    }


    // Reducer: Cuenta la cantidad de álbumes únicos publicados por día
    //          de la semana para cada año.
    //          La entrada es un año y una lista de WeeklyAlbumReleases.
    //          La salida es un par (Año, WeeklyAlbumReleases), 
    //          donde WeeklyAlbumReleases contiene los objetos DayAlbumData con 
    //          la cantidad de álbumes únicos por día de la semana.
    public static class AlbumsPerWeekdayPerYearReducer 
        extends AvroReducer<Integer, 
                            WeeklyAlbumReleases, 
                            Pair<Integer, WeeklyAlbumReleases>> 
    {

        @Override
        public void reduce( Integer key, 
                            Iterable<WeeklyAlbumReleases> values, 
                            AvroCollector<Pair<Integer,WeeklyAlbumReleases>> collector,
                            Reporter reporter) 
        throws IOException 
        {
            
            //Dicc  para llevar los albumes unicos por cada dia
            Map<CharSequence, Set<CharSequence>> uniqueAlbums = new HashMap<>();

            for (WeeklyAlbumReleases WAR : values) 
            {
                List<DayAlbumData> daysOfWeek = WAR.getDaysOfWeek();

                for (DayAlbumData dayAlbumData : daysOfWeek) 
                {
                    CharSequence day = dayAlbumData.getDay().toString();
                    List<CharSequence> albumList = dayAlbumData.getAlbumList();
                    CharSequence album = albumList.get(0).toString();

                    uniqueAlbums.putIfAbsent(day, new HashSet<>());
                    uniqueAlbums.get(day).add(album);
                }
            }

            Set<CharSequence> days = uniqueAlbums.keySet();

            Schema stringSchema = Schema.create(Schema.Type.STRING);
            Schema arrayStringSchema = Schema.createArray(stringSchema);
            Schema arrayDADSchema = Schema.createArray(DayAlbumData.getClassSchema());

            GenericArray<DayAlbumData> dayAlbumDataArray = new GenericData.Array<>(0, arrayDADSchema);

            for (CharSequence day : days) 
            {

                GenericArray<CharSequence> albumList = new GenericData.Array<>(0, arrayStringSchema); 

                // Agregar los albumes a la lista final de albumes 
                Set<CharSequence> albums = uniqueAlbums.get(day);
                for (CharSequence album : albums) 
                {
                    albumList.add(album.toString());
                }

                // Creamos el registro para este dia
                DayAlbumData dayAlbumData = new DayAlbumData(day.toString(),
                                                             albums.size(), 
                                                             albumList);
                dayAlbumDataArray.add(dayAlbumData);
            }
            
            WeeklyAlbumReleases WAR = new WeeklyAlbumReleases(dayAlbumDataArray);
        
            collector.collect(new Pair<Integer, WeeklyAlbumReleases>(key , WAR));
         
        }
    }

    public int run(String[] args) throws Exception 
    {

        if (args.length != 2) 
        {
            System.err.println("Uso: AlbumsPerWeekdayOfEachYear <input path> <output path>");
            return -1;
        }

        JobConf conf = new JobConf(getConf(), AlbumsPerWeekdayPerYear.class);
        conf.setJobName("Contar el numero de albums publicados por dia de la semana de cada año");

        Path outputPath = new Path(args[1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        AvroJob.setMapperClass(conf, AlbumsPerWeekdayPerYearMapper.class);
        AvroJob.setReducerClass(conf, AlbumsPerWeekdayPerYearReducer.class);

        Schema inputSchema = spotify.getClassSchema();
        AvroJob.setInputSchema(conf, inputSchema);

        Schema intSchema = Schema.create(Type.INT);
        Schema WARSchema = WeeklyAlbumReleases.getClassSchema();
        Schema outputSchema = Pair.getPairSchema(intSchema, WARSchema); 
        AvroJob.setOutputSchema(conf, outputSchema);

        JobClient.runJob(conf);
        return 0;
    }

    public static void main(String[] args) throws Exception 
    {

        int res = ToolRunner.run(new Configuration(), new AlbumsPerWeekdayPerYear(), args);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        if (res == 0) {
            // Accedemos al directorio donde se encuentra el output del MapReduce
            File outputDir = new File(args[1]);
            // Enlistamos los archivos de este directorio
            File[] outputFiles = outputDir.listFiles();

            // Vamos a interar sobre el arreglo
            for (File outputFile : outputFiles) 
            {
                // Nos interesa el archivo .avro con nuestros datos serializados
                if (outputFile.getName().endsWith(".avro")) 
                {
                    // Creamos el nombre para nuestro archivo txt 
                    String textName = outputFile.getName().replace(".avro", ".txt");
                    // Llamamos a la funcion espeficia para deserializar el esquema usado
                    List<String> records = DeserializationData.getPairIntWARRecords(outputFile.getAbsolutePath());
                    // Creamos el archivo con los registros obtenidos 
                    File textFile = new File(outputFile.getParent(), textName);
                    FileUtils.writeLines(textFile, records);
                }
            }
            System.out.println("Trabajo terminado con éxito");
        } else {
            System.out.println("Trabajo falló");
        }
        System.exit(res);
    }
    
}