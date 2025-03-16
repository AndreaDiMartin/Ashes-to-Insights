package mapreduce;

import java.io.IOException;
import java.util.List;
import java.io.File;

import java.util.HashSet;
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
import classes.avro.DayValue;
import classes.avro.YearDaySummary;
import classes.avro.DayPublication;

// MapReduce para crear un resumen por año de los lanzamientos diarios de albums.
// Toma como entrada la salida del trabajo de MapReduce que cuenta las publicaciones 
// de álbumes por dia de cada año.
public class AlbumsPerDaySummary extends Configured implements Tool {

    // Mapper:  -   Toma la salida del contador de publicaciones por dia y 
    //              crea un resumen inicial por año.
    //              La entrada del mapper es un par (Año, MonthValue), donde MonthValue 
    //          contiene el mes y la cantidad de publicaciones (como un string).
    //          -   La salida del mapper es un par (Año, YearDaySummary), donde 
    //              YearDaySummary contiene información sobre el año 
    //              (dia con mas lanzamientos, dia con menos y una lista por dia 
    //              y sus lanzamientos)
    public static class AlbumsPerDaySummaryMapper 
    extends AvroMapper<Pair<Integer, DayValue>, Pair<Integer, YearDaySummary>> 
    {

        @Override
        public void map(Pair<Integer, DayValue> dayValuePerYear, 
                        AvroCollector< Pair<Integer, YearDaySummary> > collector, 
                        Reporter reporter)
        throws IOException {

            // Obtenemos el la key que es el año y el valor que es DayValue
            Integer year = dayValuePerYear.key();
            DayValue dayValue = dayValuePerYear.value();

            // De DayValue obtenemos el dia y la cantidad de albumes lanzados
            // pasando de string a int 
            Integer day = dayValue.getDay();
            Integer publicationCount = Integer.valueOf(dayValue.getAlbums().toString());

            // Creamos los datos de este dia 
            DayPublication publicationDay = new DayPublication (day, publicationCount);
            
            // Creamos el array para los dias
            Schema arraySchema = Schema.createArray(DayPublication.getClassSchema());
            GenericArray<DayPublication> days = new GenericData.Array<>(0, arraySchema);

            // Vamos por orden agregando al info de cada dia,
            for (int i = 0; i < 31; i++) {

                if (i + 1 == day){
                    days.add(publicationDay);
                    continue;
                }

                DayPublication emptyPublicationDay = new DayPublication (i + 1, 0);
                days.add(emptyPublicationDay);
            }

          
            YearDaySummary yearDaySummary = new YearDaySummary(publicationDay, publicationDay, days);

            collector.collect(new Pair<Integer, YearDaySummary>(year, yearDaySummary));
        }   
    }


    // Reducer: Combina los resúmenes iniciales por año para crear un resumen final.
    //          - La entrada es un año y un YearDaySummary asociados a ese año.
    //          - La salida es un par (Año, YearDaySummary), donde YearDaySummary 
    //          contiene el dia con más publicaciones, el dia con menos publicaciones 
    //          y el conteo de publicaciones para cada dia del año.
    public static class AlbumsPerDaySummaryReducer 
        extends AvroReducer<Integer, YearDaySummary, Pair<Integer, YearDaySummary>> 
    {
        @Override
        public void reduce( Integer key, 
                            Iterable<YearDaySummary> values, 
                            AvroCollector<Pair<Integer, YearDaySummary>> collector,
                            Reporter reporter) 
        throws IOException {

            DayPublication maxDayPublication = new DayPublication(-1, Integer.MIN_VALUE);
            DayPublication minDayPublication = new DayPublication(-1, Integer.MAX_VALUE);
            DayPublication[] days = new DayPublication[31];

            // Itera sobre los YearDaySummary recibidos para el año actual
            for (YearDaySummary yearDaySummary : values) 
            {

                DayPublication dayPublication = yearDaySummary.getMaxPublicationDay();
                Integer day = dayPublication.getDay();
                Integer publicationCount = dayPublication.getPublicationCount();
                    
                if (publicationCount < minDayPublication.getPublicationCount()){
                    minDayPublication.setDay(day);
                    minDayPublication.setPublicationCount(publicationCount);
                }

                // Actualiza el dia con más publicaciones si es necesario
                if (publicationCount > maxDayPublication.getPublicationCount()){
                    maxDayPublication.setDay(day);
                    maxDayPublication.setPublicationCount(publicationCount);
                }

                // Almacena la publicación del dia en el array
                days[day - 1] = new DayPublication(day, publicationCount);
            }

            Schema arraySchema = Schema.createArray(DayPublication.getClassSchema());
            // Crea un GenericArray para almacenar las publicaciones
            GenericArray<DayPublication> Days = new GenericData.Array<>(12, arraySchema);

            // Llena el array con las publicaciones de cada dia
            for (int i = 0; i < 31; i++) {
                DayPublication publicationDay = days[i];
                Days.add(publicationDay);
            }

            YearDaySummary yearDaySummary = new YearDaySummary(maxDayPublication, minDayPublication, Days);

            collector.collect(new Pair<>( key , yearDaySummary));
         
        }
    }


    //  Configura y ejecuta el trabajo de MapReduce.
    //  args: Argumentos de la línea de comandos: [ruta de entrada] [ruta de salida]
    //  retorn 0 si el trabajo se completa con éxito, un valor distinto de cero si falla.
    public int run(String[] args) throws Exception 
    {

        if (args.length != 2) {
            System.err.println("Uso: AlbumsPerDaySummary <input path> <output path>");
            return -1;
        }

        JobConf conf = new JobConf(getConf(), AlbumsPerDaySummary.class);
        conf.setJobName("Contar el numero de albums publicados por mes de cada año");

        Path outputPath = new Path(args[1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        AvroJob.setMapperClass(conf, AlbumsPerDaySummaryMapper.class);
        AvroJob.setReducerClass(conf, AlbumsPerDaySummaryReducer.class);

        AvroJob.setInputSchema(conf, Pair.getPairSchema(Schema.create(Type.INT), DayValue.getClassSchema()));
        AvroJob.setOutputSchema(conf, Pair.getPairSchema(Schema.create(Type.INT), YearDaySummary.getClassSchema()));

        JobClient.runJob(conf);
        return 0;
    }


    // Main para iniciar el trabajo de MapReduce y deserializar la salida
    public static void main(String[] args) throws Exception 
    {

        int res = ToolRunner.run(new Configuration(), new AlbumsPerDaySummary(), args);
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
                    List<String> records = DeserializationData.getPairIntYearDaySummaryRecords(outputFile.getAbsolutePath());
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