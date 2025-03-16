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
import classes.avro.MonthValue;
import classes.avro.YearMonthSummary;
import classes.avro.MonthPublication;

// MapReduce para crear un resumen por año de los lanzamientos mensuales de albums.
// Toma como entrada la salida del trabajo de MapReduce que cuenta las publicaciones álbumes
// por mes de cada año.
public class AlbumsPerMonthSummary extends Configured implements Tool {

    // Mapper:  -   Toma la salida del contador de publicaciones por mes y 
    //              crea un resumen inicial por año.
    //              La entrada del mapper es un par (Año, MonthValue), donde MonthValue 
    //          contiene el mes y la cantidad de publicaciones (como un string).
    //          -   La salida del mapper es un par (Año, YearMonthSummary), donde 
    //              YearMonthSummary contiene información sobre el año 
    //              (mes con mas lanzamientos, mes con menos y una lista por mes 
    //              y sus lanzamientos)
    public static class AlbumsPerMonthSummaryMapper 
    extends AvroMapper<Pair<Integer, MonthValue>, Pair<Integer, YearMonthSummary>> 
    {

        @Override
        public void map(Pair<Integer, MonthValue> monthValuePerYear, 
                        AvroCollector< Pair<Integer, YearMonthSummary> > collector, 
                        Reporter reporter)
        throws IOException {

            // Obtenemos el la key que es el año y el valor que es MonthValue
            Integer year = monthValuePerYear.key();
            MonthValue monthValue = monthValuePerYear.value();

            // De monthValue obtenemos el mes y la cantidad de albumes lanzados
            // pasando de string a int 
            Integer month = monthValue.getMonth();
            Integer publicationCount = Integer.valueOf(monthValue.getAlbums().toString());

            // Creamos los datos de este mes 
            MonthPublication publicationMonth = new MonthPublication (month, publicationCount);
            
            // Creamos el array para los doce meses
            Schema arraySchema = Schema.createArray(MonthPublication.getClassSchema());
            GenericArray<MonthPublication> months = new GenericData.Array<>(12, arraySchema);

            // Vamos por orden agregando al info de cada mes,
            // Todos los meses van a salir sin datos exceptuando el actual
            for (int i = 0; i < 12; i++) {

                if (i + 1 == month){
                    months.add(publicationMonth);
                    continue;
                }

                MonthPublication emptyPublicationMonth = new MonthPublication (i + 1, 0);
                months.add(emptyPublicationMonth);
            }

            // Creamos el resumen de este año, como solo tenemos el valor del mes actual
            // lo ponemos tanto como max como min
            YearMonthSummary yearMonthSummary = new YearMonthSummary(publicationMonth, publicationMonth, months);

            collector.collect(new Pair<Integer, YearMonthSummary>(year, yearMonthSummary));
        }   
    }


    // Reducer: Combina los resúmenes iniciales por año para crear un resumen final.
    //          - La entrada es un año y un YearMonthSummary asociados a ese año.
    //          - La salida es un par (Año, YearMonthSummary), donde YearMonthSummary 
    //          contiene el mes con más publicaciones, el mes con menos publicaciones 
    //          y el conteo de publicaciones para cada mes del año.
    public static class AlbumsPerMonthSummaryReducer 
        extends AvroReducer<Integer, YearMonthSummary, Pair<Integer, YearMonthSummary>> 
    {
        @Override
        public void reduce( Integer key, 
                            Iterable<YearMonthSummary> values, 
                            AvroCollector<Pair<Integer, YearMonthSummary>> collector,
                            Reporter reporter) 
        throws IOException {

            // Inicializa los objetos para el mes con más y menos publicaciones
            MonthPublication maxMonthPublication = new MonthPublication(-1, Integer.MIN_VALUE);
            MonthPublication minMonthPublication = new MonthPublication(-1, Integer.MAX_VALUE);
            MonthPublication[] months = new MonthPublication[12];

            // Itera sobre los YearMonthSummary recibidos para el año actual
            for (YearMonthSummary yearMonthSummary : values) 
            {

                MonthPublication monthPublication = yearMonthSummary.getMaxPublicationMonth();
                Integer month = monthPublication.getMonth();
                Integer publicationCount = monthPublication.getPublicationCount();
                    
                // Actualiza el mes con menos publicaciones si es necesario
                if (publicationCount < minMonthPublication.getPublicationCount()){
                    minMonthPublication.setMonth(month);
                    minMonthPublication.setPublicationCount(publicationCount);
                }

                // Actualiza el mes con más publicaciones si es necesario
                if (publicationCount > maxMonthPublication.getPublicationCount()){
                    maxMonthPublication.setMonth(month);
                    maxMonthPublication.setPublicationCount(publicationCount);
                }

                // Almacena la publicación del mes en el array
                months[month - 1] = new MonthPublication(month, publicationCount);
            }

            Schema arraySchema = Schema.createArray(MonthPublication.getClassSchema());
            // Crea un GenericArray para almacenar las publicaciones de los 12 meses
            GenericArray<MonthPublication> Months = new GenericData.Array<>(12, arraySchema);

            // Llena el array con las publicaciones de cada mes
            for (int i = 0; i < 12; i++) {
                MonthPublication publicationMonth = months[i];
                Months.add(publicationMonth);
            }

            // Crea el objeto YearMonthSummary final
            YearMonthSummary yearMonthSummary = new YearMonthSummary(maxMonthPublication, minMonthPublication, Months);

            collector.collect(new Pair<>( key , yearMonthSummary));
         
        }
    }


    //  Configura y ejecuta el trabajo de MapReduce.
    //  args: Argumentos de la línea de comandos: [ruta de entrada] [ruta de salida]
    //  retorn 0 si el trabajo se completa con éxito, un valor distinto de cero si falla.
    public int run(String[] args) throws Exception 
    {

        if (args.length != 2) {
            System.err.println("Uso: AlbumsPerMonthSummary <input path> <output path>");
            return -1;
        }

        JobConf conf = new JobConf(getConf(), AlbumsPerMonthSummary.class);
        conf.setJobName("Contar el numero de albums publicados por mes de cada año");

        Path outputPath = new Path(args[1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        AvroJob.setMapperClass(conf, AlbumsPerMonthSummaryMapper.class);
        AvroJob.setReducerClass(conf, AlbumsPerMonthSummaryReducer.class);

        AvroJob.setInputSchema(conf, Pair.getPairSchema(Schema.create(Type.INT), MonthValue.getClassSchema()));
        AvroJob.setOutputSchema(conf, Pair.getPairSchema(Schema.create(Type.INT), YearMonthSummary.getClassSchema()));

        JobClient.runJob(conf);
        return 0;
    }


    // Main para iniciar el trabajo de MapReduce y deserializar la salida
    public static void main(String[] args) throws Exception 
    {

        int res = ToolRunner.run(new Configuration(), new AlbumsPerMonthSummary(), args);
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
                    List<String> records = DeserializationData.getPairIntYearMonthSummaryRecords(outputFile.getAbsolutePath());
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