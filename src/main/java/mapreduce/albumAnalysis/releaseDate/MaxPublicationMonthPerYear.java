package mapreduce;

import java.io.IOException;
import java.util.List;
import java.io.File;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.FileUtils;

import org.apache.avro.*;
import org.apache.avro.mapred.*;

import org.apache.avro.Schema.Type;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericArray;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import classes.avro.YearMonthSummary;
import classes.avro.MonthPublication;
import classes.avro.MonthlyPublicationRanking;


// MapReduce para identificar los meses que han sido el mes con más publicaciones
// de álbumes en al menos un año.
// La salida muestra cada mes y la lista de años en los que fue el mes con más publicaciones.
public class MaxPublicationMonthPerYear extends Configured implements Tool {


    // Obtiene el nombre del mes a partir de su número.
    // monthNumber: Número del mes (1-12).
    // retorna el mombre del mes o "Invalid month" si el número no es válido.
    public static String getMonthName(int monthNumber) 
    {
        if (monthNumber < 1 || monthNumber > 12) {
            return "Invalid month";
        }

        String[] monthNames = {
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December"
        };

        return monthNames[monthNumber - 1];
    }

    // Mapper:  - Toma el resumen anual de publicaciones mensuales y extrae el mes
    //          con la mayor cantidad de publicaciones para ese año.
    //          - La salida es un par (Mes, MonthlyPublicationRanking), donde
    //          MonthlyPublicationRanking contiene el nombre del mes, el numero de 
    //          años que lo tuvieron como maximo (1 en esta fase) y una lista con 
    //          solo el año actual
    public static class MaxPublicationMonthPerYearMapper 
    extends AvroMapper< Pair<Integer, YearMonthSummary>, 
                        Pair<Integer, MonthlyPublicationRanking>> 
    {
        @Override
        public void map(Pair<Integer, YearMonthSummary> MonthSummaryPerYear, 
                        AvroCollector< Pair<Integer, MonthlyPublicationRanking> > collector, 
                        Reporter reporter)
        throws IOException 
        {

            Integer year = MonthSummaryPerYear.key();
            YearMonthSummary yearMonthSummary =  MonthSummaryPerYear.value();

            MonthPublication maxPublicationMonth = yearMonthSummary.getMaxPublicationMonth();
            Integer month = Integer.valueOf(maxPublicationMonth.getMonth());

            String monthName = getMonthName(month);
        
            Schema intSchema = Schema.create(Schema.Type.INT);
            Schema arraySchema = Schema.createArray(intSchema);
            GenericData.Array<Integer> years = new GenericData.Array<>(0, arraySchema);
            years.add(year);

            MonthlyPublicationRanking MPR = new MonthlyPublicationRanking(monthName, 1, years);

            collector.collect(new Pair<Integer, MonthlyPublicationRanking>(month, MPR));
        }
    }

    // Reducer: Agrupa los registros por mes y cuenta cuántos años tienen ese mes
    //          como el mes con más publicaciones.
    //          - La entrada es un mes y una lista de MonthlyPublicationRanking.
    //          - La salida  es un par (Mes, MonthlyPublicationRanking), donde
    //              MonthlyPublicationRanking contiene el nombre del mes, 
    //              la cantidad de años en que fue el mes con mas publicaciones
    //              y la lista de esos años.
    public static class MaxPublicationMonthPerYearReducer 
        extends AvroReducer<Integer, 
                            MonthlyPublicationRanking, 
                            Pair<Integer, MonthlyPublicationRanking>> 
    {
        @Override
        public void reduce( Integer key, 
                            Iterable<MonthlyPublicationRanking> values, 
                            AvroCollector<Pair<Integer,MonthlyPublicationRanking>> collector,
                            Reporter reporter) 
        throws IOException 
        {
            String monthName = getMonthName(key);
            Integer count = 0;

            Schema intSchema = Schema.create(Schema.Type.INT);
            Schema arraySchema = Schema.createArray(intSchema);
            GenericData.Array<Integer> years = new GenericData.Array<>(0, arraySchema);
          

            for (MonthlyPublicationRanking MPR : values) {

                count++;
                Integer year = Integer.valueOf(MPR.getYears().get(0));
                years.add(year);
            }

            MonthlyPublicationRanking FMPR = new MonthlyPublicationRanking(monthName, count, years);

            collector.collect(new Pair<>( key , FMPR));
         
        }
    }

    // Configura y ejecuta el trabajo de MapReduce.
    public int run(String[] args) throws Exception 
    {

        if (args.length != 2) 
        {
            System.err.println("Uso: MaxPublicationMonthPerYear <input path> <output path>");
            return -1;
        }

        JobConf conf = new JobConf(getConf(), MaxPublicationMonthPerYear.class);
        conf.setJobName("Contar cuantos años tienen cierto mes como su mes con mas publicaciones");

        Path outputPath = new Path(args[1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        AvroJob.setMapperClass(conf, MaxPublicationMonthPerYearMapper.class);
        AvroJob.setReducerClass(conf, MaxPublicationMonthPerYearReducer.class);

        Schema intSchema = Schema.create(Type.INT);
        Schema yearMonthSummarySchema = YearMonthSummary.getClassSchema();
        Schema inputSchema = Pair.getPairSchema(intSchema, yearMonthSummarySchema); 
        AvroJob.setInputSchema(conf,inputSchema);

        Schema MPRSchema = MonthlyPublicationRanking.getClassSchema();
        Schema outputSchema = Pair.getPairSchema(intSchema, MPRSchema); 
        AvroJob.setOutputSchema(conf, outputSchema);

        JobClient.runJob(conf);
        return 0;
    }

    // Main para iniciar el trabajo de MapReduce.
    public static void main(String[] args) throws Exception 
    {

        int res = ToolRunner.run(new Configuration(), new MaxPublicationMonthPerYear(), args);
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
                    List<String> records = DeserializationData.getPairIntMPRRecords(outputFile.getAbsolutePath());
                    // Creamos el archivo con los registros obtenidos 
                    File textFile = new File(outputFile.getParent(), textName);
                    FileUtils.writeLines(textFile, records);
                }
            }
            System.out.println("Trabajo terminado con éxito");
        } else {
            System.out.println("El trabajo falló");
        }
        System.exit(res);
    }

}