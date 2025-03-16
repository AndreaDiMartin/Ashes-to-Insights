package mapreduce;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.Pair;
import org.apache.avro.Schema.Type;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// Esquemas usados
import classes.avro.DayValue;
import classes.avro.MonthValue;
import classes.avro.YearMonthSummary;
import classes.avro.YearDaySummary;
import classes.avro.MonthPublication;
import classes.avro.DayPublication;
import classes.avro.MonthlyPublicationRanking;
import classes.avro.WeeklyAlbumReleases;
import classes.avro.PopularityAnalysis;

// import java.io.ByteArrayOutputStream;
// import org.apache.avro.io.Encoder;
// import org.apache.avro.io.EncoderFactory;
// import org.apache.avro.io.JsonEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.generic.GenericDatumWriter;


public class DeserializationData {


    public static List<String> getPairIntPopularityAnalysisRecords(String avroFilePath) {
        List<String> records = new ArrayList<>();

        try {
            // Definir el esquema del par
            Schema schema = Pair.getPairSchema(Schema.create(Schema.Type.INT),  PopularityAnalysis.getClassSchema());

            GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
            File avroFile = new File(avroFilePath);

            // Leer el archivo Avro
            FileReader<GenericRecord> fileReader = DataFileReader.openReader(avroFile, datumReader);

            // Iterar sobre los registros
            while (fileReader.hasNext()) {
                GenericRecord record = fileReader.next();

                // Extraer la clave (key)
                int key = (int) record.get("key");

                // Extraer el valor (PopularityAnalysis)
                GenericRecord valueRecord = (GenericRecord) record.get("value");

                CharSequence albums = valueRecord.get("albums").toString();
                int min = (int) valueRecord.get("min");
                int max = (int) valueRecord.get("max");
                int mode = (int) valueRecord.get("mode");
                int range = (int) valueRecord.get("range");
                double q1 = (double) valueRecord.get("q1");
                double q2 = (double) valueRecord.get("q2");
                double q3 = (double) valueRecord.get("q3");
                double iqr = (double) valueRecord.get("iqr");

                // Crear un objeto de Pair
                Pair<Integer, PopularityAnalysis> pair = new Pair<>(
                    key,
                    new PopularityAnalysis(albums, min, max, mode, range, q1, q2, q3, iqr)
                );

                // Añadir el par como cadena a la lista
                records.add(pair.toString());
                records.add("\n");
            }

            fileReader.close(); // Cerrar el lector

        } catch (IOException e) {
            e.printStackTrace();
        }

        return records;
    }

    public static  List<String> getPairIntWARRecords(String avroFilePath){

        List<String> records = new ArrayList<>();
        try {

            Schema intSchema = Schema.create(Type.INT);
            Schema WARSchema = WeeklyAlbumReleases.getClassSchema();
            Schema outputSchema = Pair.getPairSchema(intSchema, WARSchema);             
            Schema schema = Pair.getPairSchema(intSchema, WARSchema);

            GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

            File avroFile = new File(avroFilePath);

            FileReader<GenericRecord> fileReader = DataFileReader.openReader(avroFile, datumReader);

            while (fileReader.hasNext()) {
                GenericRecord record = fileReader.next();

                // Extraer la clave 
                int key = (int) record.get("key");

                // Extraer el valor como GenericRecord
                GenericRecord weeklyAlbumReleasesRecord = (GenericRecord) record.get("value");

                // Obtener los campos del esquema WeeklyAlbumReleases
                List<GenericRecord> daysOfWeek = (List<GenericRecord>) weeklyAlbumReleasesRecord.get("daysOfWeek");

                StringBuilder daysOfWeekString = new StringBuilder("[");
                for (GenericRecord dayAlbumData : daysOfWeek) {
                    String day = dayAlbumData.get("day").toString();
                    int albumCount = (int) dayAlbumData.get("albumCount");
                    List<CharSequence> albumList = (List<CharSequence>) dayAlbumData.get("albumList");

                    // Formatear la información de cada día
                    daysOfWeekString.append(String.format("\n{ Day: %s,\n AlbumCount: %d,\n AlbumList: %s}, ",day, albumCount, albumList.toString()));
                }

                if (daysOfWeekString.length() > 1) {
                    daysOfWeekString.setLength(daysOfWeekString.length() - 2); 
                }
                daysOfWeekString.append("\n]");

                // Crear una representacion leible del registro
                String formattedRecord = String.format("Key: %d,\nDaysOfWeek: %s \n", key,daysOfWeekString.toString());

                // Agregar el registro formateado a la lista de resultados
                records.add(formattedRecord);
                
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return records;
    }

    public static  List<String> getPairIntMPRRecords(String avroFilePath){

        List<String> records = new ArrayList<>();
        try {

            Schema intSchema = Schema.create(Type.INT);
            Schema MPRSchema = MonthlyPublicationRanking.getClassSchema();             
            Schema schema = Pair.getPairSchema(intSchema, MPRSchema);;

            GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

            File avroFile = new File(avroFilePath);

            FileReader<GenericRecord> fileReader = DataFileReader.openReader(avroFile, datumReader);

            while (fileReader.hasNext()) {
                GenericRecord record = fileReader.next();
                
                // Extraer el valor de la clave 
                int key = (int) record.get("key");

                // Extraer el valor asociado (value) como GenericRecord
                GenericRecord valueRecord = (GenericRecord) record.get("value");

                // Obtener los campos de MonthlyPublicationRanking
                String month = valueRecord.get("month").toString();
                int yearCount = (int) valueRecord.get("yearCount");
                List<Integer> years = (List<Integer>) valueRecord.get("years");

                // Convertir la lista de años a una cadena
                String yearsString = years.toString();

                // Crear una representación legible del registro
                String formattedRecord = String.format(
                        "Key: %d, Month: %s, YearCount: %d, Years: %s",
                        key, month, yearCount, yearsString
                );

                // Agregar el registro formateado a la lista de resultados
                records.add(formattedRecord);

            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return records;
    }

    public static  List<String> getPairIntYearMonthSummaryRecords(String avroFilePath){

        List<String> records = new ArrayList<>();
        try {

            Schema schema = Pair.getPairSchema(Schema.create(Type.INT), YearMonthSummary.getClassSchema());

            GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

            File avroFile = new File(avroFilePath);

            FileReader<GenericRecord> fileReader = DataFileReader.openReader(avroFile, datumReader);

            while (fileReader.hasNext()) {

                GenericRecord record = fileReader.next();

                // Obtener los campos principales
                int year = (int) record.get("key");
                GenericRecord valueRecord = (GenericRecord) record.get("value");

                // Extraer los valores de YearMonthSummary
                GenericRecord maxPublicationMonth = (GenericRecord) valueRecord.get("maxPublicationMonth");
                int maxMonth = (int) maxPublicationMonth.get("month");
                int maxPublicationCount = (int) maxPublicationMonth.get("publicationCount");

                GenericRecord minPublicationMonth = (GenericRecord) valueRecord.get("minPublicationMonth");
                int minMonth = (int) minPublicationMonth.get("month");
                int minPublicationCount = (int) minPublicationMonth.get("publicationCount");

                // Crear un array de MonthPublication
                List<GenericRecord> monthlyPublications = (List<GenericRecord>) valueRecord.get("monthlyPublications");
                List<MonthPublication> monthlyPublicationList = new ArrayList<>();

                for (GenericRecord monthlyPublication : monthlyPublications) {
                    int month = (int) monthlyPublication.get("month");
                    int publicationCount = (int) monthlyPublication.get("publicationCount");
                    monthlyPublicationList.add(new MonthPublication(month, publicationCount));
                }

                // Crear un objeto YearMonthSummary
                YearMonthSummary summary = new YearMonthSummary(
                    new MonthPublication(maxMonth, maxPublicationCount),
                    new MonthPublication(minMonth, minPublicationCount),
                    monthlyPublicationList
                );

                // Agregar el par a la lista
                Pair<Integer, YearMonthSummary> pair = new Pair<>(year, summary);
                records.add(pair.toString());
                records.add("\n");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return records;
    }


     public static  List<String> getPairIntYearDaySummaryRecords(String avroFilePath){

        List<String> records = new ArrayList<>();
        try {

            Schema schema = Pair.getPairSchema(Schema.create(Type.INT), YearDaySummary.getClassSchema());

            GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

            File avroFile = new File(avroFilePath);

            FileReader<GenericRecord> fileReader = DataFileReader.openReader(avroFile, datumReader);

            while (fileReader.hasNext()) {

                GenericRecord record = fileReader.next();

                // Obtener los campos principales
                int year = (int) record.get("key");
                GenericRecord valueRecord = (GenericRecord) record.get("value");

                // Extraer los valores de YearDaySummary
                GenericRecord maxPublicationDay = (GenericRecord) valueRecord.get("maxPublicationDay");
                int maxDay = (int) maxPublicationDay.get("Day");
                int maxPublicationCount = (int) maxPublicationDay.get("publicationCount");

                GenericRecord minPublicationDay = (GenericRecord) valueRecord.get("minPublicationDay");
                int minDay = (int) minPublicationDay.get("Day");
                int minPublicationCount = (int) minPublicationDay.get("publicationCount");

                // Crear un array de DayPublication
                List<GenericRecord> dailyPublications = (List<GenericRecord>) valueRecord.get("dailyPublications");
                List<DayPublication> dailyPublicationList = new ArrayList<>();

                for (GenericRecord dailyPublication : dailyPublications) {
                    int day = (int) dailyPublication.get("Day");
                    int publicationCount = (int) dailyPublication.get("publicationCount");
                    dailyPublicationList.add(new DayPublication(day, publicationCount));
                }

                // Crear un objeto YearMonthSummary
                YearDaySummary summary = new YearDaySummary(
                    new DayPublication(maxDay, maxPublicationCount),
                    new DayPublication(minDay, minPublicationCount),
                    dailyPublicationList
                );

                // Agregar el par a la lista
                Pair<Integer, YearDaySummary> pair = new Pair<>(year, summary);
                records.add(pair.toString());
                records.add("\n");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return records;
    }

    public static  List<String> getPairIntDayValueRecords(String avroFilePath){

        List<String> records = new ArrayList<>();
        try {

            Schema schema = Pair.getPairSchema(Schema.create(Type.INT),DayValue.getClassSchema());

            GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

            File avroFile = new File(avroFilePath);

            FileReader<GenericRecord> fileReader = DataFileReader.openReader(avroFile, datumReader);

            while (fileReader.hasNext()) {

                GenericRecord record = fileReader.next();

                // Obtener los campos principales
                int year = (int) record.get("key");
                GenericRecord valueRecord = (GenericRecord) record.get("value");

                int day = (int) valueRecord.get("day");
                String albums = valueRecord.get("albums").toString();

                Pair<Integer, DayValue> pair = new Pair<>(year, new DayValue(day, albums));

                records.add(pair.toString());
                records.add("\n");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return records;
    }

    public static  List<String> getPairIntAlbumValueRecords(String avroFilePath){

        List<String> records = new ArrayList<>();
        try {

            Schema schema = Pair.getPairSchema(Schema.create(Type.INT),MonthValue.getClassSchema());

            GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

            File avroFile = new File(avroFilePath);

            FileReader<GenericRecord> fileReader = DataFileReader.openReader(avroFile, datumReader);

            while (fileReader.hasNext()) {

                GenericRecord record = fileReader.next();

                // Obtener los campos principales
                int year = (int) record.get("key");
                GenericRecord valueRecord = (GenericRecord) record.get("value");

                int month = (int) valueRecord.get("month");
                String albums = valueRecord.get("albums").toString();

                Pair<Integer, MonthValue> pair = new Pair<>(year, new MonthValue(month, albums));

                records.add(pair.toString());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return records;
    }

    /**
     * Deserializa los datos de un archivo Avro.
     * @param avroFilePath La ruta al archivo Avro.
     * @return Una lista de registros deserializados.
     */
    public static List<String> getRecords(String avroFilePath, String keyDataType, String valueDataType) 
    {
        List<String> records = new ArrayList<>();
        try {
            if(keyDataType.equals("int") && valueDataType.equals("int")){
                
            // Definir el esquema clave-valor
            String schemaString = "{\"type\":\"record\",\"name\":\"Pair\",\"fields\":[{\"name\":\"key\",\"type\":\"int\"},{\"name\":\"value\",\"type\":\"int\"}]}";
            Schema schema = new Schema.Parser().parse(schemaString);

            // Crear un lector de datos genéricos
            GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

            // Leer el archivo Avro
            File avroFile = new File(avroFilePath);
            FileReader<GenericRecord> fileReader = DataFileReader.openReader(avroFile, datumReader);

            // Iterar sobre los registros y deserializarlos
            while (fileReader.hasNext()) {
                // Leer el siguiente registro
                GenericRecord record = fileReader.next();
                // Crear un par clave-valor y obtener los valores de la clave y el valor del registro
                Pair<Integer, Integer> pair = new Pair<>(record.get("key"), record.get("value"));
                // Imprimir el par clave-valor en la consola
                records.add(pair.toString());
               // System.out.println(pair.toString());
            }

            // Cerrar el lector de archivos
            fileReader.close();
        } else if(keyDataType.equals("string")&& valueDataType.equals("int")){
            // Definir el esquema clave-valor
            String schemaString = "{\"type\":\"record\",\"name\":\"Pair\",\"fields\":[{\"name\":\"key\",\"type\":\"string\"},{\"name\":\"value\",\"type\":\"int\"}]}";
            Schema schema = new Schema.Parser().parse(schemaString);

            // Crear un lector de datos genéricos
            GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

            // Leer el archivo Avro
            File avroFile = new File(avroFilePath);
            FileReader<GenericRecord> fileReader = DataFileReader.openReader(avroFile, datumReader);

            // Iterar sobre los registros y deserializarlos
            while (fileReader.hasNext()) {
                // Leer el siguiente registro
                GenericRecord record = fileReader.next();
                // Crear un par clave-valor y obtener los valores de la clave y el valor del registro
                Pair<CharSequence, Integer> pair = new Pair<>(record.get("key"), record.get("value"));
                // Imprimir el par clave-valor en la consola
                records.add(pair.toString());
               // System.out.println(pair.toString());
            }

            // Cerrar el lector de archivos
            fileReader.close();
        }else if(keyDataType.equals("int")&& valueDataType.equals("string")){
            // Definir el esquema clave-valor
            String schemaString = "{\"type\":\"record\",\"name\":\"Pair\",\"fields\":[{\"name\":\"key\",\"type\":\"int\"},{\"name\":\"value\",\"type\":\"string\"}]}";
            Schema schema = new Schema.Parser().parse(schemaString);

            // Crear un lector de datos genéricos
            GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

            // Leer el archivo Avro
            File avroFile = new File(avroFilePath);
            FileReader<GenericRecord> fileReader = DataFileReader.openReader(avroFile, datumReader);

            // Iterar sobre los registros y deserializarlos
            while (fileReader.hasNext()) {
                // Leer el siguiente registro
                GenericRecord record = fileReader.next();
                // Crear un par clave-valor y obtener los valores de la clave y el valor del registro
                Pair<Integer, CharSequence> pair = new Pair<>(record.get("key"), record.get("value"));
                // Imprimir el par clave-valor en la consola
                records.add(pair.toString());
               // System.out.println(pair.toString());
            }

            // Cerrar el lector de archivos
            fileReader.close();
        }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return records;
    }

    /**
     * Deserializa los datos de un archivo Avro.
     * @param args Argumentos de la línea de comandos. Se espera un argumento: la ruta al archivo Avro.
     * @throws IOException Si ocurre un error al leer el archivo Avro.
     * @see Pair
     */
    public static void main(String[] args) 
    {
        if (args.length != 3) {
            System.out.println("Usage: DeserializationData <avro-file> <keyDataType> <valueDataType>");
            System.exit(1);
        }

        String avroFilePath = args[0];
        String keyDataType = args[1];
        String valueDataType = args[2];

        List<String> records = getRecords(avroFilePath, keyDataType, valueDataType);
    }
}