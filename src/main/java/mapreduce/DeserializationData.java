package mapreduce;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.Pair;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class DeserializationData {

    /**
     * Deserializa los datos de un archivo Avro.
     * @param avroFilePath La ruta al archivo Avro.
     * @return Una lista de registros deserializados.
     */
    public static List<String> getRecords(String avroFilePath, String keyDataType) {
        List<String> records = new ArrayList<>();
        try {
            if(keyDataType.equals("int")){
                
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
                System.out.println(pair.toString());
            }

            // Cerrar el lector de archivos
            fileReader.close();
        } else if(keyDataType.equals("string")){
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
                System.out.println(pair.toString());
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
    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: DeserializationData <avro-file>");
            System.exit(1);
        }

        String avroFilePath = args[0];
        String keyDataType = args[1];

        List<String> records = getRecords(avroFilePath, keyDataType);

        for (String record : records) {
            System.out.println(record);
        }
    }
}