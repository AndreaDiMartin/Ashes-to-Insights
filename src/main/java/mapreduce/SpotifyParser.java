/**
 * Este archivo contiene la clase SpotifyParser que se encarga de leer un archivo CSV 
 * y parsearlo para obtener los campos.
 */

package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class SpotifyParser {


    public List<String[]> parse(Text value) throws IOException {
        //Crea una lista para almacenar los registros
        List<String[]> records = new ArrayList<>();

        //Crea una configuración y un sistema de archivos
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        //Obtiene el path del archivo
        Path path = new Path(value.toString());
        FSDataInputStream inputStream = null;
        BufferedReader reader = null;

        // Lee el archivo linea por linea
        try {
            inputStream = fs.open(path);
            reader = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while ((line = reader.readLine()) != null) {
                //Se parsea el CSV
                String[] fields = parseCSVLine(line);
                records.add(fields);
            }
        } finally {
            //Se cierran los streams
            IOUtils.closeStream(reader);
            IOUtils.closeStream(inputStream);
        }
        return records;
    }

    private String[] parseCSVLine(String line) {
        //Se crea una lista para almacenar los campos de la linea
        List<String> fields = new ArrayList<>();

        //Se crea un StringBuilder para almacenar el campo actual
        StringBuilder currentField = new StringBuilder();
        // Crea un flag para tomar en cuenta si se está dentro de comillas
        boolean inQuotes = false;

        // Itera por los caracteres de una linea
        for (char c : line.toCharArray()) {
            // Verifica si el caracter es una comilla
            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == ',' && !inQuotes) {
                //Si el caracter es una coma y no está dentro de comillas, agrega el campo actual a la lista
                fields.add(currentField.toString());
                currentField.setLength(0); // Se limpia el campo actual
            } else {
                // Si no es una coma o una comilla, se agrega el caracter al campo actual
                currentField.append(c);
            }
        }
        //Se añade el ultimo campo
        fields.add(currentField.toString());

        //Se retorna la linea parseada como un array
        return fields.toArray(new String[0]);
    }
}
