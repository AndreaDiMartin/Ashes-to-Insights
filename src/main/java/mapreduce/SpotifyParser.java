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

    /**
     * Parse the records in a text file.
     * @param value
     * @return A list of records.
     * @throws IOException
     */
    public List<String[]> parse(Text value) throws IOException {
        // Create a list to store the records
        List<String[]> records = new ArrayList<>();

        // Create a configuration object and a file system object
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // Get the path to the file
        Path path = new Path(value.toString());
        FSDataInputStream inputStream = null;
        BufferedReader reader = null;

        // Read the file line by line
        try {
            inputStream = fs.open(path);
            reader = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            // Read the rest of the records
            while ((line = reader.readLine()) != null) {
                // Use a more robust CSV parsing method
                String[] fields = parseCSVLine(line);
                records.add(fields);
            }
        } finally {
            // Close the reader and the input stream
            IOUtils.closeStream(reader);
            IOUtils.closeStream(inputStream);
        }
        return records;
    }

    /**
     * Parse a line of CSV data.
     * @param line
     * @return An array of fields.
     */
    private String[] parseCSVLine(String line) {
        // Create a list to store the fields
        List<String> fields = new ArrayList<>();

        // Create a string builder to store the current field
        StringBuilder currentField = new StringBuilder();
        // Create a flag to keep track of whether we're inside quotes
        boolean inQuotes = false;

        // Iterate over the characters in the line
        for (char c : line.toCharArray()) {
            // Check if the character is a quote
            if (c == '"') {
                // If it is, toggle the inQuotes flag
                inQuotes = !inQuotes; // Toggle the inQuotes flag
            } else if (c == ',' && !inQuotes) {
                // If we encounter a comma and we're not inside quotes, it's the end of a field
                fields.add(currentField.toString());
                currentField.setLength(0); // Clear the current field
            } else {
                // Otherwise, just add the character to the current field
                currentField.append(c);
            }
        }
        // Add the last field
        fields.add(currentField.toString());

        // Return the fields as an array
        return fields.toArray(new String[0]);
    }
}
