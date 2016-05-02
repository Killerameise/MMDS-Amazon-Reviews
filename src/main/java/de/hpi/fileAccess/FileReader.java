package de.hpi.fileAccess;

import de.hpi.database.Review;
import de.hpi.json.JsonReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by jaspar.mang on 02.05.16.
 */
public class FileReader {
    final Path filePath;

    public FileReader(final String file) {
        filePath = Paths.get(file);

    }

    public List<Review> readFile() {
        LinkedList<Review> reviewList = new LinkedList<>();
        Charset charset = Charset.forName("US-ASCII");
        try (BufferedReader reader = Files.newBufferedReader(filePath, charset)) {
            String line = null;
            while ((line = reader.readLine()) != null) {
                reviewList.add(JsonReader.readJson(line));
            }
        } catch (IOException x) {
            System.err.format("IOException: %s%n", x);
        }
        return reviewList;
    }
}
