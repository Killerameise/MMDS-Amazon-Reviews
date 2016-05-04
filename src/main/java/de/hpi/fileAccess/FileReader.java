package de.hpi.fileAccess;

import de.hpi.database.MetadataRecord;
import de.hpi.database.ReviewRecord;
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
    private final Path filePath;
    private final Charset charset = Charset.forName("US-ASCII");

    public FileReader(final String file) {
        filePath = Paths.get(file);
    }

    public List<ReviewRecord> readReviewsFromFile() {
        LinkedList<ReviewRecord> reviewRecordList = new LinkedList<>();
        try (BufferedReader reader = Files.newBufferedReader(filePath, charset)) {
            String line;
            while ((line = reader.readLine()) != null) {
                reviewRecordList.add(JsonReader.readReviewJson(line));
            }
        } catch (IOException x) {
            System.err.format("IOException: %s%n", x);
        }
        return reviewRecordList;
    }

    public List<MetadataRecord> readMetadataFromFile() {
        LinkedList<MetadataRecord> metadataRecordList = new LinkedList<>();
        try (BufferedReader reader = Files.newBufferedReader(filePath, charset)) {
            String line;
            while ((line = reader.readLine()) != null) {
                metadataRecordList.add(JsonReader.readMetadataJson(line));
            }
        } catch (IOException x) {
            System.err.format("IOException: %s%n", x);
        }
        return metadataRecordList;
    }
}
