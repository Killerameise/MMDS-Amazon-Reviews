package de.hpi.json;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import de.hpi.database.MetadataRecord;
import de.hpi.database.ReviewRecord;

import java.lang.reflect.Type;

/**
 * Created by jaspar.mang on 02.05.16.
 */
public class JsonReader {

    public static ReviewRecord readReviewJson(String json) {
        Gson gson = new Gson();
        Type type = new TypeToken<ReviewRecord>() {
        }.getType();
        ReviewRecord reviewRecord = gson.fromJson(json, type);
        return reviewRecord;
    }

    public static MetadataRecord readMetadataJson(String json) {
        Gson gson = new Gson();
        Type type = new TypeToken<MetadataRecord>() {
        }.getType();
        MetadataRecord metadataRecord = gson.fromJson(json, type);
        return metadataRecord;
    }

}
