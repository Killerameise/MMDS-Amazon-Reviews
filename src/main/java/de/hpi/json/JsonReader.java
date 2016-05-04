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

    private static Gson gson = new Gson();

    public static ReviewRecord readReviewJson(String json) {
        Type type = new TypeToken<ReviewRecord>() {}.getType();
        return gson.fromJson(json, type);
    }

    public static MetadataRecord readMetadataJson(String json) {
        Type type = new TypeToken<MetadataRecord>() {}.getType();
        return gson.fromJson(json, type);
    }

}
