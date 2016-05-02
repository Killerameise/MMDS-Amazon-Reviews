package de.hpi.json;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import de.hpi.database.Review;

import java.lang.reflect.Type;

/**
 * Created by jaspar.mang on 02.05.16.
 */
public class JsonReader {

    public static Review readJson(String json) {
        Gson gson = new Gson();
        Type type = new TypeToken<Review>() {
        }.getType();
        Review review = gson.fromJson(json, type);
        return review;
    }

}
