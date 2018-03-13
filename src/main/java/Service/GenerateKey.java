package Service;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import java.io.Serializable;

/**
 * Created by astha.a on 10/05/17.
 */
public class GenerateKey implements Serializable {
    String fields[];
    String delimiter;

    public void generateKey(JsonObject row, JsonObject jsonObject) {
        String key = "";
        if (fields.length >= 1) {
            key += row.get(fields[0]).getAsString();
        }
        for (int i = 1; i < fields.length; i++) {
            key += delimiter;
            key += row.get(fields[i]).getAsString();
        }

        jsonObject.add("Key", new JsonPrimitive(key));
    }
}
