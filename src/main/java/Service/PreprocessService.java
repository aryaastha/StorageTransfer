package Service;

import Utils.GsonService;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by astha.a on 10/05/17.
 */
public class PreprocessService implements Serializable {
    GenerateKey Key;
    List<GenerateColumns> Qualifiers;


    public PreprocessService(JsonObject preProcessJson) throws ClassNotFoundException {
        Key = GsonService.getInstance().getGson().fromJson(preProcessJson.get("Key").getAsJsonObject(), GenerateKey.class);
        JsonArray qualifiers = preProcessJson.get("Qualifiers").getAsJsonArray();
        Qualifiers = new ArrayList<GenerateColumns>();
        for (JsonElement jsonElement : qualifiers) {
            JsonObject asJsonObject = jsonElement.getAsJsonObject();
            Class QualifierType = Class.forName("Service." + jsonElement.getAsJsonObject().get("qualifierType").getAsString());
            Qualifiers.add((GenerateColumns) GsonService.getInstance().getGson().fromJson(asJsonObject.get("properties").getAsJsonObject(), QualifierType));
        }
    }

    public JavaRDD<JsonObject> apply(JavaRDD<JsonObject> input) {
        JavaRDD<JsonObject> transformedRDD = input.map(new Function<JsonObject, JsonObject>() {
            public JsonObject call(JsonObject row) throws Exception {
                JsonObject json = new JsonObject();
                Key.generateKey(row, json);

                for (GenerateColumns columns : Qualifiers) {
                    columns.generateKey(row, json);
                }
                return json;
            }
        });
        return transformedRDD;
    }
}
