package Process;

import Service.GenerateColumns;
import Service.GenerateKey;
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
 * Created by astha.a on 17/05/17.
 */
public class Mapping implements Processes, Serializable {
    GenerateKey Key;
    List<GenerateColumns> Qualifiers;


    public Mapping(JsonObject preProcessJson) throws ClassNotFoundException {
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
        return input.map(new Function<JsonObject, JsonObject>() {
            public JsonObject call(JsonObject v1) throws Exception {
                JsonObject json = new JsonObject();
                Key.generateKey(v1, json);

                for (GenerateColumns columns : Qualifiers) {
                    columns.generateKey(v1, json);
                }
                return json;
            }
        });

    }
}
