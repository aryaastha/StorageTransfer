package Process;

import Operation.Operation;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Created by astha.a on 17/05/17.
 */
public class Pivot implements Processes, Serializable {
    String prefix;
    String[] pivotFields;
    String delimiter;
    String column;

    public Pivot(JsonObject preProcessJson) {
        prefix = preProcessJson.get("prefix").getAsString();
        delimiter = preProcessJson.get("delimiter").getAsString();
        column = preProcessJson.get("column").getAsString();
        JsonArray pivotFields = preProcessJson.get("pivotFields").getAsJsonArray();
        int arraySize = pivotFields.size();
        this.pivotFields = new String[arraySize];

        for (int i = 0; i < arraySize; i++) {
            this.pivotFields[i] = pivotFields.get(i).getAsString();
        }

    }

    public JavaRDD<JsonObject> apply(JavaRDD<JsonObject> input) {
        JavaRDD<JsonObject> map = input.mapToPair(new PairFunction<JsonObject, String, JsonObject>() {
            public Tuple2<String, JsonObject> call(JsonObject jsonObject) throws Exception {
                return new Tuple2<String, JsonObject>(jsonObject.get("Key").getAsString(), jsonObject);
            }
        }).groupByKey().map(new Function<Tuple2<String, Iterable<JsonObject>>, JsonObject>() {
            public JsonObject call(Tuple2<String, Iterable<JsonObject>> v1) throws Exception {
                JsonObject jsonObject = new JsonObject();
                Iterator<JsonObject> iterator = v1._2().iterator();
                jsonObject.add("Key", new JsonPrimitive(v1._1()));
                JsonObject rankValues = new JsonObject();
                while (iterator.hasNext()) {
                    JsonObject next = iterator.next();
                    String key2 = prefix + new Operation("concat", delimiter, 0).operate(next, pivotFields);
                    String value2 = next.get(column).getAsString();
                    rankValues.add(key2, new JsonPrimitive(value2));
                }

                jsonObject.add("rankValues", rankValues);

                return jsonObject;
            }
        });

        return map;
    }
}
