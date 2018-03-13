package Process;

import Bean.Columns;
import Utils.GsonService;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by astha.a on 17/05/17.
 */
public class Aggregation implements Processes, Serializable {
    private ArrayList<String> field;
    private ArrayList<String> keyElements;
    private Columns[] columns;

    public Aggregation(JsonObject preProcessJson) {
//        System.out.println("Preprocessjson : " + preProcessJson);
        this.field = GsonService.getInstance().getGson().fromJson(preProcessJson.getAsJsonArray("field"), ArrayList.class);
        this.keyElements = GsonService.getInstance().getGson().fromJson(preProcessJson.getAsJsonArray("keyElements"), ArrayList.class);
//        System.out.println("Length of keyElements : " + keyElements.size());
        this.columns = GsonService.getInstance().getGson().fromJson(preProcessJson.get("columns").getAsJsonArray(), Columns[].class);
    }

    public JavaRDD<JsonObject> apply(JavaRDD<JsonObject> input) {

        JavaRDD<JsonObject> map = input.map(new Function<JsonObject, JsonObject>() {
            public JsonObject call(JsonObject v1) throws Exception {
                JsonObject newObject = new JsonObject();

                for (Columns column : columns) {
                    JsonObject temp = new JsonObject();
                    for (String field : column.getFields()) {
                        temp.add(field, new JsonPrimitive(v1.get(field).getAsString()));
                    }
                    newObject.add(column.getColumnName(), temp);
                }
                newObject.addProperty("agg_key", getAggregationField(field, v1));
                for (String element : keyElements) {
                    newObject.add(element, v1.get(element));
                }
                return newObject;
            }
        });

        JavaPairRDD<String, JsonArray> inputPair = map.mapToPair(new PairFunction<JsonObject, String, JsonArray>() {
            public Tuple2<String, JsonArray> call(JsonObject jsonObject) throws Exception {
                JsonArray jsonArray = new JsonArray();
                jsonArray.add(jsonObject);
                return new Tuple2<String, JsonArray>(jsonObject.get("agg_key").toString(), jsonArray);
            }
        }).reduceByKey(new Function2<JsonArray, JsonArray, JsonArray>() {
            public JsonArray call(JsonArray v1, JsonArray v2) throws Exception {
                JsonArray jsonArray = new JsonArray();
                for (int i = 0; i < v1.size(); i++) {
                    jsonArray.add(v1.get(i));
                }

                for (int i = 0; i < v2.size(); i++) {
                    jsonArray.add(v2.get(i));
                }
                return jsonArray;
            }
        });

        return inputPair.map(new Function<Tuple2<String, JsonArray>, JsonObject>() {
            public JsonObject call(Tuple2<String, JsonArray> v1) throws Exception {
                JsonObject newObject = new JsonObject();
                JsonObject object = v1._2.get(0).getAsJsonObject();
                for (String element : keyElements) {
                    newObject.add(element, object.get(element));
                }


                for (Columns col : columns) {
                    JsonArray jsonElements = new JsonArray();
                    for (JsonElement element : v1._2) {
                        jsonElements.add(element.getAsJsonObject().get(col.getColumnName()));
                    }
//                    System.out.println("JsonElements : " + jsonElements.toString());
                    newObject.add(col.getColumnName(), new JsonPrimitive(jsonElements.toString()));
//                    System.out.println("Intermediate value of col :" + col.getColumnName() + " is : " + jsonElements.toString());
                }

//                System.out.println("V1 : " + v1);
//                System.out.println("New Object : " + newObject.toString());

                return newObject;
            }
        });
    }

    private String getAggregationField(ArrayList<String> fields, JsonObject input) {
        StringBuilder stringBuilder = new StringBuilder();
        for (String field : fields) {
            stringBuilder.append(input.get(field).getAsString());
        }
        return stringBuilder.toString();
    }
}
