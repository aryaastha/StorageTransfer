package Process;

import Bean.DerivedFields;
import Operation.DataManipulation;
import Utils.GsonService;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

public class Derivation implements Processes, Serializable {
    private DerivedFields[] derivedFields;

    public Derivation(JsonArray preProcessJson) {
        this.derivedFields = GsonService.getInstance().getGson().fromJson(preProcessJson, DerivedFields[].class);
    }

    public JavaRDD<JsonObject> apply(JavaRDD<JsonObject> input) {
        return input.map(new Function<JsonObject, JsonObject>() {
            public JsonObject call(JsonObject v1) throws Exception {
                JsonObject newObject = v1;
                for (DerivedFields derive : derivedFields) {
                    Method declaredMethod = DataManipulation.class.getDeclaredMethod(derive.getFunction(), JsonObject.class, List.class);
                    newObject.add(derive.getNewfield(), new JsonPrimitive((String) declaredMethod.invoke(DataManipulation.class.newInstance(), v1, Arrays.asList(derive.getOldFields()))));
                }
                return newObject;
            }
        });
    }
}
