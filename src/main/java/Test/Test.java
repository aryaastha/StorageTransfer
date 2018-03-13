package Test;

import Utils.GsonService;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

/**
 * Created by astha.a on 31/08/17.
 */
public class Test {
    public static void main(String[] args) {
        JsonArray array = new JsonArray();
        JsonObject jsonObject = new JsonObject();
        jsonObject.add("key", new JsonPrimitive("testKey1"));
        array.add(jsonObject);

        System.out.println(GsonService.getInstance().getGson().fromJson(array, String.class));

    }
}
