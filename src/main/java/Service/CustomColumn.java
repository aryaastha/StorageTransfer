package Service;

import Operation.Operation;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import java.io.Serializable;

/**
 * Created by astha.a on 12/05/17.
 */
public class CustomColumn implements GenerateColumns, Serializable {
    String[] columnFields;
    String delimiter;
    String[] fields;
    Operation operation;
    String customFieldName;

    public void generateKey(JsonObject row, JsonObject jsonObject) {

        String Key = (new Operation("concat", delimiter, 0)).operate(row, columnFields);
        JsonObject jsonDump = new JsonObject();
        jsonDump.add(Key, new JsonPrimitive(operation.operate(row, fields)));
        jsonObject.add(customFieldName, jsonDump);
    }
}
