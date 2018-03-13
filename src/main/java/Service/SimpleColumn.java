package Service;

import Operation.Operation;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import java.io.Serializable;

/**
 * Created by astha.a on 12/05/17.
 */
public class SimpleColumn implements GenerateColumns, Serializable {
    String fields[];
    String columnName;
    Operation operation;

    public void generateKey(JsonObject row, JsonObject jsonObject) {
        String column = operation.operate(row, fields);
        jsonObject.add(columnName, new JsonPrimitive(column));
    }
}
