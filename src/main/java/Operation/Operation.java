package Operation;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import java.io.Serializable;

/**
 * Created by astha.a on 12/05/17.
 */
public class Operation implements Serializable {
    String type;
    String delimiter;
    String[] headers;
    int headerFlag;

    public Operation(String type, String delimiter, int headerFlag) {
        this.headerFlag = headerFlag;
        this.type = type;
        this.delimiter = delimiter;
    }

    public Operation(String type, String delimiter, String[] headers, int headerFlag) {
        this.headerFlag = headerFlag;
        this.headers = headers;
        this.type = type;
        this.delimiter = delimiter;
    }

    public String concatWithHeader(JsonObject row, String[] fields) {
        String column = "";
        if (fields.length >= 1) {
            column += headers[0] + ":" + row.get(fields[0]).getAsString();
        }
        for (int i = 1; i < fields.length; i++) {
            column += delimiter;
            column += headers[i] + ":" + row.get(fields[i]).getAsString();
        }
        return column;
    }

    public String concatWithoutHeader(JsonObject row, String[] fields) {
        String column = "";

        if (fields.length >= 1) {
            if (row.get(fields[0]).isJsonArray()) {
                JsonArray asJsonArray = row.get(fields[0]).getAsJsonArray();
                for (int i = 0; i < asJsonArray.size(); i++) {
                    column += asJsonArray.get(i);
                }
            } else
                column += row.get(fields[0]).getAsString();
        }

        for (int i = 1; i < fields.length; i++) {
            column += delimiter;
            if (row.get(fields[i]).isJsonArray()) {
                JsonArray asJsonArray = row.get(fields[i]).getAsJsonArray();
                for (int j = 0; j < asJsonArray.size(); j++) {
                    column += asJsonArray.get(j);
                }
            } else
                column += row.get(fields[0]).getAsString();
        }

        return column;
    }

    public String operate(JsonObject row, String[] fields) {
        if (type.equals("json")) {
            JsonObject jsonObject = new JsonObject();
            for (String field : fields) {
                jsonObject.add(field, new JsonPrimitive(row.get(field).getAsString()));
            }
            return jsonObject.toString();
        }
        if (type.equals("concat")) {
            if (headerFlag == 0) {
                return concatWithoutHeader(row, fields);
            } else if (headerFlag == 1) {
                return concatWithHeader(row, fields);
            }
        }
        return null;
    }
}
