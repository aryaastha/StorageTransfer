package Bean;

import scala.Serializable;

/**
 * Created by astha.a on 01/09/17.
 */
public class Columns implements Serializable {
    private String columnName;
    private String[] fields;

    public String getColumnName() {
        return columnName;
    }

    public String[] getFields() {
        return fields;
    }
}
