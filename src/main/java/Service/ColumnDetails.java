package Service;

import java.io.Serializable;

/**
 * Created by astha.a on 10/05/17.
 */
public class ColumnDetails implements Serializable {
    String columnFamily;
    String[] customFieldsNames;
    String[] columnNames;
    String[] dumpJson;

    public String[] getDumpJson() {
        return dumpJson;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public String[] getCustomFieldsNames() {
        return customFieldsNames;
    }

    public String[] getColumnNames() {
        return columnNames;
    }
}
