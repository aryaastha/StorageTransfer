package DataSource;

import Bean.FilterDetails;
import Utils.QueryBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by astha.a on 20/04/17.
 */

public class Hive implements Datasources, Serializable {
    private static final Logger logger = Logger.getLogger(Hive.class);
    private String tableName;
    private String[] whiteList;  //for hive as sink, whiteList is used for schema
    private FilterDetails[] filters;


    static private HiveContext hivecontext;

    private StructType createSchema() {
        StringBuilder schemaString = new StringBuilder();
        schemaString.append("Key");
        for (String col : whiteList) {
            schemaString.append(" ").append(col);
        }

        List<StructField> fields = new ArrayList<StructField>();
        for (String fieldName : schemaString.toString().split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        return schema;
    }

    private void init(JavaSparkContext sparkContext) {
        hivecontext = new HiveContext(sparkContext);
    }


    public JavaRDD<JsonObject> get(JavaSparkContext sparkContext) {
        String query = QueryBuilder.builder(filters, whiteList, tableName).toString();

        if (hivecontext == null) {
            init(sparkContext);
        }

        JavaRDD<Row> rowJavaRDD = hivecontext.sql(query).javaRDD();
        JavaRDD<JsonObject> result = rowJavaRDD.map(new Function<Row, JsonObject>() {
            public JsonObject call(Row v1) throws Exception {
                JsonObject jsonObject = new JsonObject();
                for (String element : whiteList) {
                    jsonObject.add(element, new JsonPrimitive((v1.getAs(element) == null) ? "" : v1.getAs(element).toString()));
                }
                return jsonObject;
            }
        });
        return result;
    }


    //ToDo pass schema for the new hive table, for now all columns are string
    public void put(JavaRDD<JsonObject> dataForSink, JavaSparkContext context) {
        if (hivecontext == null) {
            init(context);
        }

        //Map to row
        JavaRDD<Row> key = dataForSink.map(new Function<JsonObject, Row>() {
            public Row call(JsonObject v1) throws Exception {
                ArrayList<String> columns = new ArrayList<String>();
                columns.add(v1.get("Key").getAsString());
                for (String col : whiteList) {
                    try {
                        columns.add(v1.get(col).getAsString());
                    } catch (Exception e) {
                        columns.add("NA");
                        logger.info("the current key is not present");
                        e.printStackTrace();
                    }
                }
                Object[] objectColu = columns.toArray();
                Row row = RowFactory.create(objectColu);
                return row;
            }
        });

        StructType schema = createSchema();

        DataFrame dataFrame = hivecontext.createDataFrame(key, schema);
        dataFrame.write().mode(SaveMode.Overwrite).saveAsTable(tableName);
    }
}
