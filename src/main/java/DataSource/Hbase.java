package DataSource;

import Bean.FilterDetails;
import Utils.HbaseUtil;
import Utils.QueryBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Created by astha.a on 06/12/17.
 */
public class Hbase implements Datasources, Serializable {
    private String tableName;
    private String[] whiteList;
    private FilterDetails[] filters;
    private HbaseUtil hbaseUtil;

    private JavaRDD<JsonObject> getBulkFromHbase(JavaRDD<JsonObject> keys) {
        JavaRDD<JsonObject> jsonObjectJavaRDD = keys.mapPartitions(new FlatMapFunction<Iterator<JsonObject>, JsonObject>() {
            public Iterable<JsonObject> call(Iterator<JsonObject> jsonObjectIterator) throws Exception {
                return hbaseUtil.getBulk(jsonObjectIterator, whiteList);
            }
        });
        return jsonObjectJavaRDD;
    }

    private JavaRDD<JsonObject> getKeysFromHive(JavaSparkContext sparkContext) {
        HiveContext hiveContext = new HiveContext(sparkContext);
        String queryForKeys = QueryBuilder.builder(filters, whiteList, tableName).toString();
        JavaRDD<JsonObject> keys = hiveContext.sql(queryForKeys).toJavaRDD().map(new Function<Row, JsonObject>() {
            public JsonObject call(Row v1) throws Exception {
                JsonObject json = new JsonObject();
                for (String field : whiteList) {
                    json.add(field, new JsonPrimitive(v1.getAs(field).toString()));
                }
                return json;
            }
        });
        return keys;
    }

    public JavaRDD<JsonObject> get(JavaSparkContext sparkContext) {
        JavaRDD<JsonObject> keys = getKeysFromHive(sparkContext);
        JavaRDD<JsonObject> bulkFromHbase = getBulkFromHbase(keys);
        return bulkFromHbase;
    }

    public void put(JavaRDD<JsonObject> dataForSink, JavaSparkContext context) {
        dataForSink.foreachPartition(new VoidFunction<Iterator<JsonObject>>() {
            public void call(Iterator<JsonObject> jsonObjectIterator) throws Exception {
                hbaseUtil.putBulk(jsonObjectIterator);
            }
        });
    }

}
