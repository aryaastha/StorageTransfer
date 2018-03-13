package Utils;

import Service.ColumnDetails;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * Created by astha.a on 11/05/17.
 */
public class HbaseUtil implements Serializable {

    String zookeeperQuorum;
    String tableName;
    String zookeeperClientPort;
    List<ColumnDetails> listColumns;

    transient Table table;
    transient Configuration hConf;
    transient Connection connection;

    private synchronized void init() throws IOException {
        if (connection == null) {
            hConf = HBaseConfiguration.create();
            hConf.set("hbase.zookeeper.quorum", zookeeperQuorum);
            hConf.set("hbase.zookeeper.property.clientPort", zookeeperClientPort);
            hConf.set("hbase.client.keyvalue.maxsize", "0");
            connection = ConnectionFactory.createConnection(hConf);
        }
        System.out.println("TableName : " + tableName);
        table = connection.getTable(TableName.valueOf(tableName));
    }

    public Iterable<JsonObject> getBulk(Iterator<JsonObject> javaRdd, String[] whiteList) throws IOException {
        if (connection == null) {
            init();
        }

        List<Get> getList = new ArrayList<Get>();
        while (javaRdd.hasNext()) {
            JsonObject json = javaRdd.next();
            Get get = new Get(Bytes.toBytes(concatKey(json, whiteList)));
            for (ColumnDetails details : listColumns) {
                for (String columnName : details.getColumnNames()) {
                    get.addColumn(Bytes.toBytes(details.getColumnFamily()), Bytes.toBytes(columnName));
                }
            }
            getList.add(get);
        }
        System.out.println("Length of getList : " + getList.size());
        Result[] results = table.get(getList);
        System.out.println("Length of resultList : " + results.length);
        int number_of_nulls = 0;
        List<JsonObject> dataFromHive = new ArrayList<JsonObject>();
        String key, qualifier, value, family;
        int i = 0;
        for (Result res : results) {
            key = Bytes.toString(res.getRow());
            JsonObject jsonObject = new JsonObject();
            try {
                List<Cell> cells = res.listCells();
                for (Cell c : cells) {
//                    family = Bytes.toString(CellUtil.cloneFamily(c));
//                    qualifier = Bytes.toString(CellUtil.cloneQualifier(c));
//                    value = Bytes.toString(CellUtil.cloneValue(c));
                    try {
                        family = Bytes.toString(CellUtil.cloneFamily(c));
                        qualifier = Bytes.toString(CellUtil.cloneQualifier(c));
                        value = Bytes.toString(CellUtil.cloneValue(c));
                        jsonObject.add(family + "-" + qualifier, new JsonPrimitive(value));
                    } catch (Exception e) {
//                        jsonObject.add(family + "-" + qualifier, new JsonPrimitive("NA"));
                        System.out.printf("the given cell was null for family and qualifier ");
                        e.printStackTrace();
                    }
                }
                jsonObject.add("Key", new JsonPrimitive(key));
                dataFromHive.add(jsonObject);
            } catch (Exception e) {
                e.printStackTrace();
                number_of_nulls++;
                System.out.println("Exception for the key : " + Bytes.toString(getList.get(i).getRow()));
            }
            i++;
        }
        System.out.println("Number of nulls : " + number_of_nulls);
        return dataFromHive;
    }

    public void putBulk(Iterator<JsonObject> javaRdd) throws IOException {
        if (connection == null) {
            init();
        }

        List<Put> putList = new ArrayList<Put>();
        while (javaRdd.hasNext()) {
            JsonObject json = javaRdd.next();
            Put put = new Put(Bytes.toBytes(json.get("Key").getAsString()));
            for (ColumnDetails cd : listColumns) {
                String cf = cd.getColumnFamily();
                for (String qualifier : cd.getColumnNames()) {
                    if (json.get(qualifier) != null) {
                        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(qualifier), Bytes.toBytes(json.get(qualifier).getAsString()));
                    }
                }
                String columnName;
                for (String customqualifier : cd.getCustomFieldsNames()) {
                    if (json.get(customqualifier) != null) {
                        columnName = json.get(customqualifier).getAsString();
                        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(columnName), Bytes.toBytes(json.get(columnName).getAsString()));
                    }
                }

                for (String key : cd.getDumpJson()) {
                    JsonObject jsonObject = json.getAsJsonObject(key);
                    Set<Map.Entry<String, JsonElement>> entries = jsonObject.entrySet();
                    for (Map.Entry<String, JsonElement> e : entries) {
                        if (e.getValue() != null) {
                            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(e.getKey()), Bytes.toBytes(e.getValue().getAsString()));
                        }
                    }
                }
            }
            putList.add(put);
        }
        System.out.println("Total PUT keys : " + putList.size());
        table.put(putList);
        table.close();

    }


    //Todo introduce delimiter if key formed from multiple fields
    String concatKey(JsonObject json, String[] whiteList) {
        StringBuilder builder = new StringBuilder();
        for (String field : whiteList) {
            builder.append(json.get(field).getAsString());
        }
        return builder.toString();
    }

}
