package Factory;

import DataSource.Datasources;
import DataSource.Hbase;
import DataSource.Hive;
import Utils.Constants;
import Utils.FileUtils;
import Utils.GsonService;
import Utils.JobProperties;

/**
 * Created by astha.a on 08/12/17.
 */
public class SinkFactory {
    public static Datasources getSinkObject() {
        if (JobProperties.getInstance().getProperty(Constants.SINK_TYPE).equals("HiveSink")) {
            String sourceProperties = FileUtils.readFileToString(JobProperties.getInstance().getProperty(Constants.SINK_PROPERTIES));
            Hive hiveSink = GsonService.getInstance().getGson().fromJson(sourceProperties, DataSource.Hive.class);
            return hiveSink;
        } else if (JobProperties.getInstance().getProperty(Constants.SINK_TYPE).equalsIgnoreCase("HbaseSink")) {
            String sourceProperties = FileUtils.readFileToString(JobProperties.getInstance().getProperty(Constants.SINK_PROPERTIES));
            Hbase hbase = GsonService.getInstance().getGson().fromJson(sourceProperties, Hbase.class);
            return hbase;
        } else return null;
    }
}
