package Factory;

import DataSource.Datasources;
import DataSource.Hbase;
import DataSource.Hive;
import Utils.Constants;
import Utils.FileUtils;
import Utils.GsonService;
import Utils.JobProperties;

/**
 * Created by astha.a on 10/05/17.
 */
public class SourceFactory {
    public static Datasources getSourceObject() {
        if (JobProperties.getInstance().getProperty(Constants.SOURCE_TYPE).equals("HiveSource")) {
            String sourceProperties = FileUtils.readFileToString(JobProperties.getInstance().getProperty(Constants.SOURCE_PROPERTIES));
            Hive hiveSource = GsonService.getInstance().getGson().fromJson(sourceProperties, Hive.class);
            return hiveSource;
        } else if (JobProperties.getInstance().getProperty(Constants.SOURCE_TYPE).equalsIgnoreCase("HbaseSource")) {
            String sourceProperties = FileUtils.readFileToString(JobProperties.getInstance().getProperty(Constants.SOURCE_PROPERTIES));
            Hbase hbase = GsonService.getInstance().getGson().fromJson(sourceProperties, Hbase.class);
            return hbase;
        } else return null;
    }

}
