package toolbox.io.spark.documentdb.conf;

import com.microsoft.azure.documentdb.hadoop.ConfigurationUtil;

import org.apache.hadoop.conf.Configuration;

public class DocumentDbIoConf {

  final Configuration conf;

  public DocumentDbIoConf(final String host, final String dbName, final String collectionName, final
  String key) {

    conf = new Configuration();

    conf.set(ConfigurationUtil.DB_HOST, host);
    conf.set(ConfigurationUtil.DB_NAME, dbName);
    conf.set(ConfigurationUtil.OUTPUT_COLLECTION_NAMES, collectionName);
    conf.set(ConfigurationUtil.DB_KEY, key);
  }

  public Configuration getConf() {
    return conf;
  }
}
