package toolbox.io.spark.documentdb;

import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.hadoop.DocumentDBOutputFormat;
import com.microsoft.azure.documentdb.hadoop.DocumentDBWritable;
import com.wizzardo.tools.json.JsonTools;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;

import java.util.Map;

import scala.Tuple2;
import toolbox.io.spark.documentdb.conf.DocumentDbIoConf;

public class DocumentDbIo {

  public static void saveToDocumentDb(final DocumentDbIoConf conf, final JavaRDD<Map<String,
      Object>> rdd) {

    final JavaPairRDD<Writable, DocumentDBWritable> documentDbRdd = rdd.mapToPair(

        new PairFunction<Map<String, Object>, Writable, DocumentDBWritable>() {

          @Override
          public Tuple2<Writable, DocumentDBWritable> call(Map<String, Object> object) throws
                                                                                       Exception {

            String elementId = object.remove("id").toString();
            return new Tuple2<Writable, DocumentDBWritable>(new Text(elementId),
                                                            new DocumentDBWritable(
                                                                new Document(
                                                                    JsonTools.serialize(
                                                                        object
                                                                    )
                                                                )
                                                            )
            );
          }
        });

    documentDbRdd.saveAsNewAPIHadoopFile("", Writable.class, DocumentDBWritable.class,
                                         DocumentDBOutputFormat.class, conf.getConf());
  }

}
