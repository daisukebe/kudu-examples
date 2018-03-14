package org.kududb.examples.sample;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import java.util.ArrayList;
import java.util.List;

public class Sample {

  private static final String KUDU_MASTER = System.getProperty(
      "kuduMaster", "quickstart.cloudera");
  private static final String TABLE = System.getProperty(
      "tableName", "flume-test");
  private static final String OP = System.getProperty(
      "operation", "create");

  public static void main(String[] args) {
    System.out.println("-------------------------------------");
    System.out.println("Will try to connect to Kudu master at " + KUDU_MASTER);
    System.out.println("to " + OP + " '" + TABLE + "' table.");
    System.out.println("Run with -DkuduMaster=myHost:port -DtableName=givenTableName -Doperation=<create/drop> to override.");
    System.out.println("-------------------------------------");

    KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();

    if (OP.matches("create")) {
      try {
        List<ColumnSchema> columns = new ArrayList(2);
	columns.add(new ColumnSchema.ColumnSchemaBuilder("payload", Type.BINARY)
		    .key(true)
		    .build());
	columns.add(new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING)
		    .nullable(true)
		    .build());
	List<String> rangeKeys = new ArrayList<>();
	rangeKeys.add("payload");
	
	Schema schema = new Schema(columns);
	client.createTable(TABLE, schema,
			   new CreateTableOptions().setRangePartitionColumns(rangeKeys));
	} catch (Exception e) {
	  e.printStackTrace();
      } finally {
	  try {
	      client.shutdown();
	  } catch (Exception e) {
	      e.printStackTrace();
	  }
      }
    } else if (OP.matches("drop")) {
      try {
        client.deleteTable(TABLE);
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        try {
          client.shutdown();
        } catch (Exception e) {
          e.printStackTrace();
	}
      }
    }
  }
}

