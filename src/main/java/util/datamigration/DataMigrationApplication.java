package util.datamigration;

import java.io.File;
import java.time.Instant;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class DataMigrationApplication {

	private static String schema = "EXCHANGE string,SYMBOL string,TRADEDATE string,FREQ string,"
			+ "OPENPX double,HIGH double,LOW double,CLOSEPX double,PREVCLOSE double,ADJCLOSEPX double,TOTTRDQTY long,TOTTRDVAL double,"
			+ "TRUERANGE double,ATR double,SMA5 double,SMA20 double,SMA50 double,SMA100 double,SMA200 double,EMA5 double,EMA20 double,"
			+ "EMA50 double,EMA100 double,EMA200 double,STOCH_K double,STOCH_D double,DC_HIGH double,DC_LOW double";
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Data Migration")
				.config("spark.master", "local[*]")
//		      .config("spark.some.config.option", "some-value")
				.getOrCreate();

		File path = new File("D:\\eodhistdata\\csv");

	    File [] files = path.listFiles();
	    for (int i = 0; i < files.length; i++){
	        if (files[i].isFile() && files[i].getName().endsWith("csv")){ //this line weeds out other directories/folders
	        	readCSV(spark,files[i].getAbsolutePath());
	        	System.out.println("Completed for File - "+files[i].getName()+ " - " + Instant.now());
	        	files[i].delete();
	        }
	    }
	    System.out.println("Processed All files !!");
	    spark.stop();
		
	}

	private static void readCSV(SparkSession spark,String file) {
		Dataset<Row> records = spark.read().format("csv")
//			    .option("sep", ",")
				.schema(schema)
//				.option("inferSchema", "true")
				.option("header", "true")
				.load(file);
//		System.out.println("Data count - "+records.count());
//		records.printSchema();
		records.na().fill(0.0, new String[] {"PREVCLOSE","ADJCLOSEPX","TOTTRDVAL"})
			.write().mode(SaveMode.Append).partitionBy("EXCHANGE","SYMBOL").parquet("D:\\eodhistdata\\parquet\\eqdata.parquet");
		
		
	}

}
