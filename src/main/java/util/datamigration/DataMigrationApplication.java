package util.datamigration;

import java.io.File;
import java.io.IOException;
import java.time.Instant;

import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class DataMigrationApplication {

	private static String sourceDirectory = "D:\\eodhistdata\\csv\\ready";
	private static String stagingDirectory = "C:\\temp\\eqdata.stage";
	private static String finalDirecotry = "C:\\temp\\eqdata.final";
	
	private static Dataset<Row> existing = null;
	private static String schema = "EXCHANGE string,SYMBOL string,TRADEDATE string,FREQ string,"
			+ "OPENPX double,HIGH double,LOW double,CLOSEPX double,PREVCLOSE double,ADJCLOSEPX double,TOTTRDQTY long,TOTTRDVAL double,"
			+ "TRUERANGE double,ATR double,SMA5 double,SMA20 double,SMA50 double,SMA100 double,SMA200 double,EMA5 double,EMA20 double,"
			+ "EMA50 double,EMA100 double,EMA200 double,STOCH_K double,STOCH_D double,DC_HIGH double,DC_LOW double";
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Data Migration")
				.config("spark.master", "local[3]")
				.config("spark.sql.sources.partitionOverwriteMode","dynamic")
//				.config("spark.memory.storageFraction","0.3")
//				.config("spark.memory.fraction","0.8")
//		      .config("spark.some.config.option", "some-value")
				.getOrCreate();
		File path = new File(sourceDirectory);
		
	    File [] files = path.listFiles();
	    for (int i = 0; i < files.length; i++){
	        if (files[i].isFile() && files[i].getName().endsWith("csv")){ //this line weeds out other directories/folders
	        	readCSV(spark,files[i].getAbsolutePath());
	        	System.out.println("Completed for File - "+files[i].getName()+ " - " + Instant.now());
//	        	files[i].delete();
	        }
	    }
	    existing.na().fill(0.0, new String[] {"PREVCLOSE","ADJCLOSEPX","TOTTRDVAL"}).orderBy("EXCHANGE","SYMBOL","TRADEDATE","FREQ")
	    	.write().mode(SaveMode.Append).partitionBy("EXCHANGE").parquet(finalDirecotry);
	    System.out.println("Processed All files !!"+ Instant.now());
	    for (int i = 0; i < files.length; i++){
	        if (files[i].isFile() && files[i].getName().endsWith("csv")){ 
	        	files[i].delete();
	        }
	    }
//		compact(spark);
//		long cnt2 = spark.read().schema(schema).parquet(finalDirecotry).count();
//		System.out.println("Compressed count - "+ cnt2);
		try {
			FileUtils.deleteDirectory(new File(stagingDirectory));
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("Compression Complete !!"+ Instant.now());
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
		if(existing  !=null ) {
			existing = existing.union(records);
		}
		else {
			existing = records;
		}
			
	}
	
	private static void compact(SparkSession spark) {
		spark.read().schema(schema).parquet(stagingDirectory).repartition(new Column("EXCHANGE")).orderBy("EXCHANGE","SYMBOL","TRADEDATE","FREQ")
		.write().mode(SaveMode.Append).partitionBy("EXCHANGE").parquet(finalDirecotry);
		
	}

}
