package util.datamigration.parser;

import java.io.File;
import java.io.IOException;
import java.time.Instant;

import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class CSVtoParquet implements IParser{

	
	private String sourceDirectory = "D:\\eodhistdata\\csv\\ready";
	private String stagingDirectory = "C:\\temp\\eqdata.stage";
	private String finalDirecotry = "C:\\temp\\eqdata.final";
	
	private Dataset<Row> existing = null;
	private String schema = "EXCHANGE string,SYMBOL string,TRADEDATE string,FREQ string,"
			+ "OPENPX double,HIGH double,LOW double,CLOSEPX double,PREVCLOSE double,ADJCLOSEPX double,TOTTRDQTY long,TOTTRDVAL double,"
			+ "TRUERANGE double,ATR double,SMA5 double,SMA20 double,SMA50 double,SMA100 double,SMA200 double,EMA5 double,EMA20 double,"
			+ "EMA50 double,EMA100 double,EMA200 double,STOCH_K double,STOCH_D double,DC_HIGH double,DC_LOW double";

	@Override
	public void parse() {
		SparkSession spark = SparkSession.builder().appName("Data Migration")
				.config("spark.master", "local[*]")
				.config("spark.sql.sources.partitionOverwriteMode","dynamic")
				.config("spark.memory.storageFraction","0.3")
				.config("spark.memory.fraction","0.8")
				.getOrCreate();
		File path = new File(sourceDirectory);
		
	    File [] files = path.listFiles();
	    for (int i = 0; i < files.length; i++){
	        if (files[i].isFile() && files[i].getName().endsWith("csv")){ //this line weeds out other directories/folders
	        	readCSV(spark,files[i].getAbsolutePath());
	        	System.out.println("Completed for File - "+files[i].getName()+ " - " + Instant.now());
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
	    
		try {
			FileUtils.deleteDirectory(new File(stagingDirectory));
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("Compression Complete !!"+ Instant.now());
	    spark.stop();
	}
	
	private void readCSV(SparkSession spark,String file) {
		Dataset<Row> records = spark.read().format("csv")
				.schema(schema)
				.option("header", "true")
				.load(file);
		if(existing  !=null ) {
			existing = existing.union(records);
		}
		else {
			existing = records;
		}
			
	}
	
//	private void compact(SparkSession spark) {
//		spark.read().schema(schema).parquet(stagingDirectory).repartition(new Column("EXCHANGE")).orderBy("EXCHANGE","SYMBOL","TRADEDATE","FREQ")
//		.write().mode(SaveMode.Append).partitionBy("EXCHANGE").parquet(finalDirecotry);
//		
//	}

}
