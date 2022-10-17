package util.datamigration.parser;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class PhoenixtoParquet implements IParser {

	
//	source.datasource.jdbcUrl=jdbc:phoenix:localhost
//			source.datasource.username=none
//			source.datasource.password=none
//			source.datasource.driver-class-name=org.apache.phoenix.jdbc.PhoenixDriver
//			source.datasource.hikari.connection-init-sql=SELECT 1

	@Override
	public void parse() {
		SparkSession spark = SparkSession.builder().appName("Data Migration")
				.config("spark.master", "local[*]")
				.config("spark.sql.sources.partitionOverwriteMode","dynamic")
				.config("spark.memory.storageFraction","0.3")
				.config("spark.memory.fraction","0.8")
				.getOrCreate();
		
		Dataset<Row> exchange = spark.read()
			      .format("jdbc")
			      .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
			      .option("url", "jdbc:phoenix:localhost")
			      .option("dbtable", "GLOBALDATA.EXCHANGE")
			      .option("user", "none")
			      .option("password", "none")
			      .load();
		exchange.write().mode(SaveMode.Overwrite).parquet("/home/ubuntu/Downloads/eodhistdata/exchange");
		
		Dataset<Row> ticker = spark.read()
			      .format("jdbc")
			      .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
			      .option("url", "jdbc:phoenix:localhost")
			      .option("dbtable", "GLOBALDATA.TICKER")
			      .option("user", "none")
			      .option("password", "none")
			      .load();
		ticker.write().mode(SaveMode.Overwrite).parquet("/home/ubuntu/Downloads/eodhistdata/ticker");
		
		Dataset<Row> splits = spark.read()
			      .format("jdbc")
			      .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
			      .option("url", "jdbc:phoenix:localhost")
			      .option("dbtable", "GLOBALDATA.SPLITS")
			      .option("user", "none")
			      .option("password", "none")
			      .load();
		splits.write().mode(SaveMode.Overwrite).parquet("/home/ubuntu/Downloads/eodhistdata/splits");
		
		// Load data from TABLE1
		
		Dataset<Row> eqdata = spark.read()
			      .format("jdbc")
			      .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
			      .option("url", "jdbc:phoenix:localhost")
			      .option("dbtable", "GLOBALDATA.EQDATA")
			      .option("user", "none")
			      .option("password", "none")
			      .load();
		
		eqdata.createOrReplaceTempView("EQDATA");
		
		
		Dataset<Row> tradeData = spark.sql("select * from EQDATA where EXCHANGE in ('VX') ");
		
		tradeData.orderBy("EXCHANGE", "SYMBOL","TRADEDATE","FREQ").write().partitionBy("EXCHANGE").mode(SaveMode.Append).parquet("/home/ubuntu/Downloads/eodhistdata/eqdata");
		System.out.println("VI data count - "+tradeData.count());
		
		Dataset<Row> parquetData = spark.read().parquet("/home/ubuntu/Downloads/eodhistdata/eqdata");
		
		parquetData.printSchema();
		System.out.println("Loaded data count - "+ parquetData.count());
		
		try {
		Thread.sleep(6000);
		}catch (Exception e) {
		}
		spark.stop();
	

	}

}
