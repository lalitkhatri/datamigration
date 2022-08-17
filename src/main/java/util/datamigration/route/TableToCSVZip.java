package util.datamigration.route;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class TableToCSVZip extends RouteBuilder {
	
	@Value("${outputType}")
	private String outputType;

	@Value("${filePath}")
	private String filePath;
	
	@Value("${tableName}")
	private String tableName;
	
	@Value("${schema}")
	private String schema;
	
	private static int count = 0;
	

	@Value("${csvSize}")
	private int csvSize;
	
	
	@Override
	public void configure() throws Exception {
		from("sql:SELECT * FROM "+schema+"."+tableName+"?outputType="+outputType+"&repeatCount=1")
		.split(body()).streaming()
			.marshal().csv()
			.process(new Processor() {

					@Override
					public void process(Exchange exchange) throws Exception {
						exchange.getIn().setHeader("CamelFilePath", filePath);
						exchange.getIn().setHeader("CamelFileName", tableName+"_"+(count++/csvSize)+".csv");
//						System.out.println(count);
					}
				
				})
//			.log("${header.CamelFilePath}${header.CamelFileName}")
			.toD("file:${header.CamelFilePath}?fileExist=Append")
		.end();
		
		
		
	}

}
