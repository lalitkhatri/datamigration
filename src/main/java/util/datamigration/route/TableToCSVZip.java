package util.datamigration.route;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("export")
public class TableToCSVZip extends RouteBuilder {
	
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
		from("sql:SELECT * FROM "+schema+"."+tableName+"?outputType=StreamList&repeatCount=1")
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
			.toD("file:${header.CamelFilePath}?fileExist=Append")
		.end();
		
		
		
	}

}
