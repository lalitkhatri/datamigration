package util.datamigration.route;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class TableToCSVZip extends RouteBuilder {
	
	@Value("${outPutType}")
	private String outputType;

	@Value("${filePath}")
	private String filePath;
	
	@Value("${tableName}")
	private String tableName;
	
	@Value("${schema}")
	private String schema;
	

	@Override
	public void configure() throws Exception {
		from("sql:SELECT * FROM "+schema+"."+tableName+"?outputType="+outputType+"&repeatCount=1")
		.split(body()).streaming()
			.log("${body}")
		.end()
		;
		
		
	}

}
