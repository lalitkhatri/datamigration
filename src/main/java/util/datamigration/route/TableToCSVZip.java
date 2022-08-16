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

	@Override
	public void configure() throws Exception {
		from("sql:SELECT * FROM GLOBALDATA.EXCHANGE WHERE TRACK='Y'?outputType="+outputType+"&repeatCount=1")
		.split(body()).streaming()
			.log("${body}")
		.end()
		;
		
		
	}

}
