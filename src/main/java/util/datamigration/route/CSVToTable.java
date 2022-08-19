package util.datamigration.route;

import java.util.ArrayList;

import org.apache.camel.AggregationStrategy;
import org.apache.camel.builder.AggregationStrategies;
import org.apache.camel.builder.RouteBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("load")
public class CSVToTable extends RouteBuilder {
	
	@Value("${filePath}")
	private String filePath;
	
	@Value("${fileName}")
	private String fileName;
	
	@Value("${tableName}")
	private String tableName;
	
	@Value("${schema}")
	private String schema;
	
	@Value("${batchSize}")
	private String batchSize;
	
	@Override
	public void configure() throws Exception {
		
		AggregationStrategy agg = AggregationStrategies.flexible(String.class)
			    .accumulateInCollection(ArrayList.class)
			    .pick(body());
		
		from("file:filePath?fileName="+fileName)
			.split(body().tokenize("\n")).streaming()	
			.aggregate(agg).constant(true).completionSize(batchSize)
			.log("${body.size}")
		.end()
		;
		
	}
	
	

}
