package util.datamigration.route;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.AggregationStrategy;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.AggregationStrategies;
import org.apache.camel.builder.RouteBuilder;
import org.apache.phoenix.shaded.org.apache.commons.lang.StringUtils;
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
	
	@Value("${colNames}")
	private String colNames;

	@Value("${schema}")
	private String schema;

	@Value("${batchSize}")
	private String batchSize;
	
	private String insertSql;

	private Processor rowProcessor = new Processor() {

		@Override
		public void process(Exchange exchange) throws Exception {
			Map<String,Object> row = new HashMap<>();
			String valStr = (String) exchange.getIn().getBody(String.class);
			String[] values = StringUtils.splitPreserveAllTokens(valStr, ",");
			String[] cols = StringUtils.splitPreserveAllTokens(colNames, ",");
			for (int i =0; i< cols.length;i++) {
				switch (cols[i]) {
				case "TOTTRDQTY":
					row.put(cols[i], Long.parseLong(values[i]));
					break;
				case "EXCHANGE":
				case "SYMBOL":
				case "TRADEDATE":
				case "FREQ":
					row.put(cols[i], values[i]);
					break;
				default:
					if(!StringUtils.isBlank(values[i])) {
						row.put(cols[i], Float.parseFloat(values[i]));
					}
					else {
						row.put(cols[i], 0f);
					}
					break;
				}
					
				
							
			}
			exchange.getIn().setBody(row);
		}
	};

	@Override
	public void configure() throws Exception {

		AggregationStrategy agg = AggregationStrategies.flexible(Map.class).accumulateInCollection(ArrayList.class)
				.pick(body());
		buildInsertSql();
		from("stream:file?fileName=" + filePath + fileName)
		.process(rowProcessor)
		.aggregate(agg).constant(true).completionSize(batchSize).completionTimeout(1000L)
		.log("Processing batch")
		.to("sql:"+insertSql+"?batch=true")
		.end();

	}

	private void buildInsertSql() {
		insertSql = "upsert into "+schema+"."+tableName+" values (:#"+colNames.replaceAll(",", ",:#")+")";
		
	}

}
