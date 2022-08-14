package util.datamigration.model;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

public class CopyRow implements RowMapper<Map<String,Object>> {
	Logger  logger = LoggerFactory.getLogger(CopyRow.class);

	SimpleJdbcInsert simpleJdbcInsert;
	
	public CopyRow(SimpleJdbcInsert simpleJdbcInsert) {
		this.simpleJdbcInsert = simpleJdbcInsert;
	}
	
	@Override
	public Map<String,Object> mapRow(ResultSet rs, int rowNum) throws SQLException {
		logger.info("Parsing row number ---- "+ rowNum);
		ResultSetMetaData meta = rs.getMetaData();
		int colCount = meta.getColumnCount();
		Map<String,Object> row = new HashMap<>();
		for(int i=1; i<=colCount;i++) {
			row.put(meta.getColumnName(i), rs.getObject(i));
		}	
		simpleJdbcInsert.execute(row);
		logger.info(row.toString());
		return null;
	}
}
