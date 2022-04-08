package util.datamigration.model;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

public class CopyRow implements org.springframework.jdbc.core.RowMapper<Map<String,Object>> {
    
	SimpleJdbcInsert simpleJdbcInsert;
	
	public CopyRow(SimpleJdbcInsert simpleJdbcInsert) {
		this.simpleJdbcInsert = simpleJdbcInsert;
	}
	
	@Override
	public Map<String,Object> mapRow(ResultSet rs, int rowNum) throws SQLException {
		ResultSetMetaData meta = rs.getMetaData();
		int colCount = meta.getColumnCount();
		Map<String,Object> row = new HashMap<>();
		for(int i=1; i<=colCount;i++) {
			row.put(meta.getColumnName(i), rs.getObject(i));
		}	
		simpleJdbcInsert.execute(row);
		return null;
	}
}
