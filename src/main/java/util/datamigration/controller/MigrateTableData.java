package util.datamigration.controller;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import util.datamigration.model.CopyRow;

@RestController
public class MigrateTableData {
	
	@Autowired
	@Qualifier("source")
	private DataSource sourceDS;
	
	@Autowired
	@Qualifier("destination")
	private DataSource destDS;
	 
	private JdbcTemplate  jdbcTemplate;
	private Map<String,SimpleJdbcInsert> simpleJdbcInsertMap= new HashMap<String, SimpleJdbcInsert>();

	
	@GetMapping("/{tableName}")
	public void migrateData(@PathVariable("tableName") String tableName) throws Exception {
		init(tableName);
		copyAllData(tableName);
	}
	
	private void init(String tableName) {
		if(jdbcTemplate==null) {
			jdbcTemplate = new JdbcTemplate(sourceDS);
			jdbcTemplate.setFetchSize(500);
		}
		
		if(simpleJdbcInsertMap.get(tableName)==null) {
			simpleJdbcInsertMap.put(tableName, new SimpleJdbcInsert(destDS).withTableName(tableName));
		}
	}
	
	private void copyAllData(String tableName) {
        jdbcTemplate.query("SELECT * FROM "+tableName, new CopyRow(simpleJdbcInsertMap.get(tableName)));
    }
	
	
}
