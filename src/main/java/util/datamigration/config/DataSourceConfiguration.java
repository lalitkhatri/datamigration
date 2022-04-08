package util.datamigration.config;

import javax.sql.DataSource;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class DataSourceConfiguration {

	
	@Bean(name = "source")
	@Primary
	@ConfigurationProperties(prefix="source.datasource")
	public DataSource aseDataSource() {
	    return DataSourceBuilder.create().build();
	}

	@Bean(name = "destination")
	@ConfigurationProperties(prefix="destination.datasource")
	public DataSource sqlserverDataSource() {
	    return DataSourceBuilder.create().build();
	}
	
}
