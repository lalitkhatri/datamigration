package util.datamigration;

import util.datamigration.parser.IParser;
import util.datamigration.parser.PhoenixtoParquet;

public class DataMigrationApplication {

	private static IParser parser;
	public static void main(String[] args) {
		
		parser = new PhoenixtoParquet();
		parser.parse();
		
	}

	
}
