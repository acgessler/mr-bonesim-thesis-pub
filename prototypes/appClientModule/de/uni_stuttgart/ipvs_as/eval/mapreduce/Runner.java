package de.uni_stuttgart.ipvs_as.eval.mapreduce;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

import javax.xml.namespace.QName;
import javax.xml.ws.Service;

import de.uni_stuttgart.ipvs_as.MapReduceWSI;
import de.uni_stuttgart.ipvs_as.MapReduceWSIException;

// Simulation runner for the MapReduce prototype
public class Runner {
	
	// MapReduce-WSI binding
	public static final String WSDL_PATH = "http://localhost:8080/mapreduce-wsi/mapreduce?wsdl";
	public static final String SERVICE_SCOPE = "http://ipvs_as.uni_stuttgart.de/";
	public static final String SERVICE_NAME = "MapReduceWSIImplService";

	// Database name determines which data size the simulation runs on
	public static final String DB_NAME = "acg_eval_small";
	
	// This URI is accessed from within the cluster so localhost does not work
	public static String DB_URI = "jdbc:mysql://acg-mysql-1-1:3306/" + DB_NAME;

	public static final String DB_USER = "root";
	public static final String DB_PW = "root";
	
	public static final String DB_INPUT_TABLE_NAME = "mapreduce4sum_input";
	public static final String DB_OUTPUT_TABLE_NAME = "octave_output";

	public static final String HDFS_MR0_INPUT_NAME = "mr_input0";
	public static final String HDFS_MR0_OUTPUT_NAME = "mr_intermediate_0";
	public static final String HDFS_MR1_INPUT_NAME = "mr_intermediate_0";
	public static final String HDFS_MR1_OUTPUT_NAME = "mr_output1";
	
	public static final int ITERATIONS = 3;

	public static final String SQOOP_PARTITION_COLUMN =
		DB_INPUT_TABLE_NAME + ".elementid";
	
	public static final String SQOOP_IMPORT_QUERY =
		"SELECT motionid, elementid, gaussid, timestep, name, value " + 
			"FROM " + DB_INPUT_TABLE_NAME + 
		" WHERE (name = \"NS0\" OR name = \"SIG_V\" OR name = \"CNUF\")";
	
	public static final String DB_OUTPUT_SCHEMA = "CREATE TABLE " +
		DB_OUTPUT_TABLE_NAME + "(\n" +
		"-- ID of the FE Element Point\n" +
		"elementid INT,\n" +
		"-- ID of the Gaussian Point\n" +
		"gaussid INT,\n" +
		"\n" +
		"-- Output variables,\n" +
		"NSTS FLOAT,\n" +
		"HCF FLOAT\n" +
	") ENGINE=NDBCLUSTER;";

	
	public static final String MR0_MAPPER_FILE = "aggregate_mapper.py";
	public static final String MR0_REDUCER_FILE = "aggregate_reducer.py";
	public static final String MR1_MAPPER_FILE = "octave_mapper.py";
	public static final String MR1_REDUCER_FILE = "octave_reducer.py";
	
	public static final String OCTAVE_CALC_FILE = "octave_calc.m";
	
	private static String readFile(String path) throws IOException {
		return new Scanner(new File(path)).useDelimiter("\\Z")
			.next().replace("\r\n", "\n");
	}
	
	private static Connection openDBConnection() throws SQLException {
		return DriverManager.getConnection(DB_URI, DB_USER, DB_PW);
	}
	
	private static void createOutputTable() throws SQLException {
		final Connection conn = openDBConnection();
		final Statement stat = conn.createStatement();

		// Clear output table and re-create it using the newest schema
		stat.execute(String.format("DROP TABLE IF EXISTS %s;", DB_OUTPUT_TABLE_NAME));

		stat.execute(DB_OUTPUT_SCHEMA);
		stat.close();
		conn.close();
	}
	
	public static void main(String[] arguments) throws IOException, SQLException {
		// Dynamically connect to the MapReduce-WSI service instance
		URL url = new URL(WSDL_PATH);
		QName qname = new QName(SERVICE_SCOPE, SERVICE_NAME);
		Service service = Service.create(url, qname);
		MapReduceWSI port = service.getPort(MapReduceWSI.class);
		
		// Read mapper/reducer scripts
		final String mr0Mapper = readFile(MR0_MAPPER_FILE);
		final String mr0Reducer = readFile(MR0_REDUCER_FILE);
		final String mr1Mapper = readFile(MR1_MAPPER_FILE);
		final String mr1Reducer = readFile(MR1_REDUCER_FILE).replace(
				"OCTAVE_SOURCE_CODE_INSERT",
				// String escaping done right - twice!
				"\"" + readFile(OCTAVE_CALC_FILE)
					.replace("\\n", "\\\\n")
					.replace("\n", "\\n")
					.replace("\"", "\\\"")
					.replace("\'", "\\\'") +
					"\"");
		
		long delta0 = 0;
		long delta1 = 0;
		for (int i = 0; i < ITERATIONS; ++i) {
			System.out.println("Iteration: " + i);
			// Create output database table first
			createOutputTable();
	
			long time0 = System.nanoTime();
			
			long scope;
			try {
				// Create a new MapReduce-WSI scope
				scope = port.createScope();
				
				// Export data to HDFS
				port.importIntoHDFS(scope, DB_URI, DB_USER, DB_PW,
						SQOOP_IMPORT_QUERY,
						SQOOP_PARTITION_COLUMN,
						HDFS_MR0_INPUT_NAME);
				
				// Run MR0: data aggregation
				port.runStreamingMapReduce(scope, mr0Mapper, mr0Reducer,
						HDFS_MR0_INPUT_NAME,
						HDFS_MR0_OUTPUT_NAME);
				
				long time1 = System.nanoTime();
				
				// Run MR1: octave
				port.runStreamingMapReduce(scope, mr1Mapper, mr1Reducer,
						HDFS_MR1_INPUT_NAME,
						HDFS_MR1_OUTPUT_NAME);
				
				// Re-import results into SQL
				port.exportToRDBMS(scope, DB_URI, DB_USER, DB_PW,
						DB_OUTPUT_TABLE_NAME, HDFS_MR1_OUTPUT_NAME);
				
				// Free the HDFS scope
				port.deleteScope(scope);
				
				long time2 = System.nanoTime();
				delta0 += (time1 - time0);
				delta1 += (time2 - time1);
			} catch (MapReduceWSIException e) {
				e.printStackTrace();
			}
		}
		
		delta0 /= ITERATIONS;
		delta1 /= ITERATIONS;
		final double scaleToSeconds = 1 / 1000000000.0;
		System.out.println("aggregation_time: " + delta0 * scaleToSeconds);
		System.out.println("octave_time: " + delta1 * scaleToSeconds);
		System.out.println("total_time: " + (delta0 + delta1) * scaleToSeconds);
	}
}
