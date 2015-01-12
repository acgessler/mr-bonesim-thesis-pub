package de.uni_stuttgart.ipvs_as.eval;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;

public class PandasDummyDataGen {
	
	public static final String DB_USER = "root";
	public static final String DB_PW = "root";
	public static final String DB_NAME = "acg_eval_small";

	public static final String DB_INPUT_TABLE_NAME = "mapreduce4sum_input";
	public static String DB_URI = "jdbc:mysql://acg-mysql-1-1:3306/" + DB_NAME;
	
	public static final String SCHEMA = "CREATE TABLE " + DB_INPUT_TABLE_NAME + "(\n" +
		"-- Unique ID of this Motion Sequence \n" +
		"motionid TINYINT,\n" +
		"-- ID of the FE Element Point\n" +
		"elementid SMALLINT,\n" +
		"-- ID of the Gaussian Point\n" +
		"gaussid TINYINT,\n" +
		"-- Name of the mathematical variable\n" +
		"name VARCHAR(5),\n" +
		"-- ID of the Timestep\n" +
		"timestep SMALLINT,\n" +
		"PRIMARY KEY(motionid, elementid, gaussid, name, timestep),\n" +
		"INDEX elemid_name_index(elementid, name),\n" +
		"-- Output variable value\n" +
		"value DOUBLE PRECISION\n" +
	") ENGINE=NDBCLUSTER;";
	
	public static final int ROW_MEM_USAGE_ESTIMATE = 20;
	
	// Variable names emitted by Pandas
	public static final String[] VAR_NAMES = new String[] {
		// Pandas instance #1
		"NS0",
		"NS",
		"TSEG",
		"WFL1",
		"WFL2",
		"WFL3",
		"SIG11",
		"SIG22",
		"SIG33",
		"SIG12",
		"SIG23",
		"SIG_V",
		"WRKEL",
		"KF",
		"SIGS",
		"GROW",
		"NUT",
		"NSTS",
		// Pandas instance #2
		"WNU1",
		"WNU2",
		"WNU3",
		"CNUF"
	};
	
	// Tweakable count of motions, elements etc.
	public static final int COUNT_MOTION = 1;
	public static final int COUNT_TIMESTEP = 100;
	public static final int COUNT_ELEMENT = 1000;
	public static final int COUNT_GAUSS = 8;
	
	public static final long COUNT_TOTAL = (long)COUNT_MOTION * COUNT_TIMESTEP *
		COUNT_ELEMENT * COUNT_GAUSS * VAR_NAMES.length;
	
	// Lower bound for output table size in MiB, assuming no compression
	public static final long TOTAL_MEM_ESTIMATE_MIB = ROW_MEM_USAGE_ESTIMATE *
		COUNT_TOTAL / (1024 * 1024);
	
	// Number of threads to use for populating the database
	public static final int CONCURRENCY = 25;

	public Connection openDBConnection() throws SQLException {
		return DriverManager.getConnection(DB_URI, DB_USER, DB_PW);
	}
	
	// Utility to collect inserted tuples, once a threshold is reached,
	// the query is submitted to the database. This avoids hitting
	// the maximum package size accepted by the server and keeps the
	// number of queries sent small.
	private class BatchInserter {
		private Statement stat;
		private Connection conn;
		
		private StringBuilder query;
		private int count = 0;
		
		// Row threshold at which to submit a batch to the server
		public static final int THRESHOLD = 25000;
		
		public BatchInserter() throws SQLException {
			rewind();			
		}
		
		public void add(String tuple) throws SQLException {
			if (count > 0) {
				query.append(",");
			}
			
			query.append(tuple);
			
			if (++count > THRESHOLD) {
				submit();
				rewind();
			}		
		}
		
		public void finish() throws SQLException {
			if (count > 0) {
				submit();
			}
			conn.close();
			stat.close();
		}
		
		private void submit() throws SQLException {
			query.append(";");
			for (int i = 0; ; ++i) {
				try {
					stat.executeUpdate(query.toString());
				
				} catch(SQLException ex) {
					System.out.println("Mysql error, retrying. " + ex.toString());
					
					conn.close();
					stat.close();
					conn = openDBConnection();
					stat = conn.createStatement();
					
					try {
						Thread.sleep(1000 * Math.min(10, (i + 1)));
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					continue;
				}
				break;
			}
		}
		
		private void rewind() throws SQLException {
			conn = openDBConnection();
			stat = conn.createStatement();
			
			query = new StringBuilder();
			query.append("INSERT INTO ");
			query.append(DB_INPUT_TABLE_NAME);
			query.append("(motionid, elementid, gaussid, name, timestep, value) VALUES");
			count = 0;
		}
	};
	
	// Populate with data for a range of FE elements
	private void populateDataPartition(int partitionId, int beginElem, int endElem)
		throws SQLException
	{
		final BatchInserter inserter = new BatchInserter();

		final long percentStep = COUNT_TOTAL / (100 * CONCURRENCY);
		long valInserted = 0;
		for (int mid = 0; mid < COUNT_MOTION; ++mid) {
			for (int feid = beginElem; feid < endElem; ++feid) {
				for (int gaussid = 0; gaussid < COUNT_GAUSS; ++gaussid) {
					for (int tid = 0; tid < COUNT_TIMESTEP; ++tid) {
						for (String name : VAR_NAMES) {

							inserter.add(String.format(Locale.US,
									"(%d, %d, %d, \'%s\', %d, %f)",
									mid, feid, gaussid, name, tid,
									Math.random()));

							if (valInserted++ % percentStep == 0) {
								final long pct = CONCURRENCY * 100
										* valInserted / COUNT_TOTAL;
								final long estimate = CONCURRENCY
										* TOTAL_MEM_ESTIMATE_MIB * valInserted
										/ COUNT_TOTAL;
								
								final String s = String.format(
										"Thread #%d inserted %d rows, ~%d%% of total, ~%d MiB",
										partitionId,
										valInserted, pct,
										estimate);
								System.out.println(s);
								System.out.flush();
							}
						}
					}
				}
			}
		}
		inserter.finish();
	}
	
	
	public void initDBContents() throws SQLException {
		final Connection conn = this.openDBConnection();
		final Statement stat = conn.createStatement();

		// Clear input table
		try {
			stat.execute(String.format("DROP TABLE %s;", DB_INPUT_TABLE_NAME));
		} catch (SQLException e) {
			// Fine for the first time.
			e.printStackTrace();
			System.out.println("This can be ignored if the generator is is run"
					+ " for the first time or the DB has been cleared.");
		}

		// Set schema for input table
		// http://johanandersson.blogspot.de/2010/04/tuning-your-cluster-with-ndbinfo-71.html
		stat.execute("set ndb_table_no_logging=1;");
		stat.execute(SCHEMA);
		stat.execute("set ndb_table_no_logging=0;");

		// Populate the input table in larger batches using multiple threads
		final int partitionSize = COUNT_ELEMENT / CONCURRENCY;
		for (int partition = 0; partition < CONCURRENCY; ++partition) {
			final int threadPartition = partition;
			new Thread() {				
				public void run() {
					try {
						int beginElem = threadPartition * partitionSize;
						int endElem = beginElem + partitionSize;
						if (COUNT_ELEMENT - endElem < partitionSize) {
							endElem = COUNT_ELEMENT;
						}
						
						populateDataPartition(threadPartition, beginElem, endElem);
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			}.start();	
		}		

		stat.close();
		conn.close();
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			(new PandasDummyDataGen()).initDBContents();
		} catch (SQLException e) {
			System.out.println("Error, check SQL commands and DB settings");
			e.printStackTrace();
		}
	}
}
