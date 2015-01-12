package de.uni_stuttgart.ipvs_as.eval.baseline;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Random;

public class WorkerNode {
	
	public static final String DB_USER = "root";
	public static final String DB_PW = "root";
	
	public static final String DB_INPUT_TABLE_NAME = "mapreduce4sum_input";
	public static final String DB_OUTPUT_TABLE_NAME = "octave_output";
	
	// Query to import data from MySQL
	public static final String QUERY_TEMPLATE =
	"SELECT A.elementid, A.gaussid, NSTS_avg, SIG_V_avg , CNUF_avg \n" +
	"FROM \n" +
	"  \n" +
	"( SELECT elementid, gaussid, AVG(value) as NSTS_avg \n" +
	"FROM " + DB_INPUT_TABLE_NAME + " \n" +
	"WHERE motionid = @CURMOTIONSEQ AND name = 'NSTS'\n" +
	"  AND elementid >= @BEGIN AND elementid < @END \n" +
	"GROUP BY elementid, gaussid \n" +
	") AS A, \n" +
	"  \n" +
	"( SELECT elementid, gaussid, AVG(value) as SIG_V_avg \n" +
	"FROM " + DB_INPUT_TABLE_NAME + " \n" +
	"WHERE motionid = @CURMOTIONSEQ AND name = 'SIG_V' \n" +
	"  AND elementid >= @BEGIN AND elementid < @END \n" +
	"GROUP BY elementid, gaussid \n" +
	" \n" +
	") AS B, \n" +
	"  \n" +
	"( SELECT elementid, gaussid, AVG(value) as CNUF_avg \n" +
	"FROM " + DB_INPUT_TABLE_NAME + " \n" +
	"WHERE motionid = @CURMOTIONSEQ AND name = 'CNUF' \n" +
	"  AND elementid >= @BEGIN AND elementid < @END \n" +
	"GROUP BY elementid, gaussid \n" +
	") AS C \n" +
	"  \n" +
	"WHERE A.elementid = B.elementid AND A.gaussid = B.gaussid \n" +
	"  AND B.elementid = C.elementid AND B.gaussid = C.gaussid \n" +
	"ORDER BY A.elementid, A.gaussid \n" +
	";";
	
	
	
	public final static int HOURS_PER_DAY = 24;
	
	private static Connection openDBConnection(String sourceDbUri) throws SQLException {
		return DriverManager.getConnection(sourceDbUri, DB_USER, DB_PW);
	}
	
	// Utility to collect inserted tuples, once a threshold is reached,
	// the query is submitted to the database.
	private static class BatchInserter {
		private final Statement stat;
		
		private StringBuilder query;
		private int count = 0;
		
		// Row threshold at which to submit a batch to the server
		public static final int THRESHOLD = 100;
		
		public BatchInserter(Connection conn) throws SQLException {
			stat = conn.createStatement();
			rewind();			
		}
		
		public void insert(int elementId, int gaussId, double result0, double result1) throws SQLException {
			if (count > 0) {
				query.append(",");
			}
			
			query.append(String.format(Locale.US,
					"(%d, %d, %f, %f)",
					elementId, gaussId, result0, result1));
			
			if (++count > THRESHOLD) {
				submit();
				rewind();
			}		
		}
		
		public void finish() throws SQLException {
			if (count > 0) {
				submit();
			}
			stat.close();
		}
		
		private void submit() throws SQLException {
			query.append(";");
			stat.executeUpdate(query.toString());
		}
		
		private void rewind() throws SQLException {
			query = new StringBuilder();
			query.append("INSERT INTO ");
			query.append(DB_OUTPUT_TABLE_NAME);
			query.append("(elementid, gaussid, NSTS, HCF) VALUES ");
			count = 0;
		}
	};
	
	
	// Encapsulates launching Octave with a two-way pipe and
	// sending over tuples of data for simulation.
	private static class OctaveBinding
	{
		ProcessBuilder pb;
		Process p;
		
		InputStream stdout;
		OutputStream stdin;
		
		StringBuilder builder = new StringBuilder();
		String octaveScriptPath;
	
		public OctaveBinding(String octaveScriptPath) throws IOException {
			pb = new ProcessBuilder("octave", "--silent", octaveScriptPath);
			pb.redirectErrorStream(true);
			p = pb.start();
			stdout = p.getInputStream();
			stdin = p.getOutputStream();
			this.octaveScriptPath = octaveScriptPath;
		}
		
		public void simulate(double[] dayVarsA, double[] dayVarsB,
			double[] dayVarsC, double[] resultTuple) throws IOException {
			
			for(double a : dayVarsA) {
				builder.append(a);
				builder.append(" ");
			}
			
			builder.append("\n\n");	
			stdin.write(builder.toString().getBytes());
			stdin.flush();		
			
			int b;
			builder = new StringBuilder();
			while ((b = stdout.read()) != -1 && b != '\n') {
				builder.append((char)b);
			}
			
			final String resultLine = builder.toString();
			final String[] elements = resultLine.split("\\s+");
			
			try {
				resultTuple[0] = Double.parseDouble(elements[0]);
				resultTuple[1] = Double.parseDouble(elements[1]);
			}
			catch(NumberFormatException ex){
				// Can happen end of stream
				System.out.println(ex.toString());
				System.out.println(resultLine);
				resultTuple[0] = 2;
				resultTuple[1] = 4;
			}
		}
		
		public void finish() {
			p.destroy();
		}
	}
	
	private static String fileForMotionSequence(int mid, String fileBase) {
		return fileBase + "motionid_" + mid + ".csv";
	}
	
	private static void exportMotionSequence(int mid, int minElementId, int maxElementId,
			Connection conn, String file) throws SQLException, IOException
	{
		final Statement stat = conn.createStatement();
		String query = QUERY_TEMPLATE;
		query = query.replace("@CURMOTIONSEQ", new Integer(mid).toString());
		query = query.replace("@BEGIN", new Integer(minElementId).toString());
		query = query.replace("@END", new Integer(maxElementId).toString());
	
		final ResultSet results = stat.executeQuery(query);
		
		BufferedWriter outWriter = new BufferedWriter(new FileWriter(file));
		while (results.next()) {
			// A.elementid, A.gaussid, NSTS_avg, SIG_V_avg , CNUF_avg
			outWriter.write(String.format("%s %s %s %s %s",
					results.getString(1),
					results.getString(2),
					results.getString(3),
					results.getString(4),
					results.getString(5)));
			outWriter.newLine();
		}
		
		outWriter.close();
		results.close();
		stat.close();
	}
	
	private static void mergeSequencesAndRunOctaveSimulation(
			int countMotionIds, 
			int[] hoursToMotionMapping,
			String fileBase,
			BatchInserter resultInserter,
			OctaveBinding octave) throws IOException, SQLException
	{
		BufferedReader[] readers = new BufferedReader[countMotionIds];
		for (int mid = 0; mid < countMotionIds; ++mid) {
			readers[mid] = new BufferedReader(new FileReader(
				fileForMotionSequence(mid, fileBase)));
		}	
	
		String[] lines = new String[countMotionIds];
		double[] meanVarsA = new double[countMotionIds];
		double[] meanVarsB = new double[countMotionIds];
		double[] meanVarsC = new double[countMotionIds];
		
		double[] dayVarsA = new double[24];
		double[] dayVarsB = new double[24];
		double[] dayVarsC = new double[24];
		double[] resultTuple = new double[2];
		
		while ((lines[0] = readers[0].readLine()) != null) {
			
			for (int mid = 1; mid < countMotionIds; ++mid) {
				lines[mid] = readers[mid].readLine();
				if (lines[mid] == null) {
					throw new IOException("unexpected eof merging CSV files");
				}
			}
			
			int elementId = -1, gaussId = -1;
			
			for (int mid = 0; mid < countMotionIds; ++mid) {
				String[] columns = lines[mid].split("\\s+");
				if (columns.length != 5) {
					throw new IOException("unexpected number of columns merging CSV files");
				}
				final int tempElementId = Integer.parseInt(columns[0]);
				final int tempGaussId = Integer.parseInt(columns[1]);
				
				if (mid == 0) {
					elementId = tempElementId;
					gaussId = tempGaussId;
				}
				else if (tempElementId != elementId || tempGaussId != gaussId) {
					throw new IOException("unexpected elementid, gaussid merging ordered CSV files.");
				}
				
				final double varA = Double.parseDouble(columns[2]);
				final double varB = Double.parseDouble(columns[3]);
				final double varC = Double.parseDouble(columns[4]);
				
				meanVarsA[mid] = varA;
				meanVarsB[mid] = varB;
				meanVarsC[mid] = varC;
			}
			
			for (int i = 0; i < HOURS_PER_DAY; ++i) {
				final int mid = hoursToMotionMapping[i];
				dayVarsA[i] = meanVarsA[mid];
				dayVarsB[i] = meanVarsB[mid];
				dayVarsC[i] = meanVarsC[mid];
			}
			
			octave.simulate(dayVarsA, dayVarsB, dayVarsC, resultTuple);
			resultInserter.insert(elementId, gaussId, resultTuple[0], resultTuple[1]);
		}
		
		for (int mid = 0; mid < countMotionIds; ++mid) {
			readers[mid].close();	
		}
	}
	
	private static void cleanupFiles(int countMotionIds, String fileBase)
	{
		for (int mid = 0; mid < countMotionIds; ++mid) {
			new File(fileForMotionSequence(mid, fileBase)).delete();
		}
	}
	
	private static final Random rnd = new Random();
	
	// Output the embedded Octave script to a file on disk,
	// return the path to it.
	private static String outputOctaveFile() throws IOException
	{
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				WorkerNode.class.getResourceAsStream(
					"octave_calc.m"),
					"UTF-8"));
		
		// Really important to only generate positive ints.
		// Negative ints cause Octave to treat the file name
		// as the beginning of a command line argument.
		final String filename = new Integer(rnd.nextInt(10000)).toString() + "o.m";
		
		// Must be missing the obvious here.
		BufferedWriter outWriter = new BufferedWriter(new FileWriter(filename));

		String line;
		while ((line = reader.readLine()) != null) {
			outWriter.write(line);
			outWriter.write("\n");
		}
		
		outWriter.close();
		reader.close();
		return filename;
	}

	public static long run(int countMotionIds, int minElementId, int maxElementId,
			int[] hoursToMotionMapping,
			String sourceDbUri,
			String fileBase) throws SQLException, IOException
	{
		if (hoursToMotionMapping.length != HOURS_PER_DAY) {
			throw new IllegalArgumentException("hoursToMotionMapping");
		}
		
		final Connection conn = openDBConnection(sourceDbUri);	
		final String octaveScriptPath = outputOctaveFile();
		
		for (int mid = 0; mid < countMotionIds; ++mid) {
			exportMotionSequence(mid, minElementId, maxElementId, conn,
					fileForMotionSequence(mid, fileBase));
		}
		
		final long time1 = System.nanoTime();
		
		final OctaveBinding octave = new OctaveBinding(octaveScriptPath);
		final BatchInserter resultInserter = new BatchInserter(conn);
		mergeSequencesAndRunOctaveSimulation(countMotionIds,
				hoursToMotionMapping,
				fileBase,
				resultInserter,
				octave);
		
		octave.finish();
		
		resultInserter.finish();
		conn.close();
		
		cleanupFiles(countMotionIds, fileBase);
		
		final long time2 = System.nanoTime();
		return time2 - time1;
	}
	
	
	/**
	 * @param args 6 arguments forwarded from SimRunner.
	 * 
	 * - Number of motion sequences/IDs (globally)
	 * - Minimum element id (for this worker)
	 * - Maximum element id (for this worker)
	 * - Motion ID configuration given as a comma-separated list of 24
	 *     IDs in the hourly order in which they occur during the day (no spaces!).
	 * - Source database name (JDBC)
	 * - Destination file name base
	 * @throws IOException 
	 * @throws SQLException 
	 * @throws NumberFormatException 
	 */
	public static void main(String[] args) throws NumberFormatException, SQLException, IOException {
		if (args.length != 6) {
			throw new IllegalArgumentException("args");
		}
		int[] hoursMap = new int[HOURS_PER_DAY];
		String[] s = args[3].split(",");
		if (s.length != HOURS_PER_DAY) {
			throw new IllegalArgumentException("hours mapping");
		}
		for (int i = 0; i < HOURS_PER_DAY; ++i) {
			hoursMap[i] = Integer.parseInt(s[i]);
		}
		
		final long octaveTime = run(Integer.parseInt(args[0]),Integer.parseInt(args[1]),
				Integer.parseInt(args[2]),
				hoursMap,
				args[4],
				args[5]);
		
		System.out.println(new Long(octaveTime).toString());
		System.out.flush();
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
