package de.uni_stuttgart.ipvs_as.eval.baseline;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;

import net.neoremind.sshxcute.core.ConnBean;
import net.neoremind.sshxcute.core.IOptionName;
import net.neoremind.sshxcute.core.Result;
import net.neoremind.sshxcute.core.SSHExec;
import net.neoremind.sshxcute.exception.TaskExecFailException;
import net.neoremind.sshxcute.task.impl.ExecCommand;


// Simulation runner for the baseline prototype
public class Runner {

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

	
	// Worker (compute) nodes in the order in which they should be used
	public static final String[] WORKER_NODES = {
		"w16",
		"w26",
		"w15",
		"w25",
		"w14",
		"w24",
		"w13",
		"w23",
	};
	
	// Node credentials. All of these allow SSH with password only
	public static final String REMOTE_USER_NAME = "mapreduce_wsi";
	public static final String REMOTE_PASSWORD = "mapreduce_wsi";
	
	// JAR containing worker code, on local disk accessible to SimRunner
	public static final String WORKER_JAR_LOCATION = "WorkerNode.jar";
	public static final String WORKER_MYSQL_JAR_LOCATION = "mysql-connector-java-5.1.29-bin.jar";
	
	// Remote file system location. Note that this is *not*
	// HDFS itself but only the local folder which is also
	// used by HDFS to store file blocks. This folder must
	// be used because it is the mount point for /dev/vdb1,
	// which is the SAN storage with 100 GiB/worker.
	//
	// As a preliminary, the folder must be created on each
	// of the nodes and chown'd by the mapreduce_wsi user.
	public static final String REMOTE_JAR_LOCATION = "/hdfs/baseline_experiments/";
	
	// Maximum parallelism per worker (== CPU count on the worker)
	public static final int MAX_PARALLELISM_PER_WORKER_NODE = 4;
	
	// Eval config compute resources
	public static final int USE_WORKER_NODE_COUNT = 8;
	public static final int USE_PARALLELISM_PER_WORKER_NODE = 4;
	
	public static final int USE_DATA_PARTITION_COUNT = USE_WORKER_NODE_COUNT *
		USE_PARALLELISM_PER_WORKER_NODE;
	
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
	
	private static int countMotionSequences() throws SQLException {
		final Connection conn = openDBConnection();
		final Statement stat = conn.createStatement();
		
		final ResultSet results = stat.executeQuery(
				"SELECT MAX(motionid) - MIN(motionid) + 1 FROM " +
				DB_INPUT_TABLE_NAME + ";");
		
		results.next();
		final int count = results.getInt(1);
		
		results.close();
		stat.close();
		conn.close();
		return count;
	}
	
	private static int[] getElementIdRange() throws SQLException {
		final Connection conn = openDBConnection();
		final Statement stat = conn.createStatement();
		
		final ResultSet results = stat.executeQuery(
				"SELECT MIN(elementid), MAX(elementid) FROM " +
				DB_INPUT_TABLE_NAME + ";");
		
		results.next();
		final int minId = results.getInt(1);
		final int maxId = results.getInt(2);
		
		results.close();
		stat.close();
		conn.close();
		return new int[] {minId, maxId};
	}
	
	private static SSHExec initSSHConnection(String host) throws IllegalArgumentException {
		ConnBean cb = new ConnBean(host,
				REMOTE_USER_NAME,
				REMOTE_PASSWORD);
		
		// SSHXCUTE by default only provides a singleton instance, which
		// is not capable of dealing with multiple concurrent sessions.
		//
		// The underlying code base, in particular JSch (which does the
		// actual work) of course supports multiple sessions. So, get
		// access to the private c'tor of SSHExec and there we go.
		Constructor<SSHExec> constructor = null;
		try {
			constructor = SSHExec.class.getDeclaredConstructor(new Class[] {ConnBean.class});
		} catch (SecurityException e1) {
			e1.printStackTrace();
		} catch (NoSuchMethodException e1) {
			e1.printStackTrace();
		}
		constructor.setAccessible(true);
        
		SSHExec sshConnection = null;
		try {
			sshConnection = constructor.newInstance(cb);
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
		if (!sshConnection.connect()) {
			throw new IllegalArgumentException("Failed to connect to remote host " + host);
		}
		SSHExec.setOption(IOptionName.INTEVAL_TIME_BETWEEN_TASKS, 0L);
		return sshConnection;
	}
	
	private static void copyToRemote(String srcJarName, String destName, SSHExec sshConnection)
			throws IllegalArgumentException {
		try {
			// SSHXCUTE is not thread-safe
			synchronized (sshConnection) {
				sshConnection.uploadSingleDataToServer(srcJarName, destName);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new IllegalArgumentException(String.format(
					"Failed to copy source file %s to destination %s",
					srcJarName, destName), e);
		}
	}
	
	private static String execRemote(String command, SSHExec sshConnection)
		throws IllegalArgumentException {
		try {
			// SSHXCUTE is not thread-safe
			synchronized (sshConnection) {
				final Result res = sshConnection.exec(new ExecCommand(command));
				return res.sysout;
			}
		} catch (TaskExecFailException e) {
			e.printStackTrace();
			throw new IllegalArgumentException ("Failed to execute remote command",
					e);
		}
	}
	
	public static void main(String[] arguments) throws IOException, SQLException, InterruptedException {
		
		long aggregationTimeAcc = 0;
		long octaveTimeAcc = 0;
		for (int i = 0; i < ITERATIONS; ++i) {
			System.out.println("Iteration: " + i);
			createOutputTable();
			
			final long time0 = System.nanoTime();
			
			// Determine column cardinality and split size
			final int countMotion = countMotionSequences();
			final int[] elementMinMax = getElementIdRange();
			
			final int elementDelta = elementMinMax[1] - elementMinMax[0] + 1;
			final int partitionSize = elementDelta / USE_DATA_PARTITION_COUNT;
			if (partitionSize < 1) {
				throw new IllegalArgumentException("config");
			}
			
			final long[] octaveTimesPerThread = new long[USE_DATA_PARTITION_COUNT];
			final CountDownLatch cd = new CountDownLatch(USE_DATA_PARTITION_COUNT);
			
			// Deploy JAR with worker code to each node and run it immediately
			// Each worker returns the microseconds it spent executing the
			// octave simulation (i.e. octave_time). We take the maximum of
			// all worker's times plus the time it took us to bring the
			// respective worker up and running as the time measured.
			// The aggregation_time is then simply the total_time -
			// octave_time.
			for (int j = 0; j < USE_WORKER_NODE_COUNT; ++j) {
				final int saveJ = j;
				final String node = WORKER_NODES[j];
				
				new Thread() {
					@Override
					public void run() {
						final SSHExec ssh = initSSHConnection(node);
						copyToRemote(WORKER_JAR_LOCATION, REMOTE_JAR_LOCATION + "w.jar", ssh);
						copyToRemote(WORKER_MYSQL_JAR_LOCATION, REMOTE_JAR_LOCATION + "m.jar", ssh);
											
						ssh.disconnect();
						
						for (int k = 0; k < USE_PARALLELISM_PER_WORKER_NODE; ++k) {
							final int saveK = k;
							new Thread() {
								@Override
								public void run() {	
									final int cursor = saveJ * USE_PARALLELISM_PER_WORKER_NODE + saveK;
									final int elementRangeBegin = partitionSize * cursor + elementMinMax[0];
									int elementRangeEnd = elementRangeBegin + partitionSize;
									if (elementMinMax[1] + 1 - elementRangeEnd < partitionSize) {
										elementRangeEnd = elementMinMax[1] + 1;
									}
									final SSHExec ssh = initSSHConnection(node);
									final String filebase = REMOTE_JAR_LOCATION + "instance_" + saveK + "/";
									execRemote("mkdir " + filebase, ssh);
									
									final String hourMapping = "0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0";
									
									final String command = String.format("java -cp %sm.jar:%sw.jar "+
										" de.uni_stuttgart.ipvs_as.eval.baseline.WorkerNode " +
										"%d %d %d %s \"%s\" \"%s\"",
										REMOTE_JAR_LOCATION,
										REMOTE_JAR_LOCATION,
										countMotion,
										elementRangeBegin,
										elementRangeEnd,
										hourMapping,
										DB_URI,
										filebase);
									
									final String res = execRemote(command, ssh);
									
									try {
										final long threadOctaveTime = Long.parseLong(res.trim());
										System.out.println(String.format("octave_time for j=%d,k=%d is %d nanoseconds",
												saveJ, saveK, threadOctaveTime));
										
										octaveTimesPerThread[cursor] = threadOctaveTime;
									}
									finally {
										cd.countDown();
									}
									ssh.disconnect();																
								};
							}.start();
						}		
					};
				}.start();
			}
			
			cd.await();
			
			long octaveTime = 0;
			for (long threadOctaveTime : octaveTimesPerThread) {
				if (threadOctaveTime > octaveTime) {
					octaveTime = threadOctaveTime;
				}
			}
			
			final long time2 = System.nanoTime();
			final long totalTime = time2 - time0;
			
			aggregationTimeAcc += totalTime - octaveTime;
			octaveTimeAcc += octaveTime;
		}
		
		aggregationTimeAcc /= ITERATIONS;
		octaveTimeAcc /= ITERATIONS;
		final double scaleToSeconds = 1 / 1000000000.0;
		System.out.println("aggregation_time: " + aggregationTimeAcc * scaleToSeconds);
		System.out.println("octave_time: " + octaveTimeAcc * scaleToSeconds);
		System.out.println("total_time: " + (aggregationTimeAcc + octaveTimeAcc) * scaleToSeconds);
	}
}
