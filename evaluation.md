Evaluation
===========

This section describe how the two prototypes, the MapReduce and the baseline
prototype, can be brought to life. This allows to fully reproduce the results
from the thesis, assuming a comparable Hadoop cluster.

Root for all source code paths is `prototypes/appClientModule/`.

Preliminaries
-----

1. Ensure MySQL Cluster (or MySQL) instance is up and running
2. Ensure Hadoop Cluster is up and running
3. Pull in the `prototypes/mapreduce-wsi` git submodule
     Run `git submodule init; git submodule update`
     or clone the entire repository using the `--recursive` flag.
       (See http://git-scm.com/book/en/v2/Git-Tools-Submodules)
4. Open `prototypes/` folder as Eclipse workspace
5. Update the MapReduce-WSI configuration file (`mapreduce-wsi.xml`) with
   the correct node to launch jobs from, make sure MapReduce-WSI
   is setup as HDFS user (see the [MapReduce-WSI documentation](https://github.com/acgessler/mapreduce-wsi/blob/master/README.md)
   for the exact setup instructions. In short, you need to
     - Create the `mapreduce_wsi` user
     - Make sure the remote node allows SSH with a password
     - Create the `/user/mapreduce_wsi` HDFS folder and `chown` it using
       the new account.
6. Launch MapReduce-WSI using an existing or newly created TomCat 7 instance
   (Eclipse: Right-click on the MapReduce-WSI project, Run As, Run on Server)


Populating MySQL Cluster
-----

1. Navigate to `de.uni_stuttgart.ipvs_as.eval.PandasDummyDataGen`
2. Tweak constants, i.e., DB credentials
    - `DB_NAME`: Name of database, _acg_eval_small, acg_eval_medium, acg_eval_large_
            were the database names used for this thesis.
    - `COUNT_TIMESTEP, COUNT_MOTION, COUNT_ELEMENT, COUNT_GAUSS`: column
            cardinalities, see Section 6.1.1 for values used.
3. Run. This can take a while, in particular for the large data set. Monitor MySQL
   cluster memory usage using the `ndb_mgm` tool.


MapReduce prototype
-----

1. Navigate to `de.uni_stuttgart.ipvs_as.eval.mapreduce.Runner`
2. Tweak constants, i.e. DB credentials, name of database
4. Right now, MapReduce-WSI offers no way to submit job configuration
   with the `runStreamingMapReduce` API (this can however easily be solved by
   adding a parameter that takes a set of key-value pairs to pass to Hadoop).
   Thus to select settings you have to hard-code them into MapReduce-WSI :-(
   Simply change `MapReduceWSIImpl.java` and wait for Tomcat to reload
   the service (this can take a few seconds and pops up red lines
   in Eclipse's console).

   Change the `runStreamingMapReduce` method body as to include the
   correct parameters in the yarn command line:
	
         sb.append(" -jobconf mapreduce.input.fileinputformat.split.minsize=50000");
         sb.append(" -jobconf mapreduce.input.fileinputformat.split.maxsize=300000");

   And, depending on the size of the data set also the following (insert
   the appropriate values from Section 6.6 of the thesis)

         sb.append(" -jobconf mapreduce.job.maps=8");
         sb.append(" -jobconf mapreduce.job.reduces=8");
         sb.append(" -jobconf mapreduce.map.memory.mb=4000");
         sb.append(" -jobconf mapreduce.reduce.memory.mb=4000");

   __Note__ that you may not want to set those at all unless your prime
   purpose is to exactly reproduce my original measurements. Hadoop
   is normally good ad determining appropriate values on its own,
   tweaking such configuration should only be done if the defaults
   have been observed to be not working well.

5. Run. Three iterations are run and results displayed in the output
   window. While it runs, monitor progress and look for possible
   problems using the Hadoop Application Web UI.


Baseline prototype
-----

1. Navigate to `de.uni_stuttgart.ipvs_as.eval.mapreduce.Runner`
2. Tweak constants, i.e. DB credentials, name of database
3. If any of the WorkerNode code has been modified, re-export
   `WorkerNode.jar` (Eclipse: right-click on project, export,
   Executable Jar, select WorkerNode as main class)
4. Run. Three iterations are run and results displayed in the output
   window. Warning: Excessive amount of parallel SSHXCUTE threads can
   render the system temporarily unresponsible. You can ignore occasional
   errors from worker nodes parsing an empty string: this is caused by
   data loss when closing a pipe and does not affect the result.

