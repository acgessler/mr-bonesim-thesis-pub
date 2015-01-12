MapReduce to Couple a Bio-mechanical and a Systems-biological Simulation
--------------

B.Sc. Thesis, University of Stuttgart (2014)

####Repository Structure

Note: you need to use the `--recursive` flag when cloning this repository to make sure all submodules are included.

  - `prototypes/`: Full source code of the MapReduce and the baseline prototype developed for the thesis.
  - `prototypes/mapreduce-wsi`: Git submodule, the linked repository contains the MapReduce Web Server
     Interface that has also been developed as part of this thesis ([Github Repository](https://github.com/acgessler/mapreduce-wsi), [JavaDoc](http://acgessler.github.io/mapreduce-wsi/doc/index.html)).
  - `thesis/`: LATEX source code with all graphics to re-build the document.
  - `results/`: The original documents and spreadsheets used to track and analyze results.

####Documentation

  - [Evaluation](https://github.com/acgessler/mr-bonesim-thesis/blob/master/evaluation.md) (reproducing results, getting started with the prototype's source code).
  - [Hadoop/Ambari Cookbook](https://github.com/acgessler/mr-bonesim-thesis/blob/master/hadoop.md) (pitfalls and issues encountered when setting up a Hadoop cluster from scratch using Ambari, partly specific to CentOS 6.5).

####Abstract

	Recently, workflow technology has fostered the hope of the scientific community in that
	they could help complex scientific simulations to become easier to implement and maintain.
	The subject of this thesis is an existing workflow for a multi-scalar simulation which
	calculates the flux of porous mass in human bones. The simulation consists of separate
	systems-biological and bio-mechanical simulation steps coupled through additional data
	processing steps. The workflow exhibits a high potential for parallelism which is only
	used to a marginal degree. Thus we investigate whether _Big Data_ concepts such as
	MapReduce or NoSQL can be integrated into the workflow.
	
	A prototype of the workflow is developed using the Apache Hadoop ecosystem to parallelize
	the simulation and this prototype compared against a hand-parallelized baseline prototype
	in terms of performance and scalability. NoSQL concepts for storing inputs and results are
	utilized with an emphasis on HDFS, the Hadoop File System, as a schemaless distributed
	file system and MySQL Cluster as an intermediary between a classic database system and a
	NoSQL system.
	
	Lastly, the MapReduce-based prototype is implemented in the WS-BPEL workflow language
	using the SIMPL[0] framework and a custom Web Service to access
	Hadoop functionality. We show the simplicity of the resulting workflow model and argue
	that the approach greatly decreases implementation effort and at the same time enables
	simulations to scale to very large data volumes at ease.

        [0] P. Reimann, M. Reiter, H. Schwarz, D. Karastoyanova, F. Leymann. SIMPL - A 
        Framework for Accessing External Data in Simulation Workflows. In BTW, pp. 534–553.
        Kaiserslautern, Germany, 2011. 
