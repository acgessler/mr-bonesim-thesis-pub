
This cookbook is written with Centos 6.5 in mind. Commands should translate directly to other `yum` based distributions (i.e. RedHat, SUSE). Some workarounds may not be required elsewhere. For Ubuntu etc. appropriate `apt` commands need ot be substituted, services such as `sshd` or `iptables` are configured differently and generally YMMV. 

###Setting up VMs

- Create the following VMs using the same shared key pair:
  1. 1 node to hold _Ambari_, Centos 6.5, can be a lowish VM size. Local disk is enough.
  2. `n` nodes to hold Hadoop cluster nodes. The ones I used has 4 vCPUs and 8 GiB RAM. Local disk 20 GiB is enough, boot from Centos 6.5 image. Do not create a volume at the same time, this causes occasional errors. Create the nodes individually and assign them useful host names (using automatic instance multiplication adds lengthy hex postfixes to the hostnames). __Important__: The host names need to be distinct and permanent. You can change the hostname locally on each of the nodes using `hostname` but this change does not propagate into the DNS.
- Create `n` volumes (empty) and attach them individually to the Hadoop nodes. The rest of this cookbook assumes that the attachment point is `/dev/vdb`.

The default login user to our Centos 6.5 image (the OpenStack default image) is `cloud-user`. The VMs only allow access with a SSH key. There is no password-based login and it is not possible to `su` to `root`. However, `cloud-user` is in SUDOERS so it can use the `sudo` command to gain root privileges. Do not and under no circumstances disable the `sshd` service running on any of the nodes.

###Getting parallel access to the VMs

The following instructions are to be executed on _Ambari_.

 - Manually `ssh` into the _Ambari_ node and install the shared key pair on it. ([Link](http://askubuntu.com/questions/15378/how-do-i-install-a-ssh-private-key-generated-by-puttygen)).  
 - Create a file called `~/all_hosts` and put the hostnames of all `n` Hadoop nodes into it. Use `ping` to verify those can actually be reached.
 - `yum install epel-release pssh`
 
Now the _Parallel-SSH_ tool (`pssh`) can be used to submit ssh commands to all Hadoop nodes in parallel. However I envountered two issues with it:

 - Some commands, most prominently `sudo`, require a (pseudo) terminal to run. The `-t -t` command to `ssh` enforces this. To have `pssh` pass this parameter though, use the `-x "-t -t" flag.`
 - In the OpenStack environment, IPs are dynamically assigned and do change over the lifetime of a VM (unless floating IPs are used, but these route via the gateway and are thus expectedly slower). This causes SSH warnings since the host keys change. It is best to disable host key checking altogether using `-O UserKnownHostsFile=/dev/null -O StrictHostKeyChecking=no`.

The final `pssh` command line becomes:

     pssh -h all_hosts -O UserKnownHostsFile=/dev/null -O StrictHostKeyChecking=no -l cloud-user -x"-t -t" -i 'COMMAND'

where `COMMAND` is the shell command to be executed on all hosts. 

###Preparing VM harddisks

After volumes have been attached to the Hadoop nodes, the additional storage does not naturally become available since the volume requires formatting before it can be mounted. To do so:
 
 - Format: `sudo mke2sf -F -F -t ext4 /dev/vdb`. __Important__: do __not__ use `pssh` to do this. Do it manually on all nodes in the cluster. The combined IO otherwise overloads the SAN, causing the entire operation to take hours.
 - Mount: `sudo mkdir /hdfs; sudo mount /dev/vdb /hdfs -t ext4`. This can be done using `pssh`.

This makes the volumes available in the `/hdfs/` folder of each of the nodes. Use the `df` tool to verify the storage capacity. __Note__: the mount command needs to be repeated if the nodes are restarted. Alternatively you can add an entry to each node's `/etc/fstab` configuration file.

###Misc. VM setup

In order for Ambari to be able to properly setup the cluster, the following steps are required on each Hadoop node. Use `pssh` as explained before to do it for all nodes at the same time.

 - Installing and turning on ntpd (network time server daemon):
 
     `sudo yum -y install ntp ntpdate ntp-doc; sudo /sbin/chkconfig ntpd on;sudo /etc/init.d/ntpd start`

 - Turn off IPTables (firewall - only safe if the cluster is within a protected network): 
 
     `sudo /etc/init.d/iptables stop; sudo chkconfig iptables off`

###Setup mit Ambari

 - Install Ambari on _Ambari_ using [these instructions](https://cwiki.apache.org/confluence/display/AMBARI/Install+Ambari+1.7.0+from+Public+Repositories).
 - If you plan to access the Ambari web UI from a different machine that _Ambari_) (i.e. from a Windows VM with a Remote Desktop Connection), you may have to set a command line flag to change the network interface it uses to `0.0.0.0` (`ambari-server start - ..`).
 - Follow the setup steps. You want the latest software stack. When asked for host names, give the fully qualified host names (i.e. with the `.novalocal` postfix). Do not use IP addresses since they are dynamic, and some parts of the setup scripts do rely on having proper host names.
 - You can leave Ambari's defaults in most cases. However: 
    - Setup dummy data for the Ganglia reporting mail address. 
    - If you want to exactly reproduce the results from the thesis, lower the HDFS block size to 25217536 (in the _Advanced_ section of the HDFS configuration tab).
    - Put HDFS DataNode's and the Client programs on all the nodes.
 - Deploy. Afterwards, verify everything is ok by looking at the Ambari web dashboard and running a test mapreduce (for example the builtin examples, i.e. `terasort` or `pi`, [Link](http://stackoverflow.com/questions/16312868/where-are-the-hadoop-examples-and-hadoop-test-jars-in-cdh4-2)). If you have trouble running them due to permission errors, try creating HDFS user folders for the current user (or `root` if you run the job via `sudo`):
 
      `sudo -u hdfs hadoop fs -mkdir /user/USERNAME`
      `sudo -u hdfs hadoop fs -chown GROUP:USERNAME /user/USERNAME`





