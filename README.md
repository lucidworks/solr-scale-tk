Solr Scale Toolkit
========

Fabric-based framework for deploying and managing SolrCloud clusters in the cloud.

Questions?
========

Please visit the Google Groups forum - https://groups.google.com/forum/#!forum/solr-scale-toolkit


Setup
========

Make sure you're running Python 2.7 and have installed Fabric and boto dependencies. 

On the Mac, you can do:

sudo easy_install fabric
sudo easy_install boto

Configure boto to connect to AWS by adding your credentials to: ~/.boto
```
[Credentials]
aws_access_key_id = ?
aws_secret_access_key = ?
```
For more information about boto, please see: https://github.com/boto/boto

For more information about fabric, see: http://docs.fabfile.org/en/1.8/

Clone the pysolr project from github and set it up as well:
```
git clone https://github.com/toastdriven/pysolr.git
cd pysolr
sudo python setup.py install
```
Note, you do not need to know any Python in order to use this framework.

AWS Setup
========

You'll need to setup a security group named solr-scale-tk (or update the fabfile.py to change the name).

At a minimum you should allow TCP traffic to ports: 8983, 8984-8989, SSH, and 2181 (ZooKeeper). However, it is your responsibility to review the security configuration of your cluster and lock it down appropriately.

You'll also need to create an keypair (using the Amazon console) named solr-scale-tk (you can rename the key used by the framework, see: AWS_KEY_NAME). After downloading the keypair file (solr-scale-tk.pem), save it to ~/.ssh/ and change permissions: chmod 600 ~/.ssh/solr-scale-tk.pem

Local Setup
========

You can also use the Fabric tasks against a local Solr cluster running on localhost. This requires two additional
setup tasks:

1) Enable passphraseless SSH to localhost, i.e. ssh username@localhost should log in immediately without prompting you for a password. Typically, this is accomplished by doing:
```
$ ssh-keygen -t dsa -P '' -f ~/.ssh/id_dsa
$ cat ~/.ssh/id_dsa.pub >> ~/.ssh/authorized_keys
```
2) Add the "local" cluster to your ~/.sstk file, such as:
```
{
  "clusters": {
    "local": {
      "name": "local",
      "hosts": [ "localhost" ],
      "provider:": "local",
      "ssh_user": "dstruan",
      "ssh_keyfile_path_on_local": "",
      "username": "${ssh_user}",
      "user_home": "/Users/${username}",
      "solr_java_home": "/Library/Java/JavaVirtualMachines/jdk1.7.0_67.jdk/Contents/Home",
      "zk_home": "${user_home}/zookeeper-3.4.5",
      "zk_data_dir": "${zk_home}/data",
      "instance_type": "m3.large",
      "sstk_cloud_dir": "${user_home}/projects/solr-scale-tk/cloud",
      "solr_tip": "${user_home}/solr-4.10.0",
      "fusion_home": "${user_home}/fusion"
    }
  }
}
```
The "local" cluster object overrides property settings that allows the Fabric tasks to work with your local directory structure / environment. For instance, the location of the Solr directory to use to launch the cluster will be resolved to: /Users/dstruan/solr-4.10.0 because the ${user_home} variable is resolved dynamically to /Users/dstruan.

Once you've defined all the properties of the local cluster, you can run any of the Fabric tasks that take a cluster ID using: fab <task>:local, such as fab setup_solrcloud:local,... will setup a SolrCloud cluster using the local property settings.

You can also override any of the global settings defined in fabfile.py using ~/.sstk. For instance, if you want to change the default value for the AWS_HVM_AMI_ID setting, you can do:

```
{
  "clusters": {
     ...
  },
  "AWS_HVM_AMI_ID": "?"
}
```

Overview
========

The solr-scale-tk framework works off the concept of a named "cluster". The named "cluster" concept is simply so that you don't need to worry about hostnames or IP addresses; the framework knows how to lookup hosts for a specific cluster. Behind the scenes, this uses Amazon tags to find instances and collect their host names.

Quick Demo

The easiest thing to do to ensure your environment is setup correctly and the script is working is to run:
```
fab demo:demo1,n=1
```
The demo command performs the following tasks in order:
1. Provisions one m3.medium instance in EC2
2. Sets up the meta node (SiLK, RabbitMQ, etc)
3. Configures one ZooKeeper server
4. Launches two Solr nodes in cloud mode (ports 8984 and 8985)
5. Creates a new collection named "demo" in SolrCloud; 1 shards, replication factor of 2
6. Indexes 10,000 synthetic documents in the demo collection

After verifying this works for you, take a moment to navigate to the Solr admin console and issue some queries against the collection. You can also go to the Solr admin console for the Solr instance that Logstash4Solr is using to index log messages from your SolrCloud nodes. After experimenting with the demo cluster, terminate the EC2 instance by doing: 
```
fab kill_mine
```
Fabric Commands

The demo command is cool, but to get the most out of the solr-scale-tk framework, you need to understand a little more about what's going on behind the scenes. Here's an example of how to launch a 3-node ZooKeeper ensemble and a 4-node SolrCloud cluster that talks to the ZooKeeper ensemble.
```
fab new_zk_ensemble:zk1,n=3 
fab new_solrcloud:cloud1,n=4,zk=zk1
fab new_collection:cluster=cloud1,name=test,shards=2,rf=2
```
Running these commands will take several minutes, but if all goes well, you'll have a collection named "test" with 2 shards and 2 replicas per shard in a 4-node SolrCloud cluster named "cloud1" and a 3-node ZooKeeper ensemble named "zk1" (7 EC2 instances in total). When this command finishes, you should see a message that looks similar to: 

Successfully launched new SolrCloud cluster cloud1; visit: SOLR_URL

Of course there is a lot going on here, so let's unpack these commands to get an idea of how the framework works.

new_zk_ensemble performs the following sub-commands for you:

new_ec2_instances(cluster='zk1', n=3, instance_type='m3.medium'): provisions 3 instances of our custom AMI on m3.medium VMs and tags each instance with the cluster name, such as "zk1"; notice that the type parameter used the default for ZK nodes: m3.medium

zk_ensemble(cluster='zk1', n=3): configures and starts ZooKeeper on each node in the cluster

All the nodes will be provisioned in the us-east-1 region by default. After the command finishes, you'll have a new ZooKeeper ensemble. Provisioning and launching a new ZK ensemble is separate from SolrCloud because you'll most likely want to reuse an ensemble between tests and it saves time to not have to re-create a ZooKeeper ensemble for each test. Your choice either way.

Next, we launch a SolrCloud cluster using the new_solrcloud command, which does the following:

new_ec2_instances(cluster='cloud1', n=4, instance_type='m3.medium'): provisions 4 instances of our custom AMI on m3.medium and tags each instance with the cluster name, such as "cloud1"; again the type uses the default m3.medium but you can override this by passing the instance_type='???' parameter on the command-line.

setup_solrcloud(cluster='cloud1', zk='zk1', nodesPerHost=1): configures and starts 1 Solr process in cloud mode per machine in the 'cloud1' cluster. The zkHost is determined from the zk parameter and all znodes are chroot'd using the cluster name 'cloud1'. If you're using larger instance types, then you can run more than one Solr process on a machine by setting the nodesPerHost parameter, which defaults to 1.

Lastly, we create a new collection named "test" in the SolrCloud cluster named "cloud1". At this point, you are ready to run tests.

Nuts and Bolts
========

Much of what the solr-scale-tk framework does is graceful handling of waits and status checking. For the most part, parameters have sensible defaults. Of course, parameters like number of nodes to launch don't have a sensible default, so you usually have to specify sizing type parameters. Overall, the framework breaks tasks into 2 phases:

Provisioning instances of our AMI
Configuring, starting, and checking services on the provisioned instances
The first step is highly dependent on Amazon Web Services. Working with the various services on the provisioned nodes mostly depends on SSH and standard Linux commands and can be ported to work with other server environments.
This two-step process implies that if step #1 completes successfully but an error / problem occurs in #2, that the nodes have already been provisioned and that you should not re-provision nodes. Let's look at an example to make sure this is clear. Specifically, imagine you get an error when running the new_solrcloud command. First you need to take note as to whether nodes have been provisioned. You will see green informational message about this in the console, such as:
** 2 EC2 instances have been provisioned **
If you see a message like this, then you know step 1 was successful and you only need to worry about correcting the problem and re-run step 2, which in our example would be running the solrcloud action. However, it is generally safe to just re-run the command (e.g. new_solrcloud) with the same cluster name as the framework will notice that the nodes are already provisioned and simply prompt you on whether it should re-use them.

Fabric Know-How

To see available commands, do:
```
fab -l
```
To see more information about a specific command, do:
```
fab -d <command>
```
Meta Node

You have the option of deploying a "meta" node to support testing. The meta node is intended to run shared services, such as log aggregation with Logstash4Solr. When you spin-up a new SolrCloud cluster using the new_solrcloud command, you have the option of specifying a meta node name. If supplied, then the Solr instances will be configured to integrate their log files with the meta node. Let's see an example of this in action to make sure it is clear:

```
fab new_meta_node:metta_world_peace
fab new_solrcloud:mycloud,n=3,meta=metta_world_peace
```

Running these commands will result in 4 instances being launched: 3 for SolrCloud and 1 meta node. The Solr nodes will send WARN and more severe log messages to Logstash4Solr on the meta node. You can find log messages by going to the Solr Query form for the Solr instance that logstash4solr is using, see URL in the console output for the new_meta_node command.

NOTE: The meta node is a work-in-progress, so please let us know if you have suggestions about other useful shared services we can deploy on the meta node.

Patching

Let's say you've spun up a cluster and need to patch the Solr core JAR file after fixing a bug. To do this, use the patch_jars command. Let's look at an example based on branch_4x. First, you need to build the JARs locally:
```
cd ~/dev/lw/projects/branch_4x/solr
ant clean example
cd ~/dev/lw/projects/solr-scale-tk
fab patch_jars:<cluster>,localSolrDir='~/dev/lw/projects/branch_4x/solr'
```
This will scp the solr-core-<VERS>.jar and solr-solrj-<VERS>.jar to each server, install them in the correct location for each Solr node running on the host, and then restart Solr. It performs this patch process very methodically so that you can be sure of the result. For example, after restarting, it polls the server until it comes back online to ensure the patch was successful (or not) before proceeding to the next server. Of course this implies the version value matches on your local workstation and on the server. Currently, we're using <VERS> == 4.7-SNAPSHOT.

The patch_jars command also supports a few other parameters. For example, let's say you only want to patch the core JAR on the first instance in the cluster:

fab patch_jars:<cluster>,localSolrDir='~/dev/lw/projects/branch_4x/solr',n=0,jars=core

Important Tips
========

The most important thing to remember is that you need to terminate your cluster when you are finished testing using the kill command. For instances, to terminate all 7 nodes created in our example above, you would do:

fab kill:cloud1

If you're not sure what clusters you are running, simply do:
fab mine

And, to kill all of your running instances, such as before signing off for the day, do:
fab kill_mine

The second most important thing to remember is that you don't want to keep provisioning new nodes. So if you try to run fab new_solrcloud:cloud1... and the nodes have already been provisioned for "cloud1", then you'll be prompted to decide if you are trying to create another cluster or whether you just intend to re-run parts of that command against the already provisioned nodes.

