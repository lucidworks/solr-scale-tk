from fabric.api import *
from fabric.colors import green as _green, blue as _blue, red as _red, yellow as _yellow
from fabric.contrib.files import append as _fab_append, exists as _fab_exists
from fabric.contrib.console import confirm
from StringIO import StringIO as _strio    
import boto
from boto.s3.key import Key as _S3Key
import boto.ec2
import time
import sys
import urllib2
import getpass
import json
import pysolr
import os.path
import datetime
import dateutil.parser
import shutil

# Global constants used in this module; only change this if you know what you're doing ;-)
CLUSTER_TAG = 'cluster'
USERNAME_TAG = 'username'
INSTANCE_STORES_TAG = 'numInstanceStores'
AWS_PV_AMI_ID = 'ami-806bc8e8'
AWS_HVM_AMI_ID = 'ami-0664c76e'
AWS_AZ = 'us-east-1b'
AWS_INSTANCE_TYPE = 'm3.medium'
AWS_SECURITY_GROUP = 'solr-scale-tk'
AWS_KEY_NAME = 'solr-scale-tk'
ssh_user = 'ec2-user'
user_home = '/home/' + ssh_user
ssh_keyfile_path_on_local = '~/.ssh/solr-scale-tk.pem'
zk_data_dir = '/vol0/data'
CTL_SCRIPT = 'solr-ctl.sh'
ENV_SCRIPT = 'solr-ctl-env.sh'

# default config settings if not specifically overridden in the user's ~/.sstk file
_config = {}
_config['provider'] = 'ec2'
_config['user_home'] = user_home
_config['ssh_keyfile_path_on_local'] = ssh_keyfile_path_on_local
_config['ssh_user'] = ssh_user
_config['solr_java_home'] = '${user_home}/jdk1.7.0_67'
_config['solr_tip'] = '${user_home}/solr-4.10.0'
_config['zk_home'] = '${user_home}/zookeeper-3.4.6'
_config['zk_data_dir'] = zk_data_dir
_config['sstk_cloud_dir'] = '${user_home}/cloud'
_config['SSTK_ENV'] = '${sstk_cloud_dir}/' + ENV_SCRIPT
_config['SSTK'] = '${sstk_cloud_dir}/' + CTL_SCRIPT
_config['AWS_PV_AMI_ID'] = AWS_PV_AMI_ID
_config['AWS_HVM_AMI_ID'] = AWS_HVM_AMI_ID
_config['AWS_AZ'] = AWS_AZ
_config['AWS_SECURITY_GROUP'] = AWS_SECURITY_GROUP
_config['AWS_INSTANCE_TYPE'] = AWS_INSTANCE_TYPE
_config['AWS_KEY_NAME'] = AWS_KEY_NAME
_config['fusion_home'] = '${user_home}/fusion'

instanceStoresByType = { 'm1.small':0, 'm3.medium':1, 'm3.large':1, 'm3.xlarge':2,
                        'm3.2xlarge':2, 'i2.4xlarge':4,'i2.2xlarge':2, 'i2.8xlarge':8,
                        'r3.large':1, 'r3.xlarge':1, 'r3.2xlarge':1, 'r3.4xlarge':1 }

class _HeadRequest(urllib2.Request):
    def get_method(self):
        return 'HEAD'

def _status(msg):
    print(_yellow(msg))

def _info(msg):
    print(_green(msg))

def _warn(msg):
    print(_blue('WARN: ' + msg))

def _error(msg):
    sys.stderr.write(_red('\n\t************************'))
    sys.stderr.write(_red('\n\tERROR: %s\n' % str(msg)))
    sys.stderr.write(_red('\t************************\n\n'))

# Helper to log a message and kill the application after a fatal error occurs.
def _fatal(msg):
    _error(msg)
    exit(1)

def _copy_dir(src, dest):
    try:
        shutil.copytree(src, dest)
    # Directories are the same
    except shutil.Error as e:
        print('Directory not copied. Error: %s' % e)
    # Any error saying that the directory doesn't exist
    except OSError as e:
        print('Directory not copied. Error: %s' % e)

def _save_config():
    sstk_cfg = _get_config()
    sstkCfg = os.path.expanduser('~/.sstk')
    sstkCfgFile = open(sstkCfg, 'w')
    sstkCfgFile.write(json.dumps(sstk_cfg, indent=2))
    sstkCfgFile.close()

def _expand_config_var(cluster, val):
    if len(val) > 0:
        startVar = val.find('${')
        while startVar != -1:
            endVar = val.find('}',startVar+1)
            varStr = val[startVar:endVar+1]
            varVal = _env(cluster, varStr[2:len(varStr)-1])
            val = val.replace(varStr, varVal)
            startVar = val.find('${')
    return val

def _get_config():
    if _config.has_key('sstk_cfg'):
        return _config['sstk_cfg']

    sstkCfg = os.path.expanduser('~/.sstk')
    if os.path.isfile(sstkCfg) is False:
        _config['sstk_cfg'] = {}
    else:
        sstkCfgFile = open(sstkCfg)
        sstkJson = json.load(sstkCfgFile)
        sstkCfgFile.close()
        _config['sstk_cfg'] = sstkJson

    return _config['sstk_cfg']

# resolves an environment property by first checking the cluster specific settings,
# then user specific settings, then global settings
def _env(cluster, key):
    return _env(cluster, key, None)

def _env(cluster, key, defaultValue=None):

    if key is None:
        _fatal('Property key is required!')

    val = None
    sstk_cfg = _get_config()
    if cluster is not None:
        if sstk_cfg.has_key('clusters') and sstk_cfg['clusters'].has_key(cluster):
            if sstk_cfg['clusters'][cluster].has_key(key):
                val = sstk_cfg['clusters'][cluster][key]

    if val is None:
        if sstk_cfg.has_key(key):
            val = sstk_cfg[key]

    if val is None:
        if _config.has_key(key):
            val = _config[key]

    if val is None:
        val = defaultValue

    if val is None:
        _fatal('Unable to resolve required property setting: '+key)

    return _expand_config_var(cluster, val)

def _get_ec2_provider(region):
    if region is not None:
        return boto.ec2.connect_to_region(region)
    else:
        return boto.connect_ec2()

# Get a handle to a Cloud Provider API (or mock for local mode)
def _provider_api():
    sstk_cfg = _get_config()

    if sstk_cfg.has_key('provider') is False:
        sstk_cfg['provider'] = 'ec2' # default

    provider = sstk_cfg['provider']
    if provider == 'ec2':
        region = None
        if sstk_cfg.has_key('region'):
            region = sstk_cfg['region']
        return _get_ec2_provider(region)
    else:
        _fatal(provider+' not supported! Please correct your ~/.sstk configuration file.')
        return None

# Polls until instances are running, up to a max wait
def _poll_for_running_status(rsrv, maxWait=180):
    _info('Waiting for ' + str(len(rsrv.instances)) + ' instances to start (will wait for a max of 3 minutes) ...')

    startedAt = time.time()
    waitTime = 0
    sleepInterval = 15
    runningSet = set([])
    allRunning = False
    while allRunning is False and waitTime < maxWait:
        allRunning = True
        for inst in rsrv.instances:
            if inst.id in runningSet:
                continue

            status = inst.update()
            _status('Instance %s has status %s after %d seconds' % (inst.id, status, (time.time() - startedAt)))
            if status == 'running':
                runningSet.add(inst.id)
            else:
                allRunning = False

        if allRunning is False:
            time.sleep(sleepInterval)
            waitTime = round(time.time() - startedAt)

    if allRunning:
        _info('Took %d seconds to launch %d instances.' % (time.time() - startedAt, len(runningSet)))
    else:
        _warn('Only %d of %d instances running after waiting %d seconds' % (len(runningSet), len(rsrv.instances), waitTime))

    return len(runningSet)

def _find_instances_in_cluster(cloud, cluster, onlyIfRunning=True):
    tagged = {}
    byTag = cloud.get_all_instances(filters={'tag:' + CLUSTER_TAG:cluster})
    for rsrv in byTag:
        for inst in rsrv.instances:
            if (onlyIfRunning and inst.state == 'running') or onlyIfRunning is False:
                tagged[inst.id] = inst.public_dns_name

    return tagged

def _find_all_instances(cloud, onlyIfRunning=True):
    tagged = {}
    byTag = cloud.get_all_instances(filters={'tag-key':'cluster','tag-key':'username'})
        
    for rsrv in byTag:
        for inst in rsrv.instances:
            if (onlyIfRunning and inst.state == 'running') or onlyIfRunning is False:
                tagged[inst.id] = inst

    return tagged


def _find_user_instances(cloud, username, onlyIfRunning=True):
    tagged = {}
    byTag = cloud.get_all_instances(filters={'tag:' + USERNAME_TAG:username})
        
    for rsrv in byTag:
        for inst in rsrv.instances:
            if (onlyIfRunning and inst.state == 'running') or onlyIfRunning is False:
                tagged[inst.id] = inst

    return tagged

def _is_solr_up(hostAndPort):
    isSolrUp = False
    try:
        urllib2.urlopen(_HeadRequest('http://%s/solr/#/' % hostAndPort))
        # if no exception on the ping, assume the HTTP listener is up
        _info('Solr at ' + hostAndPort + ' is online.')
        isSolrUp = True
    except:
        # ignore it as we're just checking if the HTTP listener is up
        # print "Unexpected error:", sys.exc_info()[0]
        isSolrUp = False

    return isSolrUp

# Test for SSH connectivity to an instance
def _ssh_to_new_instance(host):
    sshOk = False
    with settings(host_string=host), hide('output', 'running', 'warnings'):
        try:
            run('uname -a')
            sshOk = True
        except:
            sshOk = False

    return sshOk

def _cluster_hosts(cloud, cluster):

    clusterHosts = None
    sstkCfg = _get_config()
    if sstkCfg.has_key('clusters') and sstkCfg['clusters'].has_key(cluster):
        if sstkCfg['clusters'][cluster].has_key('hosts'):
            clusterHosts = sstkCfg['clusters'][cluster]['hosts']

    if clusterHosts is None:
        # not cached locally ... must hit provider API
        clusterHosts = []
        taggedInstances = _find_instances_in_cluster(cloud, cluster)
        for key in taggedInstances.keys():
            clusterHosts.append(taggedInstances[key])
        if len(clusterHosts) == 0:
            _fatal('No active hosts found for cluster ' + cluster + '! Check your command line args and re-try')
        # use a predictable order each time
        clusterHosts.sort()

    # setup the Fabric env for SSH'ing to this cluster
    ssh_user = _env(cluster, 'ssh_user')
    ssh_keyfile = _env(cluster, 'ssh_keyfile_path_on_local')

    if len(ssh_keyfile) > 0 and os.path.isfile(os.path.expanduser(ssh_keyfile)) is False:
        _fatal('SSH key file %s not found!' % ssh_keyfile)
    env.hosts = []
    env.user = ssh_user
    env.key_filename = ssh_keyfile

    return clusterHosts

def _verify_ssh_connectivity(hosts, maxWait=120):
    # if using localhost for this cluster, no need to SSH
    if len(hosts) == 1 and hosts[0] == 'localhost':
        return

    _status('Verifying SSH connectivity to %d hosts' % len(hosts))

    waitTime = 0
    startedAt = time.time()
    hasConn = False
    sshSet = set([])
    while hasConn is False and waitTime < maxWait:
        hasConn = True  # assume true and prove false with SSH failure
        for host in hosts:
            if (host in sshSet) is False:
                if _ssh_to_new_instance(host):
                    sshSet.add(host)
                else:
                    hasConn = False
        if hasConn is False:
            time.sleep(5)
            waitTime = round(time.time() - startedAt)
            _status('Waited %d seconds so far to verify SSH connectivity to %d hosts' % (waitTime, len(hosts)))

    if hasConn:
        _info('Verified SSH connectivity to %d hosts.' % len(sshSet))
    else:
        _warn('SSH connectivity verification timed out after %d seconds! Verified %d of %d' % (maxWait, len(sshSet), len(hosts)))

    return hasConn

def _gen_zoo_cfg(zkDataDir, zkHosts):
    zoo_cfg = ''
    zoo_cfg += 'tickTime=2000\n'
    zoo_cfg += 'initLimit=10\n'
    zoo_cfg += 'syncLimit=5\n'
    zoo_cfg += 'dataDir='+zkDataDir+'\n'
    zoo_cfg += 'clientPort=2181\n'
    numHosts = len(zkHosts)
    if numHosts > 1:
        for z in range(0, numHosts):
            zoo_cfg += 'server.%d=%s:2888:3888\n' % (z + 1, zkHosts[z])

    return zoo_cfg

# some silly stuff going on here ...
def _gen_log4j_cfg(localId=None, rabbitMqHost=None, mqLevel='WARN'):
    cfg = ''

    if rabbitMqHost is None:
        cfg += 'log4j.rootLogger=INFO, file'
    else:
        cfg += 'log4j.rootLogger=INFO, file, rabbitmq'

    cfg += '''
log4j.appender.file=org.apache.log4j.RollingFileAppender
log4j.appender.file.MaxFileSize=200MB
log4j.appender.file.MaxBackupIndex=20
log4j.appender.file.File=logs/solr.log
log4j.appender.file.layout=org.apache.log4j.PatternLayout
log4j.appender.file.layout.ConversionPattern=%d{ISO8601} [%t] %-5p %c{3} %x - %m%n
log4j.logger.org.apache.zookeeper=WARN
log4j.logger.org.apache.http=WARN
log4j.logger.org.apache.solr.update.processor.LogUpdateProcessor=WARN
'''

    if rabbitMqHost is not None:
        cfg += '''
log4j.appender.rabbitmq=com.plant42.log4j.appenders.RabbitMQAppender
log4j.appender.rabbitmq.threshold=''' + mqLevel + '''
log4j.appender.rabbitmq.identifier=''' + localId + '''
log4j.appender.rabbitmq.host=''' + rabbitMqHost + '''
log4j.appender.rabbitmq.port=5672
log4j.appender.rabbitmq.username=guest
log4j.appender.rabbitmq.password=guest
log4j.appender.rabbitmq.virtualHost=/
log4j.appender.rabbitmq.exchange=log4j-exchange
log4j.appender.rabbitmq.type=direct
log4j.appender.rabbitmq.durable=false
log4j.appender.rabbitmq.queue=log4j-queue
log4j.appender.rabbitmq.layout=com.plant42.log4j.layouts.JSONLayout
'''
    return cfg

def _get_solr_in_sh(cluster, remoteSolrJavaHome, solrJavaMemOpts, zkHost):

    provider = _env(cluster, 'provider')
    if provider == "local":
        solrHost = "localhost"
    else:
        solrHost = '`curl -s http://169.254.169.254/latest/meta-data/public-hostname`'

    solrInSh = ('''#!/bin/bash
SOLR_JAVA_HOME="%s"

# Increase Java Min/Max Heap as needed to support your indexing / query needs
SOLR_JAVA_MEM="%s"

# Enable verbose GC logging
GC_LOG_OPTS="-verbose:gc -XX:+PrintHeapAtGC -XX:+PrintGCDetails \
-XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps -XX:+PrintTenuringDistribution -XX:+PrintGCApplicationStoppedTime"

# These GC settings have shown to work well for a number of common Solr workloads
GC_TUNE="-XX:-UseSuperWord \
-XX:NewRatio=3 \
-XX:+UseParNewGC \
-XX:ConcGCThreads=4 -XX:ParallelGCThreads=4 \
-XX:SurvivorRatio=4 \
-XX:TargetSurvivorRatio=90 \
-XX:MaxTenuringThreshold=8 \
-XX:+UseConcMarkSweepGC \
-XX:+CMSScavengeBeforeRemark \
-XX:PretenureSizeThreshold=64m \
-XX:CMSFullGCsBeforeCompaction=1 \
-XX:+UseCMSInitiatingOccupancyOnly \
-XX:CMSInitiatingOccupancyFraction=50 \
-XX:CMSTriggerPermRatio=80 \
-XX:CMSMaxAbortablePrecleanTime=6000 \
-XX:+CMSParallelRemarkEnabled \
-XX:+ParallelRefProcEnabled \
-XX:+AggressiveOpts"

# Mac OSX and Cygwin don't seem to like the UseLargePages flag
thisOs=`uname -s`
# for now, we don't support running this script from cygwin due to problems
# like not having lsof, ps waux, curl, and awkward directory handling
if [[ "$thisOs" != "Darwin" && "${thisOs:0:6}" != "CYGWIN" ]]; then
  # UseLargePages flag causes JVM crash on Mac OSX
  GC_TUNE="$GC_TUNE -XX:+UseLargePages"
fi

# Set the ZooKeeper connection string if using an external ZooKeeper ensemble
# e.g. host1:2181,host2:2181/chroot
# Leave empty if not using SolrCloud
ZK_HOST="%s"

# Set the ZooKeeper client timeout (for SolrCloud mode)
#ZK_CLIENT_TIMEOUT="15000"

# By default the start script uses "localhost"; override the hostname here
# for production SolrCloud environments to control the hostname exposed to cluster state
SOLR_HOST=%s

# By default the start script uses UTC; override the timezone if needed
#SOLR_TIMEZONE="UTC"

# By default the start script enables some RMI related parameters to allow attaching
# JMX savvy tools like VisualVM remotely, set to "false" to disable that behavior
# (recommended in production environments)
ENABLE_REMOTE_JMX_OPTS="true"
    ''' % (remoteSolrJavaHome, solrJavaMemOpts, zkHost, solrHost))

    return solrInSh


def _get_zk_hosts(cloud, zk):
    zkHosts = []
    zkInstances = _find_instances_in_cluster(cloud, zk)
    for key in zkInstances.keys():
        zkHosts.append(zkInstances[key] + ':2181')
    return zkHosts

def _check_zk_health(n, hosts, maxWait=60):
    waitTime = 0
    startedAt = time.time()
    zkHealthy = False
    zkNodeSet = set([])
    while zkHealthy is False and waitTime < maxWait:
        zkHealthy = True
        for z in range(0, n):
            if (hosts[z] in zkNodeSet) is False:
                with settings(host_string=hosts[z]), hide('output', 'running', 'warnings'):
                    zkSrvrResp = run('echo srvr | nc localhost 2181 || true')
                    if zkSrvrResp is None or zkSrvrResp == "" or zkSrvrResp.lower().find('mode: ') == -1:
                        _warn('ZooKeeper server on host %s seems to be unhealthy? %s' % (hosts[z], zkSrvrResp))
                        zkHealthy = False
                    else:
                        zkNodeSet.add(hosts[z])
        if maxWait == 1:
            break

        if zkHealthy is False:
            time.sleep(5)
            waitTime = round(time.time() - startedAt)
            _status('Waited %d seconds so far to verify ZooKeeper health on %d hosts' % (waitTime, n))

    if zkHealthy:
        _info('ZooKeeper is healthy on %d hosts.' % len(zkNodeSet))
    else:
        _warn('ZooKeeper health checks timed out after %d seconds! Verified %d of %d' % (maxWait, len(zkNodeSet), n))

def _lookup_hosts(cluster, verify_ssh=False):

    hosts = None
    # first, check our local cache for cluster hosts
    sstkCfg = _get_config()
    if sstkCfg.has_key('clusters') and sstkCfg['clusters'].has_key(cluster):
        if sstkCfg['clusters'][cluster].has_key('hosts'):
            hosts = sstkCfg['clusters'][cluster]['hosts']

    if hosts is None:
        # hosts not found in the local config
        cloud = _provider_api()
        hosts = _cluster_hosts(cloud, cluster)
        cloud.close()

        # cache the hosts in the local config
        if sstkCfg.has_key('clusters') is False:
            sstkCfg['clusters'] = {}

        if sstkCfg['clusters'].has_key(cluster) is False:
            sstkCfg['clusters'][cluster] = {}

        sstkCfg['clusters'][cluster]['hosts'] = hosts

        _save_config()
    else:
        # setup the Fabric env for SSH'ing to this cluster
        ssh_user = _env(cluster, 'ssh_user')
        ssh_keyfile = _env(cluster, 'ssh_keyfile_path_on_local')
        if len(ssh_keyfile) > 0 and os.path.isfile(os.path.expanduser(ssh_keyfile)) is False:
            _fatal('SSH key file %s not found!' % ssh_keyfile)
        env.hosts = []
        env.user = ssh_user
        env.key_filename = ssh_keyfile

    if verify_ssh:
        _verify_ssh_connectivity(hosts)

    return hosts

def _zk_ensemble(cluster, hosts):
    n = len(hosts)
    zoo_cfg = _gen_zoo_cfg(_env(cluster,'zk_data_dir'), hosts)

    zkHosts = []
    for z in range(0, n):
        zkHosts.append(hosts[z] + ':2181')

    zkDir = _env(cluster, 'zk_home')
    remoteZooCfg = zkDir + '/conf/zoo.cfg'

    zkDataDir = _env(cluster,'zk_data_dir')

    remoteZooMyid = zkDataDir+'/myid'

    _status('Setting up ZooKeeper on ' + str(','.join(zkHosts)))

    sshUser = _env(cluster, 'ssh_user')

    provider = _env(cluster, 'provider')

    for z in range(0, n):
        with settings(host_string=hosts[z]), hide('output', 'running', 'warnings'):
            # stop zk
            run(zkDir + '/bin/zkServer.sh stop')

            # setup to clean snapshots
            if provider == 'local':
                local('mkdir -p '+zkDataDir+' || true')
                local('rm -rf '+zkDataDir+'/*')
            else:
                put('zk_cron.sh', zkDir)
                run('chmod +x %s/zk_cron.sh && echo "0 * * * *  %s/zk_cron.sh" > %s/crondump && crontab < %s/crondump' % (zkDir, zkDir, zkDir, zkDir))
                sudo('mkdir -p '+zkDataDir+' || true')
                sudo('rm -rf '+zkDataDir+'/*')
                sudo('chown -R '+sshUser+': '+zkDataDir)

            run('rm -f ' + remoteZooCfg)
            _fab_append(remoteZooCfg, zoo_cfg)
            if (n > 1):
                _fab_append(remoteZooMyid, str(z + 1))

    # restart after stopping and clearing data
    for z in range(0, n):
        with settings(host_string=hosts[z]), hide('output', 'running', 'warnings'):
            run(zkDir + '/bin/zkServer.sh start')

    _info('Started ZooKeeper on %d nodes ... checking status' % n)
    time.sleep(5)
    _check_zk_health(n, hosts, 60)

    return zkHosts

def _wait_to_see_solr_up_on_hosts(hostAndPorts, maxWait=180):
    time.sleep(5)
    waitTime = 0
    startedAt = time.time()
    allUp = False
    upSet = set([])
    downSet = set([])
    while allUp is False and waitTime < maxWait:
        # assume all are up at the beginning of each loop and then prove false if we see one that isn't
        allUp = True
        for srvr in hostAndPorts:
            if (srvr in upSet) is False:
                if _is_solr_up(srvr):
                    upSet.add(srvr)
                    try:
                        downSet.remove(srvr)
                    except:
                        pass                    
                else:
                    allUp = False
                    downSet.add(srvr)


        # sleep a little between loops to give the servers time to start
        if allUp is False:
            time.sleep(5)
            waitTime = round(time.time() - startedAt)

    if allUp:
        _info('%d Solr servers are up!' % len(upSet))
    else:
        _error('Only %d of %d Solr servers came up within %d seconds.' % (len(upSet), len(hostAndPorts), maxWait))
        for down in downSet:
            _error(down+' is down!')

def _restart_solr(cluster, host, solrPortBase):
    binSolrScript = _env(cluster, 'solr_tip') + '/bin/solr'
    zkHost = _read_cloud_env(cluster)['ZK_HOST'] # get the zkHost from the env on the server
    solrPort = '89' + solrPortBase
    solrDir =  'cloud'+solrPortBase
    remoteStartCmd = '%s start -cloud -p %s -d %s' % (binSolrScript, solrPort, solrDir)
    with settings(host_string=host), hide('output', 'running', 'warnings'):
        _status('Running start on remote: ' + remoteStartCmd)
        run('nohup ' + remoteStartCmd + ' &', pty=False)
        time.sleep(2)

def _stop_solr(cluster, host, solrPortBase):
    binSolrScript = _env(cluster, 'solr_tip') + '/bin/solr'
    solrPort = '89' + solrPortBase
    remoteStopCmd = '%s stop -p %s' % (binSolrScript, solrPort)
    with settings(host_string=host), hide('output', 'running', 'warnings'):
        _status('Running stop Solr on '+host+': ' + remoteStopCmd)
        run(remoteStopCmd)
        time.sleep(2)

def _get_instance_type(cloud, cluster):
    instance_type = _env(cluster, 'instance_type', 'null')
    if instance_type != 'null':
        return instance_type

    byTag = cloud.get_all_instances(filters={'tag:' + CLUSTER_TAG:cluster})
    if len(byTag) > 0:
        return byTag[0].instances[0].instance_type
    return None

def _get_solr_java_memory_opts(instance_type, numNodesPerHost):
    
    _status('Determining Solr Java memory options for running %s Solr nodes on a %s' % (numNodesPerHost, instance_type))
    
    if instance_type is None or numNodesPerHost <= 0: # garbage in, just use default
        return '-Xms512m -Xmx512m -XX:MaxPermSize=512m -XX:PermSize=256m'

    showWarn = False
    if instance_type == 'm1.small':
        if numNodesPerHost == 1:
            mx = '512m'
        elif numNodesPerHost == 2:
            mx = '256m'
        else:
            showWarn = True
            mx = '128m'
    elif instance_type == 'm3.medium':
        if numNodesPerHost == 1:
            mx = '1g'
        elif numNodesPerHost == 2:
            mx = '512m'
        elif numNodesPerHost == 3:
            mx = '384m'
        else:
            showWarn = True
            mx = '256m'
    elif instance_type == 'm3.large':
        if numNodesPerHost == 1:
            mx = '3g'
        elif numNodesPerHost == 2:
            mx = '1536m'
        elif numNodesPerHost == 3:
            mx = '1g'
        elif numNodesPerHost == 4:
            mx = '768m'
        else:
            showWarn = True
            mx = '512m'
    elif instance_type == 'm3.xlarge' or instance_type == 'r3.large':
        if numNodesPerHost == 1:
            mx = '7g'
        elif numNodesPerHost == 2:
            mx = '3g'
        elif numNodesPerHost == 3:
            mx = '2g'
        elif numNodesPerHost == 4:
            mx = '1536m'
        elif numNodesPerHost == 5:
            mx = '1g'
        else:
            showWarn = True
            mx = '640m'
    elif instance_type == 'm3.2xlarge' or instance_type == 'r3.xlarge':
        if numNodesPerHost == 1:
            mx = '15g'
        elif numNodesPerHost == 2:
            mx = '8g'
        elif numNodesPerHost == 3:
            mx = '6g'
        elif numNodesPerHost == 4:
            mx = '5g'
        elif numNodesPerHost == 5:
            mx = '4g'
        else:
            showWarn = True
            mx = '3g'
    elif instance_type == 'r3.2xlarge':
        mx = '12g'
    elif instance_type == 'i2.4xlarge' or instance_type == 'r3.4xlarge':
        if numNodesPerHost <= 4:
            mx = '12g'
        else:
            showWarn = True
            mx = '10g'
    else:
        _warn('Memory settings for %s instances not configured! Please update this script to set appropriate memory settings.' % instance_type)
        if numNodesPerHost == 1:
            mx = '8g'
        elif numNodesPerHost == 2:
            mx = '6g'
        elif numNodesPerHost == 3:
            mx = '4g'
        else:
            mx = '2g'
        
    if showWarn:
        _warn('%d nodes on an %s is probably too many! Consider using a larger instance type.' % (numNodesPerHost, instance_type))    
        
    return '-Xms%s -Xmx%s -XX:MaxPermSize=512m -XX:PermSize=256m' % (mx, mx)
        
def _uptime(launch_time):
    launchTime = dateutil.parser.parse(launch_time)
    now = datetime.datetime.utcnow()
    diff = now - launchTime.replace(tzinfo=None)
    deltaStr = str(diff)
    dotAt = deltaStr.find('.')
    if dotAt != -1:
        deltaStr = deltaStr[0:dotAt]                
    return ' for '+deltaStr

def _parse_env_data(envData):
    cloudEnvVars = {}
    for line in envData.split('\n'):
        line = line.strip()
        if len(line) == 0 or line.startswith('#'):
            continue
        if line.startswith('export '):
            line = line[len('export '):]
        eqAt = line.find('=')
        if eqAt != -1:
            name = line[0:eqAt].strip()
            valu = line[eqAt+1:].strip()
            if len(name) > 0 and len(valu) > 0:
                if valu.startswith('"'):
                    valu = valu[1:]
                if valu.endswith('"'):
                    valu = valu[0:len(valu)-1]
                cloudEnvVars[name] = valu

    return cloudEnvVars
    
def _read_cloud_env(cluster):
    sstkEnvScript = _env(cluster, 'SSTK_ENV')
    hosts = _lookup_hosts(cluster, False)
    with settings(host_string=hosts[0]), hide('output', 'running', 'warnings'):
        cloudEnvReader = _strio()
        get(sstkEnvScript, cloudEnvReader)
        envData = cloudEnvReader.getvalue()
    return _parse_env_data(envData)
    
def _num_solr_nodes_per_host(cluster):
    cloudEnv = _read_cloud_env(cluster)
    return 1 if cloudEnv.has_key('NODES_PER_HOST') is False else int(cloudEnv['NODES_PER_HOST'])

def _rolling_restart_solr(cloud, cluster, solrHostsAndPortsToRestart=None, wait=0, waitAfterKill=30):

    remoteSolrDir = _env(cluster, 'solr_tip')

    # determine which servers to patch if they weren't passed into this method
    if solrHostsAndPortsToRestart is None:
        hosts = _cluster_hosts(cloud, cluster)
    
        numNodes = _num_solr_nodes_per_host(cluster)
        activePorts = []
        for n in range(0,numNodes):
            activePorts.append(str(84 + n))
    
        # upload the latest version of the script before restarting    
        # determine the active Solr nodes on each host
        solrHostsAndPortsToRestart = {}            
        for host in hosts:
            solrHostsAndPortsToRestart[host] = set([]) # set is important
            with settings(host_string=host), hide('output', 'running', 'warnings'):
                for port in activePorts:
                    if _fab_exists(remoteSolrDir + '/cloud' + port):
                        solrHostsAndPortsToRestart[host].add(port)

    _status('Doing a rolling restart (which can take a while so be patient) ...')

    zkHost = _read_cloud_env(cluster)['ZK_HOST']
    remoteSolrJavaHome = _env(cluster, 'solr_java_home')
    solrJavaMemOpts = _read_cloud_env(cluster)['SOLR_JAVA_MEM']
    solrInSh = _get_solr_in_sh(cluster, remoteSolrJavaHome, solrJavaMemOpts, zkHost)
    solrInShPath = remoteSolrDir+'/bin/solr.in.sh'

    for solrHost in solrHostsAndPortsToRestart.keys():

        # update the solr.in.sh file to the latest settings
        with settings(host_string=solrHost), hide('output', 'running', 'warnings'):
            run('rm -f '+solrInShPath)
            _fab_append(solrInShPath, solrInSh)

        for port in solrHostsAndPortsToRestart[solrHost]:
            solrSrvr = '%s:89%s' % (solrHost, port)
            _status('Restarting Solr node on %s' % solrSrvr)
            _stop_solr(cluster, solrHost, port)
            _status('Sleeping for 30 seconds before restarting Solr on '+solrSrvr)
            time.sleep(int(waitAfterKill)) # need to sleep before restarting so /live_nodes gets updated and leaders migrate
            _restart_solr(cluster, solrHost, port)
            if int(wait) <= 0:
                _status('Restarted ... waiting to see Solr come back up ...')
                _wait_to_see_solr_up_on_hosts([solrSrvr], maxWait=180)
            else:
                _status('Restarted ... sleeping for '+str(wait)+' seconds before proceeding ...')
                time.sleep(int(wait))
            
    _info('Rolling restart completed.')
    
def _configure_banana(metaHost):
    # Setup the correct hostname for the banana UI
    bananaWebapp = user_home + '/slk-4.7.0/solr-4.7.0/SiLK/banana-webapp/webapp'
    bananaDefaultJson = bananaWebapp + '/app/dashboards/default.json'
    put('banana_monitoring_db.json', bananaDefaultJson)
    solrHost = '''"solr": {
    "server": "http://''' +metaHost+ ''':8983/solr/",
    "core_name": "logstash_logs"
  }
}'''
    _fab_append(bananaDefaultJson, solrHost)
        
    solrConfigJs = '''define(['settings'],
function (Settings) {
  "use strict";
  return new Settings({
    elasticsearch: "http://localhost:9200",
    solr: "http://'''+metaHost+''':8983/solr/",
    solr_core: "logstash_logs",
    kibana_index: "kibana-int",
    panel_names: [
      'histogram',
      'table',
      'timepicker',
      'text',
      'column',
      'query',
      'terms'
    ]
  });
});
'''
    bananaConfigJs = bananaWebapp + '/config.js'
    run('rm '+bananaConfigJs)
    _fab_append(bananaConfigJs, solrConfigJs)
    
def _setup_instance_stores(hosts, numInstStores, ami, xdevs):
    numStores = int(numInstStores)
    if numStores <= 0:
        return

    hvmAmiId = _env(None, 'AWS_HVM_AMI_ID')

    for h in range(0,len(hosts)):
        with settings(host_string=hosts[h]): #, hide('output', 'running', 'warnings'):
            for v in range(0,numStores):
                # get rid of the silly /mnt point which only sometimes gets
                # setup correctly by Amazon
                if ami == hvmAmiId and v == 0:
                    sudo('rm -rf /vol0')
                    sudo("sh -c 'if [ -d \"/mnt\" ]; then umount /mnt || true; rm -rf /mnt; fi'")
                # mount the instance store device on the correct /vol disk
                if _fab_exists('/vol%d' % v) is False:
                    sudo('mkfs -t ext4 /dev/%s || true' % xdevs[v])
                    sudo('mkdir /vol%d' % v)
                    sudo('echo "/dev/%s /vol%d ext4 defaults 0 2" >> /etc/fstab' % (xdevs[v], v))
                    sudo('mount /vol%d' % v)
                # grant ownership to our ssh user
                sudo('chown -R %s: /vol%d' % (ssh_user, v))

def _integ_host_with_meta(cluster, host, metaHost):
    # setup logging on the Solr server based on whether there is a meta host
    # running rabbitmq and the logstash4solr stuff   
    if metaHost is not None:
        log4jCfg = _gen_log4j_cfg(host, metaHost, 'WARN')
    else:
        log4jCfg = _gen_log4j_cfg()

    cloudDir = _env(cluster, 'sstk_cloud_dir')
    remoteLog4JPropsFile = cloudDir + '/log4j.properties'
    run('rm -f ' + remoteLog4JPropsFile)
    _fab_append(remoteLog4JPropsFile, log4jCfg)

    # if we're running a metanode, startup collectd with the network plugin configured
    if metaHost is not None:
        sudo('rm -f /etc/collectd.conf')
        collectdNetwork = '''FQDNLookup   true
    Interval     10 
    LoadPlugin logfile
    <Plugin logfile>
    LogLevel info
    File "/var/log/collectd.log"
    Timestamp true
    PrintSeverity false
    </Plugin>
    LoadPlugin cpu
    LoadPlugin disk
    LoadPlugin interface
    LoadPlugin load
    LoadPlugin memory
    LoadPlugin network
    <Plugin network>
    <Server "'''+metaHost+'''" "25826">
    Interface "eth0"
    </Server>
    </Plugin>
    Hostname "'''+host+'''"
    '''
        collectdConf = '/etc/collectd.conf'
        _fab_append(collectdConf, collectdNetwork, use_sudo=True)
        sudo('service collectd restart')

def _lookup_overseer(cluster):
    """
    Retrieve overseer leader node for the specified cluster.

    Arg Usage:
      cluster: Identifies the SolrCloud cluster you want to get status for.
    """

    hosts = _lookup_hosts(cluster, False)
    statusAction = 'http://%s:8984/solr/admin/collections?action=OVERSEERSTATUS&wt=json' % hosts[0]
    try:
        response = urllib2.urlopen(statusAction)
        solr_resp = response.read()
        overseerStatus = json.loads(solr_resp)
        leader = overseerStatus['leader']
        return leader
    except urllib2.HTTPError as e:
        _error('Overseer status retrieval failed due to: %s' % str(e) + '\n' + e.read())

def _add_overseer_role(hosts, nodeName):
    _info('Adding overseer role to node: %s' % nodeName)
    roleAction = 'http://%s:8984/solr/admin/collections?action=addrole&role=overseer&node=%s&wt=json' % (
        hosts[0], nodeName)
    _info('Requesting: %s' % roleAction)
    try:
        response = urllib2.urlopen(roleAction)
        response_s = response.read()
        solr_resp = json.loads(response_s)
        respHeader = solr_resp['responseHeader']
        if respHeader['status'] != 0:
            _error('Unable to add role. \n%s' % response_s)
        else:
            _info('Successfully added overseer role to node %s' % nodeName)
    except urllib2.HTTPError as e:
        _error('Unable to add overseer role to node due to: %s' % str(e) + '\n' + e.read())

def _assign_overseer_nodes(hosts, numNodesPerHost, num_overseer_nodes, totalNodes):
    overseerNodes = set({})
    if int(num_overseer_nodes) > 0:
        # let's designate some random nodes as overseers
        for x in range(0, int(num_overseer_nodes)):
            overseer = random.randrange(0, totalNodes)
            # print('overseer = %d , hosts = %d , numNodesPerHost = %d' % (overseer, n, numNodesPerHost))
            oHost = overseer / numNodesPerHost
            oPort = overseer % numNodesPerHost
            # _info('Using host: %d and port: 89%d as overseer' % (oHost, 84 + oPort))
            overseerNodes.add('%s:89%d_solr' % (hosts[oHost], 84 + oPort))
    return overseerNodes


# -----------------------------------------------------------------------------
# Fabric actions begin here ... anything above this are private helper methods.
# -----------------------------------------------------------------------------
def new_ec2_instances(cluster,
                      n=1,
                      maxWait=180,
                      instance_type=None,
                      ami=None,
                      key=None,
                      az=None,
                      placement_group=None,
                      skipStores=None):

    """
    Launches one or more instances in EC2; each instance is tagged with a cluster id and username.
    SSH connectivity to each instance is verified before this command returns with a maximum wait
    of 3 minutes.

    Example:
      Provision 4 instances tagged with the cluster ID "cloud1": fab new_ec2_instances:cloud1,n=4

    Arg Usage:
      cluster: A short but informative identifier for the cluster you are launching.
      n (int, optional): Number of instances to launch; running this command will cost at least
        the per hour instance price * n, so be careful.
      maxWait (int, optional): Maximum number of seconds to wait for the instances to be online;
        default is 180 seconds.
      instance_type (optional): Amazon EC2 instance instance_type, default is m3.medium
      placement_group (optional): Launches one or more instances in an AWS placement group, which
        gives better network performance between instances working in a cluster; you must create
        the placement group from the AWS console before using this option.
      skipStores (optional): Skips creation and mounting of filesystems; useful when you don't
        need access to the additional instance storage devices.
      

    Returns:
      hosts (list): A list of public DNS names for the instances launched by this command.
    """

    if instance_type is None:
        instance_type = _env(cluster, 'AWS_INSTANCE_TYPE')

    if ami is None:
        ami = _env(cluster, 'AWS_PV_AMI_ID')

    if key is None:
        key = _env(cluster, 'AWS_KEY_NAME')

    if az is None:
        az = _env(cluster, 'AWS_AZ')

    hvmAmiId = _env(cluster, 'AWS_HVM_AMI_ID')

    # help the user avoid making mistake by trying to use a pv ami when hvm is required
    if (instance_type.startswith('i2.') or instance_type.startswith('r3.')) and ami != hvmAmiId:
        ami = hvmAmiId
        _warn('Must use ' + ami + ' for '+instance_type+' instances!')
    
    if instance_type.find('m1.') != -1 and instance_type != 'm1.small':
        _fatal('''Please use the m3.* instance types instead of m1! 
        The m3 instance types are usually better and less expensive
        as Amazon is phasing out the m1 types.''')    

    ec2 = _provider_api()
    num = int(n)

    _status('Going to launch %d new EC2 %s instances using AMI %s' % (num, instance_type, ami))

    # verify no cluster with the same ID already exists with running instances
    existing = _find_instances_in_cluster(ec2, cluster, True)
    if len(existing) > 0:
        if confirm('Found %d running instances for cluster %s, do you want re-use this cluster?' % (len(existing), cluster)):
            hosts = _cluster_hosts(ec2, cluster)
            ec2.close()
            return hosts
        else:
            _fatal('''Found %d running instances for cluster %s.
            Please use a unique cluster ID or terminate all running instances of the existing cluster.
            This is a safety mechanism to prevent you from affecting other clusters.''' % (len(existing), cluster))

    # device mappings
    setupInstanceStores = True if skipStores is None else False
    numInstanceStores = instanceStoresByType[instance_type]
    if numInstanceStores is None:
        _fatal('Must specify the number of instance stores for instance instance_type: ' % instance_type)
    
    numStores = int(numInstanceStores)
    if numStores > 4:
        _fatal('Too many instance stores requested! Please specify an int between 0-4.')
        
    # warn the user that they are configuring instances without any additional instance storage configured
    # TODO: need to build up a dict containing instance type metadata
    if numStores == 0:
        if confirm(instance_type+' instances support multiple instance stores, are you sure you want to launch these instances without using these disks?') is False:
            _fatal('numInstanceStores=0 setting not confirmed!')

    # TODO: this is hacktastic! Need a mapping from /dev/sd# to /dev/xvd#
    devs = ['sdb','sdc','sdd','sde']
    xdevs = ['xvdf','xvdg','xvdh','xvdi']
    
    if ami == hvmAmiId:
        xdevs = ['xvdb','xvdc','xvdd','xvde']

    bdm = None   
    if setupInstanceStores is True and numStores > 0:
        bdm = boto.ec2.blockdevicemapping.BlockDeviceMapping()
        for s in range(0,numStores):
            ephName = 'ephemeral%d' % s
            devName = '/dev/%s' % devs[s]
            dev_sdb = boto.ec2.blockdevicemapping.BlockDeviceType(ephemeral_name=ephName)
            bdm[devName] = dev_sdb
            _info('Setup Instance store BlockDeviceMapping: %s -> %s' % (devName, ephName))

    awsSecGroup = _env(cluster, 'AWS_SECURITY_GROUP')
    # launch the instances in EC2
    rsrv = ec2.run_instances(ami,
                             min_count=num,
                             max_count=num,
                             instance_type=instance_type,
                             key_name=key,
                             security_groups=[awsSecGroup],
                             block_device_map=bdm,
                             monitoring_enabled=True,
                             placement=az,
                             placement_group=placement_group)

    time.sleep(4) # sometimes the AWS API is a little sluggish in making these instances available to this API
    
    # add a tag to each instance so that we can filter many instances by our cluster tag
    username = getpass.getuser()

    for inst in rsrv.instances:
        inst.add_tag(CLUSTER_TAG, cluster)
        inst.add_tag(USERNAME_TAG, username)
        inst.add_tag(INSTANCE_STORES_TAG, numStores)

    numRunning = _poll_for_running_status(rsrv, maxWait=int(maxWait))
    if int(numRunning) != int(num):
        _fatal('Only %s of %s instances are running for cluster %s! Check the AWS console to diagnose the issue.' % (str(numRunning), str(num), cluster))

    time.sleep(5)

    # Sets the env.hosts param to contain the hosts we just launched; helps when chaining Fabric commands
    hosts = _cluster_hosts(ec2, cluster)
    ec2.close()

    # don't return from this operation until we can SSH into each node
    # and some hvm (esp the larger instance types) can take longer to provision
    waitSecs = 360 if ami == hvmAmiId else 180
    if _verify_ssh_connectivity(hosts, waitSecs) is False:
        _fatal('Failed to verify SSH connectivity to all hosts!')

    # mount the instance stores on /vol#
    if setupInstanceStores:
        _status('Making instance store file systems ... please be patient')
        _setup_instance_stores(hosts, numStores, ami, xdevs)
    else:
        _warn('Skipping instance store configuration!')

    sstk_cfg = _get_config()
    if sstk_cfg.has_key('clusters') is False:
        sstk_cfg['clusters'] = {}

    sstk_cfg['clusters'][cluster] = { 'provider':'ec2', 'name':cluster, 'hosts':hosts, 'username':username }
    _save_config()

    return hosts

def setup_solrcloud(cluster, zk=None, zkn=1, nodesPerHost=1, meta=None):
    """
    Configures and starts a SolrCloud cluster on machines identified by the cluster parameter.
    SolrCloud is configured to connect to an existing ZooKeeper ensemble or boostrap a single
    ZooKeeper server on the first host.

    If you don't already have machines provisioned, then you should use the new_solrcloud
    command, which will provision the machines for you. This command is intended to be used
    with existing EC2 instances.

    Args Usage:
      cluster: Identifies the EC2 instances to deploy SolrCloud on.
      zk (optional): Identifies the ZooKeeper ensemble to connect SolrCloud to; if not
        provided, this SolrCloud will start ZooKeeper on the first host of the cluster.
      nodesPerHost (int, optional): Number of Solr nodes to start per host; each Solr will
        have a unique port number, starting with 8984.

    Returns:
      hosts - A list of hosts for the SolrCloud cluster.
    """

    hosts = _lookup_hosts(cluster, True)
    numNodesPerHost = int(nodesPerHost)
    totalNodes = numNodesPerHost * len(hosts)

    _info('Setting up %d SolrCloud nodes on cluster: %s' % (totalNodes, cluster))

    cloud = _provider_api()

    # setup/start zookeeper
    zkHosts = []
    if zk is None:
        # just run 1 node on the first host
        numZkNodes = int(zkn) if zkn is not None else 1
        if numZkNodes > len(hosts):
            _warn('Number of requested local ZooKeeper nodes %d exceeds number of available hosts! Using %d instead.' % (numZkNodes, len(hosts)))
        zkHosts = _zk_ensemble(cluster, hosts[0:numZkNodes])
    else:
        zkHosts = _get_zk_hosts(cloud, zk)

    zkHost = ','.join(zkHosts)

    # chroot the znodes for this cluster
    zkHost += ('/' + cluster)

    instance_type = _get_instance_type(cloud, cluster)
    solrJavaMemOpts = _get_solr_java_memory_opts(instance_type, numNodesPerHost)

    remoteSolrDir = _env(cluster, 'solr_tip')
    remoteSolrJavaHome = _env(cluster, 'solr_java_home')

    # setup local if needed
    provider = _env(cluster, 'provider')
    if provider == "local":
        cloudDir = os.path.expanduser(_env(cluster, 'sstk_cloud_dir'))
        if os.path.isdir(cloudDir) is False:
            os.makedirs(cloudDir)

        cloud84Dir = os.path.expanduser(remoteSolrDir+'/cloud84')
        if os.path.isdir(cloud84Dir) is False:
            # make the cloud84 dir from example
            exampleDir = os.path.expanduser(remoteSolrDir+'/example')
            if os.path.isdir(exampleDir) is False:
                _fatal(str(exampleDir)+' not found! Cannot create the cloud84 directory.')
            _copy_dir(exampleDir, cloud84Dir)
            os.rename(cloud84Dir+'/solr/collection1', cloud84Dir+'/solr/cloud')
            os.remove(cloud84Dir+'/solr/cloud/core.properties')
            shutil.rmtree(cloud84Dir+'/solr/cloud/data')
            shutil.rmtree(cloud84Dir+'/example-schemaless')
            shutil.rmtree(cloud84Dir+'/exampledocs')
            shutil.rmtree(cloud84Dir+'/example-DIH')
            shutil.rmtree(cloud84Dir+'/multicore')

    # make sure the solr-scale-tk shell scripts are up-to-date on the remote servers
    exportCloudEnv = ('''#!/bin/bash
export SOLR_JAVA_HOME="%s"
export SOLR_TOP="%s"
export ZK_HOST="%s"
export CLOUD_SCRIPTS_DIR="$SOLR_TOP/cloud84/scripts/cloud-scripts"
export SOLRCLOUD_CLUSTER="%s"
export NODES_PER_HOST=%d
export SOLR_JAVA_MEM="%s"
''' % (remoteSolrJavaHome, remoteSolrDir, zkHost, cluster, numNodesPerHost, solrJavaMemOpts))

    # write the include file for the bin/solr script
    solrInSh = _get_solr_in_sh(cluster, remoteSolrJavaHome, solrJavaMemOpts, zkHost)
    solrInShPath = remoteSolrDir+'/bin/solr.in.sh'

    sstkEnvScript = _env(cluster, 'SSTK_ENV')
    sstkScript = _env(cluster, 'SSTK')
    binSolrScript = remoteSolrDir + '/bin/solr'
    cloudDir = _env(cluster, 'sstk_cloud_dir')
    for host in hosts:
        with settings(host_string=host), hide('output', 'running', 'warnings'):
            run('rm -f ' + sstkEnvScript)
            _fab_append(sstkEnvScript, exportCloudEnv)
            run('chmod +x ' + sstkEnvScript)
            put('./'+CTL_SCRIPT, cloudDir)
            run('chmod +x ' + sstkScript)
            run('rm -f '+solrInShPath)
            _fab_append(solrInShPath, solrInSh)

    # upload config to ZK
    with settings(host_string=hosts[0]), hide('output', 'running', 'warnings'):
        run('rm -rf '+cloudDir+'/tmp/*; mkdir -p '+cloudDir+'/tmp/cloud; cp -r '+remoteSolrDir+'/cloud84/solr/cloud/* '+cloudDir+'/tmp/cloud/')
        remoteUpconfigCmd = '%s upconfig cloud' % sstkScript
        run(remoteUpconfigCmd)

    # support option to enable logstash4solr integration and zabbix agent
    metaHost = None
    if meta is not None:        
        metaHost = _lookup_hosts(meta, True)[0]
        _status('Found metaHost: '+metaHost)
    else:
        _status('No meta node specified.')

    # setup N Solr nodes per host using the script to do the actual starting
    numStores = instanceStoresByType[instance_type]
    if numStores is None:
        numStores = 1
        
    solrHostAndPorts = []
    for host in hosts:
        with settings(host_string=host), hide('output', 'running', 'warnings'):
            _integ_host_with_meta(cluster, host, metaHost)

            for p in range(0, numNodesPerHost):
                solrPort = str(84 + p)
                solrHostAndPorts.append(host + ':89' + solrPort)
                volIndex = p
                if volIndex >= numStores:
                    volIndex = volIndex % numStores
                remoteSetupCmd = '%s setup %s %d' % (sstkScript, solrPort, volIndex)
                _status('Running setup on remote: ' + remoteSetupCmd)
                run(remoteSetupCmd)
                time.sleep(2)
                
            for x in range(0, numNodesPerHost):
                solrPortUniq = str(84 + x)
                solrPort = '89' + solrPortUniq
                solrDir =  'cloud'+solrPortUniq
                remoteStartCmd = '%s start -cloud -p %s -d %s' % (binSolrScript, solrPort, solrDir)
                _status('Running start on remote: ' + remoteStartCmd)
                run(remoteSolrDir+'/bin/solr stop -p '+solrPort+' || true')
                run('nohup ' + remoteStartCmd + ' &', pty=False)
                time.sleep(2)

    # wait until the Solr servers report they are up
    _status('Solr instances launched ... waiting up to %d seconds to see %d Solr servers come online.' % (180, totalNodes))
    _wait_to_see_solr_up_on_hosts(solrHostAndPorts,180)

    cloud.close()

    return hosts

def is_solr_up(cluster):
    """
    Quick check to see if Solr is responding to HTTP requests on all nodes in the cluster.
    """
    hosts = _lookup_hosts(cluster)
    numNodes = _num_solr_nodes_per_host(cluster)
    solrHostAndPorts = []
    for h in hosts:
        for n in range(0,numNodes):
            solrHostAndPorts.append(h + ':89' + str(84 + n))
    _wait_to_see_solr_up_on_hosts(solrHostAndPorts, 5)

# create a collection
def new_collection(cluster, name, rf=1, shards=1, conf='cloud'):
    """
    Create a new collection in the specified cluster.

    This command assumes the configuration given by the conf parameter has already been uploaded
    to ZooKeeper.

    Arg Usage:
      cluster: Identifies the SolrCloud cluster you want to create the collection on.
      name: Name of the collection to create.
      rf (int, optional): Replication factor for the collection (number of replicas per shard)
      shards (int, optional): Number of shards to distribute this collection across

    Returns:
      collection stats
    """

    hosts = _lookup_hosts(cluster)
    numNodes = _num_solr_nodes_per_host(cluster)
    nodes = len(hosts) * numNodes
    replicas = int(rf) * int(shards)
    maxShardsPerNode = int(max(1, round(replicas / nodes)))
    _info('%d cores across %d nodes requires maxShardsPerNode=%d' % (replicas, nodes, maxShardsPerNode))

    createAction = ('http://%s:8984/solr/admin/collections?action=CREATE&name=%s&replicationFactor=%s&numShards=%s&collection.configName=%s&maxShardsPerNode=%s' %
         (hosts[0], name, str(rf), str(shards), conf, str(maxShardsPerNode)))

    _info('Create new collection named %s using:\n%s' % (name, createAction))
    try:
        response = urllib2.urlopen(createAction)
        solr_resp = response.read()
        _info('Create collection succeeded\n' + solr_resp)
    except urllib2.HTTPError as e:
        _error('Create new collection named %s failed due to: %s' % (name, str(e)) + '\n' + e.read())

def delete_collection(cluster, name):
    """
    Delete a collection from the specified cluster.

    Arg Usage:
      cluster: Identifies the SolrCloud cluster you want to delete the collection from.
      name: Name of the collection to delete.
    """

    hosts = _lookup_hosts(cluster, False)

    deleteAction = 'http://%s:8984/solr/admin/collections?action=DELETE&name=%s' % (hosts[0], name)

    _info('Delete the collection named %s using:\n%s' % (name, deleteAction))
    try:
        response = urllib2.urlopen(deleteAction)
        solr_resp = response.read()
        _info('Delete collection succeeded\n' + solr_resp)
    except urllib2.HTTPError as e:
        _error('Delete collection named %s failed due to: %s' % (name, str(e)) + '\n' + e.read())

# Uncomment this when CLUSTERSTATUS action is available: Solr 4.8+
#
# def cluster_status(cluster, collection=None, shard=None):
#     """
#     Retrieve status for the specified cluster.
#
#     Arg Usage:
#       cluster: Identifies the SolrCloud cluster you want to get status for.
#       collection (optional): restricts status info to this collection.
#       shard (optional, comma-separated list): restricts status info to this shard/set of shards.
#     """
#
#     hosts = _lookup_hosts(cluster, False)
#
#     params = ''
#     if collection is not None or shard is not None:
#         params = '?'
#         if collection is not None:
#             params += collection
#             if shard is not None: params += '&'
#         if shard is not None: params += shard
#
#     listAction = 'http://%s:8984/solr/admin/collections?action=CLUSTERSTATUS%s' % (hosts[0], params)
#
#     _info('Retrieving cluster status using:\n%s' % listAction)
#     try:
#         response = urllib2.urlopen(listAction)
#         solr_resp = response.read()
#         _info('Cluster status retrieval succeeded\n' + solr_resp)
#     except urllib2.HTTPError as e:
#         _error('Cluster status retrieval failed due to: %s' % str(e) + '\n' + e.read())

def new_zk_ensemble(cluster, n=3, instance_type='m3.medium', az=None, placement_group=None):
    """
    Configures, starts, and checks the health of a ZooKeeper ensemble on one or more nodes in a cluster.

    Arg Usage:
      cluster (str): Cluster ID used to identify the ZooKeeper ensemble created by this command.
      n (int, optional): Size of the cluster.
      instance_type (str, optional):

    Returns:
      zkHosts: List of ZooKeeper hosts for the ensemble.
    """

    paramReport = '''
    *****
    Launching new ZooKeeper ensemble with the following parameters:
        cluster: %s
        numInstances: %d
        instance_type: %s
    *****
    ''' % (cluster, int(n), instance_type)
    _info(paramReport)

    hosts = new_ec2_instances(cluster=cluster, n=n, instance_type=instance_type, az=az, placement_group=placement_group)

    _info('\n\n*** %d EC2 instances have been provisioned ***\n\n' % len(hosts))

    zkHosts = _zk_ensemble(cluster, hosts)
    _info('Successfully launched new ZooKeeper ensemble')

    return zkHosts

def setup_zk_ensemble(cluster):
    """
    Configures, starts, and checks the health of a ZooKeeper ensemble in an existing cluster.
    """
    cloud = _provider_api()
    hosts = _cluster_hosts(cloud, cluster)
    _verify_ssh_connectivity(hosts)    
    zkHosts = _zk_ensemble(cluster, hosts)
    _info('Successfully launched new ZooKeeper ensemble')
    return zkHosts

def kill(cluster):
    """
    Terminate all running nodes of the specified cluster.
    """
    cloud = _provider_api()
    taggedInstances = _find_instances_in_cluster(cloud, cluster)
    instance_ids = taggedInstances.keys()
    if confirm('Found %d instances to terminate, continue? ' % len(instance_ids)):
        cloud.terminate_instances(instance_ids)
    cloud.close()

    # update the local config to remove this cluster
    sstk_cfg = _get_config()
    sstk_cfg['clusters'].pop(cluster, None)
    _save_config()

# pretty much just chains a bunch of commands together to create a new solr cloud cluster ondemand
def new_solrcloud(cluster, n=1, zk=None, zkn=1, nodesPerHost=1, meta=None, instance_type=None, ami=None, az=None, placement_group=None):
    """
    Provisions n EC2 instances and then deploys SolrCloud; uses the new_ec2_instances and setup_solrcloud
    commands internally to execute this command.
    """

    if zk is None:
        zkHost = '*local*'
    else:
        cloud = _provider_api()
        zkHosts = _get_zk_hosts(cloud, zk)
        zkHost = zk + ': ' + (','.join(zkHosts))
        cloud.close()

    if az is None:
        az = _env(cluster, 'AWS_AZ')

    paramReport = '''
    *****
    Launching new SolrCloud cluster with the following parameters:
        cluster: %s
        zkHost: %s
        instance_type: %s
        numInstances: %d
        nodesPerHost: %d
        ami: %s
        az: %s
        placement_group: %s
        meta: %s
    *****
    ''' % (cluster, zkHost, instance_type, int(n), int(nodesPerHost), ami, az, placement_group, meta)
    _info(paramReport)
    
    if confirm('Verify the parameters. OK to proceed?') is False:
        return

    ec2hosts = new_ec2_instances(cluster=cluster, n=n, instance_type=instance_type, ami=ami, az=az, placement_group=placement_group)

    _info('\n\n*** %d EC2 instances have been provisioned ***\n\n' % len(ec2hosts))

    hosts = setup_solrcloud(cluster=cluster, zk=zk, zkn=zkn, meta=meta, nodesPerHost=nodesPerHost)
    solrUrl = 'http://%s:8984/solr/#/' % str(hosts[0])
    _info('Successfully launched new SolrCloud cluster ' + cluster + '; visit: ' + solrUrl)

def stop_solrcloud(cluster):
    """
    Sends the kill command to all Solr nodes across the cluster.
    """
    hosts = _lookup_hosts(cluster)
    numNodes = _num_solr_nodes_per_host(cluster)
    binSolrScript = _env(cluster, 'solr_tip') + '/bin/solr'
    for h in hosts:
        with settings(host_string=h), hide('output', 'running', 'warnings'):                
            for n in range(0,numNodes):
                solrPort = '89' + str(84+n)
                _status('Stopping Solr node on '+h+':'+solrPort+' using command: bin/solr stop -p '+solrPort)
                run(binSolrScript + ' stop -p '+solrPort)
            

def ssh_to(cluster,n=0):
    """
    Open an interactive SSH shell with the first host in a cluster.
    """
    idx = int(n)
    hosts = _lookup_hosts(cluster, False)
    if idx < 0 or idx >= len(hosts):
        _fatal('Expected 0-based host index between 0 >= x < %d' % len(hosts))

    sshKeyFile = _env(cluster,'ssh_keyfile_path_on_local')
    sshUser = _env(cluster, 'ssh_user')
    local('ssh -o StrictHostKeyChecking=no -i %s %s@%s' % (sshKeyFile, sshUser, hosts[idx]))

def verify_ssh(cluster):
    """
    Verifies SSH connectivity on all hosts in a cluster.
    """
    _lookup_hosts(cluster, True)

def check_zk(cluster, n=None):
    """
    Performs health check against all servers in a ZooKeeper ensemble.
    """
    hosts = _lookup_hosts(cluster, False)
    num = len(hosts) if n is None else int(n)
    _status('Checking ZooKeeper health on hosts: %s' % str(', '.join(hosts)))
    _check_zk_health(num, hosts, 1)  # 1 means no wait, just check and exit

def mine(user=None):
    """
    Shows all running instances for the current user, organized by cluster name.
    """
    isMe = False
    if user is None:
        user = getpass.getuser()
        isMe = True

    cloud = _provider_api()
    if user == 'all':
        _status('Finding all running instances')
        instances = _find_all_instances(cloud)
    else:
        _status('Finding instances tagged with username: ' + user)
        instances = _find_user_instances(cloud, user, False)
    cloud.close()

    clusterList = []
    byUser = {}
    for key in instances.keys():
        inst = instances[key]
        if inst.state != 'running':
            continue
        
        usertag = inst.__dict__['tags']['username']
        if usertag is None:
            usertag = '?user?'

        if byUser.has_key(usertag) is False:
            byUser[usertag] = {}
        
        clusters = byUser[usertag]          
        
        cluster = inst.__dict__['tags']['cluster']
        if cluster is None:
            cluster = '?CLUSTER?'

        if clusters.has_key(cluster) is False:
            clusters[cluster] = []

        upTime = _uptime(inst.launch_time)
        clusters[cluster].append('%s: %s (%s %s%s)' % 
          (inst.public_dns_name, key, inst.instance_type, inst.state, upTime))

        clusterList.append(cluster)

    # to be consistent, if the user has defined any local clusters, include in the output
    if isMe:
        sstk_cfg = _get_config()
        if sstk_cfg.has_key('clusters'):
            if byUser.has_key(user) is False:
                byUser[user] = {}
            clusters = sstk_cfg['clusters']
            for clusterId in clusters.keys():
                cluster = clusters[clusterId]
                if cluster.has_key('provider') and cluster['provider'] == 'local':
                    byUser[user][clusterId] = ['localhost: '+cluster['solr_tip']]
                    clusterList.append(clusterId)

    for u in byUser.keys():
        clusters = byUser[u]    
        for c in clusters.keys():
            clusters[c].sort() # sort so that the ssh_to indexes line up
        print('\n*** user: '+u+' ***')    
        print(json.dumps(byUser[u], indent=2))

    # make sure the local config knows about all my clusters
    if isMe:
        sstk_cfg = _get_config()
        for clusterId in clusterList:
            hosts = _lookup_hosts(clusterId)
            if sstk_cfg.has_key('clusters') is False:
                sstk_cfg['clusters'] = {}
            if sstk_cfg['clusters'].has_key(clusterId) is False:
                sstk_cfg['clusters'][clusterId] = {}
            else:
                if sstk_cfg['clusters'][clusterId].has_key('provider') and sstk_cfg['clusters'][clusterId]['provider'] == 'local':
                    continue
            sstk_cfg['clusters'][clusterId]['hosts'] = hosts
            sstk_cfg['clusters'][clusterId]['provider'] = _config['provider']
            sstk_cfg['clusters'][clusterId]['name'] = clusterId
            sstk_cfg['clusters'][clusterId]['username'] = user
        _save_config()

    return # end of mine

def kill_mine():
    """
    Helper command to terminate all running instances for the current user.
    """
    cloud = _provider_api()
    username = getpass.getuser()
    my_instances = _find_user_instances(cloud, username)
    instance_ids = my_instances.keys()
    if len(instance_ids) > 0:
        if confirm('Found %d instances to terminate, continue? ' % len(instance_ids)):
            cloud.terminate_instances(instance_ids)
            # update the local config to remove cluster's we're killing
            sstk_cfg = _get_config()
            for iid in instance_ids:
                if my_instances[iid].tags.has_key('cluster'):
                    cluster = my_instances[iid].tags['cluster']
                    sstk_cfg['clusters'].pop(cluster, None)
            _save_config()


    else:
        _info('No running instances found for user: ' + username)

    cloud.close()

def setup_meta_node(cluster):
    """
    Setup a meta node with supporting services, such as: Logstash4Solr, Rabbitmq, Zabbix
    TODO: support kafka instead of rabbitmq
    """
    hosts = _lookup_hosts(cluster, False)
    metaHost = hosts[0]
    _status('Setting up the meta node on ' + metaHost)
    with settings(host_string=metaHost), hide('output', 'running', 'warnings'):
        # start rabbitmq service on meta node
        sudo('service rabbitmq-server restart')

        # create the exchanges / queues
        run('rabbitmqadmin -V / declare exchange name=log4j-exchange type=direct durable=false')
        run('rabbitmqadmin -V / declare queue name=log4j-queue durable=false')
        
        _status('Started RabbitMQ service ...')

        # configure banana
        _status('Configuring logstash4solr and banana dashboard ...')
        _configure_banana(metaHost)
        
        # setup the data directory on /vol0 for the logstash_logs core
        # /home/ec2-user/slk-4.7.0/solr-4.7.0/SiLK/solr/logstash_logs
        run('rm -rf /vol0/logstash_logs_data; mkdir -p /vol0/logstash_logs_data')
        coreProps = '''config=solrconfig.xml
name=logstash_logs
schema=schema.xml
shard=
dataDir=/vol0/logstash_logs_data
collection=        
'''
        run('rm -f '+user_home+'/slk-4.7.0/solr-4.7.0/SiLK/solr/logstash_logs/core.properties')
        _fab_append(user_home+'/slk-4.7.0/solr-4.7.0/SiLK/solr/logstash_logs/core.properties', coreProps)        

        # start solr for logstash
        startLogstashSolrServer = user_home + '/slk-4.7.0/solr-4.7.0/SiLK/silk-ctl.sh start'
        
        run('nohup ' + startLogstashSolrServer + ' >& /dev/null < /dev/null &', pty=False)
        _status('Waiting to see logstash4solr server come online ...')
        _wait_to_see_solr_up_on_hosts([metaHost + ':8983'])

        _status('Started Solr for logstash4solr')

        # start logstash4solr on meta node
        logstash4solr = user_home + '/slk-4.7.0/solrWriterForLogStash/logstash_deploy/logstash4solr-ctl.sh'
        run('nohup ' + logstash4solr + ' >& /dev/null < /dev/null &', pty=False)        
        _status('Started logstash4solr process')
        
        # do this again to reset the dashboard correctly
        _configure_banana(metaHost)        

    print('RabbitMQ Admin UI @ ' + _blue('http://%s:15672/' % metaHost))
    print('Logstash4Solr Solr Console @ ' + _blue('http://%s:8983/solr/#/' % metaHost))
    print('Banana UI @ ' + _blue('http://%s:8983/banana' % metaHost))

def new_meta_node(meta, instance_type='m3.large', ami=None, az=None, placement_group=None):
    """
    Launches a new meta node on an m3.large instance.
    """
    ec2hosts = new_ec2_instances(cluster=meta, n=1, instance_type=instance_type, ami=ami, az=az, placement_group=placement_group)
    _info('\n\n*** %d EC2 instances have been provisioned ***\n\n' % len(ec2hosts))
    setup_meta_node(meta)

def demo(demoCluster, n=3, instance_type='m3.medium'):
    """
    Demo of all this script's capabilities in one command.
    The result is a SolrCloud cluster with all the fixin's ...
    """
    ec2hosts = new_ec2_instances(cluster=demoCluster, n=n, instance_type=instance_type)
    numHosts = len(ec2hosts)
    _info('\n\n*** %d EC2 instances have been provisioned ***\n\n' % numHosts)

    # print them out to the console if it is a small number
    if numHosts < 10:
        for h in ec2hosts:
            _info(h)
    print('\n')
    setup_demo(demoCluster)
    
def setup_demo(cluster):
    hosts = _lookup_hosts(cluster)
    numHosts = len(hosts)
    setup_meta_node(cluster)
    numZkHosts = 3 if numHosts >= 3 else 1
    zkHosts = _zk_ensemble(cluster, hosts[0:numZkHosts])
    _info('Successfully launched new ZooKeeper ensemble: ' + str(zkHosts))
    _info('\nzkHost=%s/%s\n' % (str(','.join(zkHosts)), cluster))
    hosts = setup_solrcloud(cluster=cluster, zk=cluster, meta=cluster, nodesPerHost=2)
    solrUrl = 'http://%s:8984/solr/#/' % str(hosts[0])
    _info('Successfully launched new SolrCloud cluster ' + cluster + '; visit: ' + solrUrl)
    new_collection(cluster=cluster, name='demo', shards=numHosts, rf=2)
    _status('Created new collection ... indexing some synthetic documents ...')
    zkHost = ','.join(zkHosts) + '/' + cluster
    local('./tools.sh indexer -collection=demo -zkHost=%s -numDocsToIndex=%d' % (zkHost, 20000))

def commit(cluster, collection):
    """
    Sends a hard commit to the specified collection in the specified cluster.
    """
    hosts = _lookup_hosts(cluster, False)
    solr = pysolr.Solr('http://%s:8984/solr/%s' % (hosts[0], collection), timeout=10)
    solr.commit()

def patch_jars(cluster, localSolrDir, n=None, jars='core solrj', vers='4.7.1'):
    """
    Replaces Solr JAR files on remote servers with new ones built locally.
    This command helps you patch a running system with a quick fix w/o having
    to rebuild the AMI.
    """
    localSolrDir = os.path.expanduser(localSolrDir)
    if os.path.isdir(localSolrDir) is False:
        _fatal('Local Solr directory %s not found!' % localSolrDir)

    # on first server, rm -rf cloud/tmp/jars/*; mkdir -p cloud/tmp/jars
    # upload jars to first server into cloud/tmp/jars
    # upload the ssh key to .ssh
    # scp jars from first server to others via fab run

    jarList = jars.split()
    filesToPatch = []
    for jar in jarList:
        jarFile = '%s/build/solr-%s/solr-%s-%s.jar' % (localSolrDir, jar, jar, vers)
        if os.path.isfile(jarFile):
            filesToPatch.append(jarFile)
        else:
            _fatal('JAR %s not found on LOCAL FS!' % jarFile)
    
    # get list of hosts and verify SSH connectivity
    cloud = _provider_api()    
    hosts = _cluster_hosts(cloud, cluster)
    
    # ability to patch a single server only
    if n is not None:
        hosts = [hosts[int(n)]]

    _verify_ssh_connectivity(hosts)

    remoteSolrDir = _env(cluster, 'solr_tip')

    # get num Solr nodes per host to determine which ports are active
    numNodes = _num_solr_nodes_per_host(cluster)
    activePorts = []
    for n in range(0,numNodes):
        activePorts.append(str(84 + n))
    solrHostsAndPortsToRestart = {}            
    with settings(host_string=hosts[0]), hide('output', 'running', 'warnings'):
        host = hosts[0]
        solrHostsAndPortsToRestart[host] = set([]) # set is important
        remoteJarDir = '%s/cloud/tmp/jars' % user_home
        run('mkdir -p %s/.ssh' % user_home)
        run('rm -rf %s; mkdir -p %s' % (remoteJarDir,remoteJarDir))
        put(_env(cluster,'ssh_keyfile_path_on_local'), '%s/.ssh' % user_home)
        run('chmod 600 '+_env(cluster,'ssh_keyfile_path_on_local'))
        for jarFile in filesToPatch:
            lastSlashAt = jarFile.rfind('/')    
            remoteJarFile = '%s/%s' % (remoteJarDir, jarFile[lastSlashAt+1:])
            _status('Uploading to %s on %s ... please be patient (the other hosts will go faster)' % (remoteJarFile, host))            
            put(jarFile, remoteJarDir)        
            run('cp %s %s/dist' % (remoteJarFile, remoteSolrDir))
            for port in activePorts:
                solrHostsAndPortsToRestart[host].add(port)
                run('cp %s %s/cloud%s/solr-webapp/webapp/WEB-INF/lib' % (remoteJarFile, remoteSolrDir, port))
                    
            # scp from the first host to the rest
            if len(hosts) > 1:
                for h in range(1,len(hosts)):
                    host = hosts[h]
                    solrHostsAndPortsToRestart[host] = set([]) # set is important
                    run('scp -o StrictHostKeyChecking=no -i %s %s %s@%s:%s' % (_env(cluster,'ssh_keyfile_path_on_local'), remoteJarFile, ssh_user, host, remoteSolrDir+'/dist'))
                    for port in activePorts:
                        run('scp -o StrictHostKeyChecking=no -i %s %s %s@%s:%s/cloud%s/solr-webapp/webapp/WEB-INF/lib' % (_env(cluster,'ssh_keyfile_path_on_local'), remoteJarFile, ssh_user, host, remoteSolrDir, port))
                        solrHostsAndPortsToRestart[host].add(port)
    _info('JARs uploaded and patched successfully.')
    
    _rolling_restart_solr(cloud, cluster, solrHostsAndPortsToRestart, 0)
        

def deploy_config(cluster,localConfigDir,configName):
    """
    Upload and deploy a local configuration directory to ZooKeeper; after creating the
    config in ZK, you can create collections that reference this config using conf=configName
    """
    
    if os.path.isdir(localConfigDir) is False:
        _fatal('Local config directory %s not found!' % localConfigDir)
        
    parts = localConfigDir.split('/')
    dirName = parts[len(parts)-1]
    hosts = _lookup_hosts(cluster, False)
    sstkScript = _env(cluster, 'SSTK')
    with settings(host_string=hosts[0]):
        remoteDir = user_home+'/cloud/tmp/'+configName
        run('rm -rf '+remoteDir)
        run('mkdir -p '+remoteDir)
        put(localConfigDir, remoteDir)
        run('mv cloud/tmp/%s/%s cloud/tmp/%s/conf || true' % (configName, dirName, configName))
        run(sstkScript + ' upconfig ' + configName)

def backup_to_s3(cluster,collection,bucket='solr-scale-tk',dry_run=0,ebs=None):
    """
    Backup an existing collection to S3 using the replication handler's snapshot support.
    Take a snapshot of each shard leader's index, tar up the files, and ship to S3.
    
    Result is a directory under the s3://solr-scale-tk/ bucket containing a tar file per shard
    e.g. for a collection named 'cloud1' with 4 shards running in a cluster named 'foo', 
    you will have:
    
    s3://bucket/foo_cloud1/
      -> shard1/snapshot
      -> shard2/snapshot
      -> shard3/snapshot
      -> shard4/snapshot
    
    """
    # Verify the S3 bucket is usable
    pfx = cluster+'_'+collection+'/'
    if ebs is None:
        _status('Setting up S3 bucket: %s/%s' % (bucket,pfx))
        s3conn = boto.connect_s3()
        rootBucket = s3conn.get_bucket(bucket)    
        for key in rootBucket.list():
            if key.name.startswith(pfx):
                key.delete()
    
    dryRun = str(dry_run) == '1'
    if dryRun:
        _warn('Doing a dry-run only.')

    cloud = _provider_api()    
    backupDirOnRemoteHost = '../backups/'+collection
    hosts = _cluster_hosts(cloud, cluster)
    numNodes = _num_solr_nodes_per_host(cluster)

    remoteSolrDir = _env(cluster, 'solr_tip')

    for host in hosts:
        with settings(host_string=host), hide('output', 'running', 'warnings'):
            for n in range(0,numNodes):                
                solrPort = str(84+n)
                backupDir = '%s/cloud%s/solr/backups/%s' % (remoteSolrDir, solrPort, collection)
                cmd = 'rm -rf %s; mkdir -p %s' % (backupDir, backupDir)
                if dryRun is False:
                    run(cmd)
                else:
                    _info('run( '+cmd+' )')
            
    # start the backup
    zkHost = _read_cloud_env(cluster)['ZK_HOST'] # get the zkHost from the env on the server
    cmd = './tools.sh backup -backupDir %s -collection %s -zkHost %s' % (backupDirOnRemoteHost, collection, zkHost)    
    if dryRun is False:
        local(cmd)
        pass
    else:
        _info('local( '+cmd+' )')
    
    if ebs is None:
        # S3 approach
        _status('Preparing to backup to S3 ...')
        for h in range(0,len(hosts)):
            host = hosts[h]
            with settings(host_string=host), hide('output', 'running', 'warnings'):  
                for n in range(0,numNodes):                
                    solrPort = str(84+n)
                    backupDir = '%s/cloud%s/solr/backups/%s' % (remoteSolrDir, solrPort, collection)
                    tars2s3 = '#!/bin/bash\n'
                    tars2s3 += 'cd '+backupDir+';'
                    tars2s3 += 'find . -name "shard*" -type d -exec s3cmd --progress --recursive put {} s3://solr-scale-tk/%s \;\n' % pfx
                    run('rm -f '+backupDir+'/s3put.sh')
                    _fab_append(backupDir+'/s3put.sh', tars2s3)
                    if dryRun is False:
                        if ebs is not None:
                            put(_env(cluster,'ssh_keyfile_path_on_local'), '%s/.ssh' % user_home)
                            run('chmod 600 '+_env(cluster,'ssh_keyfile_path_on_local'))                        
                        run('nohup sh '+backupDir+'/s3put.sh > '+backupDir+'/s3put.out 2>&1 &', pty=False)
                    else:                    
                        _info('run( '+tars2s3+' )')
    else:
        # EBS doesn't like a bunch of scp's running concurrently?
        _status('Preparing to backup to EBS volume '+ebs+'/'+pfx+' on '+hosts[0]+' ...')
        scriptName = 'scp_to_ebs'
        for h in range(0,len(hosts)):
            host = hosts[h]
            with settings(host_string=host), hide('output', 'running', 'warnings'):
                put(_env(cluster,'ssh_keyfile_path_on_local'), '%s/.ssh' % user_home)
                run('chmod 600 '+_env(cluster,'ssh_keyfile_path_on_local'))                        

                if h == 0 and ebs is not None:
                    sudo('mkdir -p '+ebs+'/'+pfx+' && chown -R '+ssh_user+': '+ebs+'/'+pfx)
                    
                scpToEbsSh = '#!/bin/bash\n'
                
                for n in range(0,numNodes):                
                    solrPort = str(84+n)
                    backupDir = '%s/cloud%s/solr/backups/%s' % (remoteSolrDir, solrPort, collection)
                    scpToEbsSh += 'cd '+backupDir+';'
                    scpToEbsSh += 'find . -name "shard*" -type d -exec scp -o StrictHostKeyChecking=no -r -i %s {} ec2-user@%s:%s/%s \;\n' % (_env(cluster,'ssh_keyfile_path_on_local'), hosts[0], ebs, pfx)
                    
                backupScript = backupDir+'/'+scriptName+'.sh'
                run('rm -f '+backupScript)                
                _fab_append(backupScript, scpToEbsSh)
                _status('Running backup script on '+host+' ... be patient, this can take a while depending on your index size ...')
                if dryRun is False:
                    run('sh '+backupDir+'/'+scriptName+'.sh')
                    _info('Backup script finished on '+host)
                else:                    
                    _info('run( '+scpToEbsSh+' )')
        
        
    done = 0
    while done < len(hosts):
        done = 0 # reset the counter since we check all nodes each loop
        if dryRun is False:
            _status('sleeping for 30 seconds before checking status')
            time.sleep(30)
        for host in hosts:
            with settings(host_string=host), hide('output', 'running', 'warnings'):
                pid = run('ps waux | grep s3put | grep -v grep | grep '+collection+' | awk \'{print $2}\' | sort -r')
                if len(pid.strip()) == 0:
                    done += 1
                    _status('upload to S3 done on '+host)
                else:
                    _status('upload to S3 still running on '+host+', pid(s): '+pid)
    _info('upload to S3 seems to be done on '+str(len(hosts))+' hosts')
    
    # clean-up backup files to free disk
    for host in hosts:
        with settings(host_string=host):
            for n in range(0,numNodes):                
                solrPort = str(84+n)
                backupDir = '%s/cloud%s/solr/backups/%s' % (remoteSolrDir, solrPort, collection)
                cmd = 'rm -rf %s' % backupDir
                if dryRun is False:
                    run(cmd)
                else:
                    _info('run( '+cmd+' )')
    
    

def restore_backup(cluster,backup_name,collection,bucket='solr-scale-tk',alreadyDownloaded=0,ebsVol=None,ebsMount='/ebs0'):
    """
    Restores an index from backup into an existing collection with the same number of shards
    """
    cloud = _provider_api()    
    hosts = _cluster_hosts(cloud, cluster)
    
    needsDownload = True if int(alreadyDownloaded) == 0 else False
    
    ebsHost = None
    backupOnEbs = None
    useEbsVol = False
    
    # EBS volume may be mounted on this host already
    if ebsVol is None and ebsMount is not None:
        # check if ebsMount exists
        with settings(host_string=hosts[0]):
            backupOnEbs = ebsMount+'/'+backup_name
            if _fab_exists(backupOnEbs):
                ebsHost = hosts[0]
                useEbsVol = True
                needsDownload = False
                _info('Restoring from EBS backup %s mounted on %s' % (backupOnEbs, ebsHost))
    
    # may need to mount the EBS volume
    if ebsVol is not None and useEbsVol is False:
        # find the instance ID of the first host
        instId = None
        for rsrv in cloud.get_all_instances(filters={'tag:' + CLUSTER_TAG:cluster}):
            for inst in rsrv.instances:
                if inst.public_dns_name == hosts[0]:
                    instId = inst.id
        
        if instId is None:
            _fatal('Could not determine the instance ID for '+hosts[0])
        
        with settings(host_string=hosts[0]):
            cloud.attach_volume(ebsVol, instId, '/dev/sdf')
            time.sleep(10)
            sudo('lsblk')
            sudo('mkdir -p ' + ebsMount)
            sudo('mount /dev/xvdf ' + ebsMount)
            sudo('chown -R %s: %s' % (ssh_user,ebsMount))
            run('df -h')            
            backupOnEbs = ebsMount+'/'+backup_name
            if _fab_exists(backupOnEbs):
                ebsHost = hosts[0]
                needsDownload = False
                useEbsVol = True
                _info('Restoring from EBS backup %s mounted on %s' % (backupOnEbs, ebsHost))
            else:
                _fatal('EBS backup '+backupOnEbs+' not found on '+hosts[0])
    
    
    # download from s3 to one of the nodes
    # use the meta file to determine shard locations
    backup = None
    if useEbsVol is False and bucket is not None:
        s3conn = boto.connect_s3()
        rootBucket = s3conn.get_bucket(bucket)
        backup = '%s/%s' % (bucket, backup_name)
        pfx = backup_name+'/'
        _status('Validating backup at s3://%s' % backup)
        foundIt = False
        for key in rootBucket.list():
            if key.name.startswith(pfx):
                foundIt = True
                break
        if foundIt is False:
            _fatal(backup+' not found in S3!')
            
    # keeps track of which hosts have shard data we're restoring
    hostShardMap = {}

    remoteSolrDir = _env(cluster, 'solr_tip')
        
    # collect the replica information for each shard for the collection we're restoring data into
    shardDirs = {}
    with settings(host_string=hosts[0]), hide('output','running'):
        run('source ~/cloud/'+ENV_SCRIPT+'; cd %s/cloud84/scripts/cloud-scripts; ./zkcli.sh -zkhost $ZK_HOST -cmd getfile /clusterstate.json /tmp/clusterstate.json' % remoteSolrDir)
        get('/tmp/clusterstate.json', './clusterstate.json')    
        # parse the clusterstate.json to get the shard leader node assignments
        _status('Fetching /clusterstate.json to get shard leader node assignments for '+collection)
        clusterStateFile = open('./clusterstate.json')    
        clusterState = json.load(clusterStateFile)
        clusterStateFile.close()                       
        if clusterState.has_key(collection) is False:
            # assume an external collection
            _warn('Collection '+collection+' not found in /clusterstate.json, looking for external state ...')
            run('source ~/cloud/'+ENV_SCRIPT+'; cd %s/cloud84/scripts/cloud-scripts; ./zkcli.sh -zkhost $ZK_HOST -cmd getfile /collections/%s/state /tmp/state.json' % (remoteSolrDir, collection))
            get('/tmp/state.json', './state.json')
            clusterStateFile = open('./state.json')
            clusterState = json.load(clusterStateFile)   
            clusterStateFile.close()    
            if clusterState.has_key(collection) is False:
                _fatal('Cannot find state information for '+collection+' in ZooKeeper!')
             
    collState = clusterState[collection]
    shards = collState['shards']
    for shard in shards.keys():
        shardDirs[shard] = []
        replicas = shards[shard]['replicas']
        for replica in replicas.keys():
            node_name = replicas[replica]['node_name']
            if node_name.endswith('_solr'):
                node_name = node_name[0:len(node_name)-5]
            hostAndPort = node_name.split(':') 
            info = {}
            info['leader'] = replicas[replica].has_key('leader')               
            info['host'] = hostAndPort[0]
            info['port'] = hostAndPort[1][2:]
            info['core'] = replicas[replica]['core']
            shardDirs[shard].append(info)
            
    _status('Found shard replica host assignments:')
    print(json.dumps(shardDirs, indent=2))

    # clean-up restore dir on all hosts to prepare for download
    if ebsHost is None:
        for host in hosts:    
            with settings(host_string=host): #, hide('output'):
                if needsDownload:
                    sudo('rm -rf /vol0/restore/%s' % backup_name) # delete if we're downloading new
                sudo('mkdir -p /vol0/restore/%s; chown -R %s: /vol0/restore/%s' % (backup_name, ssh_user, backup_name))

    # for each shard, download the shardN.tar file from S3 on to the leader host
    if needsDownload:
        _info('Setting up to download index from s3://'+backup)
        for shard in shardDirs.keys():
            for replica in shardDirs[shard]:
                if replica['leader']:
                    host = replica['host']
                    if hostShardMap.has_key(host) is False:
                        hostShardMap[host] = []
                    hostShardMap[host].append(shard)
                    # kick off the s3 sync to run in the background so that we can download more files concurrently
                    with settings(host_string=host): #, hide('output'):
                        s3sync = ('s3cmd --progress sync s3://%s/%s /vol0/restore/%s/' % (backup, shard, backup_name))
                        run('nohup '+s3sync+' > /vol0/restore/'+backup_name+'/s3sync-'+shard+'.log 2>&1 &', pty=False)
                        
        # poll each host that is downloading until the downloads are complete
        done = 0
        numDownloading = len(hostShardMap.keys())
        _status('Waiting on %d hosts to download %d shard index files' % (numDownloading, len(shardDirs.keys())))            
        while done < numDownloading:
            done = 0 # reset the counter since we check all nodes each loop
            _status('Sleeping for 30 seconds before checking status of S3 downloads on %d hosts' % numDownloading)
            time.sleep(30)
            for host in hostShardMap.keys():
                with settings(host_string=host), hide('output', 'running', 'warnings'):
                    pid = run('ps waux | grep s3cmd | grep -v grep | grep '+backup_name+' | awk \'{print $2}\' | sort -r')
                    if len(pid.strip()) == 0:
                        done += 1
                        _info('s3cmd done on '+host)
                    else:
                        _status('s3cmd still running on '+host+', pid: '+pid)
        _info('s3cmd seems to be done on '+str(numDownloading)+' hosts')
    else:
        # need to find where the downloaded files live
        for shard in shardDirs.keys():
            if ebsHost is None:
                for host in hosts:
                    with settings(host_string=host):
                        found = run('find /vol0/restore/'+backup_name+' -name '+shard+' -type d | wc -l')
                        numFound = int(found)
                        if numFound > 0:
                            if hostShardMap.has_key(host) is False:
                                hostShardMap[host] = []
                            hostShardMap[host].append(shard)
            else:
                if hostShardMap.has_key(ebsHost) is False:
                    hostShardMap[ebsHost] = []
                hostShardMap[ebsHost].append(shard)
                

        _info('hostShardMap: ')
        print(json.dumps(hostShardMap, indent=2))
    
    # TODO: try to run the following commands on all nodes at once vs. synchronously
       
    # restore the index data for all replicas across the cluster
    # one might think you only need to do for the leader, but then the replica
    # would need to snap-pull from the leader anyway, so better to do all work now               
    for host in hostShardMap.keys():
        with settings(host_string=host), hide('output'):
            # we'll be ssh'ing and scp'ing from the first host to the others
            # to move files around for the restore process
            put(_env(cluster,'ssh_keyfile_path_on_local'), '%s/.ssh' % user_home)
            run('chmod 600 '+_env(cluster,'ssh_keyfile_path_on_local'))
                        
            for shard in hostShardMap[host]:                
                _status('Restoring '+shard+' from host '+host)
                                
                # untar all the downloaded shardN.tar files on this host
                #if needsDownload:            
                #run('cd /vol0/restore/%s; cat %s-tgz-* | tar xz; rm -f %s-tgz-*' % (backup_name, shard, shard))
                restoreFromDir = '/vol0/restore/'+backup_name if backupOnEbs is None else backupOnEbs
                            
                replicas = shardDirs[shard]
                for r in replicas:                
                    shardHost = r['host']
                    coreDir = ('%s/cloud%s/solr/%s' % (remoteSolrDir, r['port'], r['core']))
                    # do all the other hosts first so that we can move vs. copy on the localhost
                    if shardHost != host:
                        # scp
                        sshCmd = ('ssh -o StrictHostKeyChecking=no -i %s %s@%s "mv %s/data/index %s/data/index-old; rm -rf %s/data/tlog/*"' % 
                                  (_env(cluster,'ssh_keyfile_path_on_local'), ssh_user, shardHost, coreDir, coreDir, coreDir))
                        run(sshCmd)
                        _status('scp index data for '+shard+' on '+shardHost+':'+r['port'])
                        
                        scpCmd = ('scp -o StrictHostKeyChecking=no -r -i %s %s/%s/* %s@%s:%s/data/index' % 
                                  (_env(cluster,'ssh_keyfile_path_on_local'), restoreFromDir, shard, ssh_user, shardHost, coreDir))
                        run(scpCmd)
    
                for r in replicas:
                    shardHost = r['host']
                    coreDir = ('%s/cloud%s/solr/%s' % (remoteSolrDir, r['port'], r['core']))
                    if shardHost == host:
                        # local copy to replica
                        _status('cp index data for '+shard+' on '+shardHost+':'+r['port'])
                        moveCmd = ('mv %s/data/index %s/data/index-old; cp -r %s/%s/* %s/data/index; rm -rf %s/data/tlog/*' % 
                                   (coreDir, coreDir, restoreFromDir, shard, coreDir, coreDir))
                        run(moveCmd)                        
                
    _status('Restore index data complete ... reloading collection: '+collection)          
    urllib2.urlopen('http://%s:8984/solr/admin/collections?action=RELOAD&name=%s' % (hosts[0], collection))
    time.sleep(10)        
    solr = pysolr.Solr('http://%s:8984/solr/%s' % (hosts[0], collection), timeout=10)
    results = solr.search('*:*')                
    _info('Restore '+collection+' complete. Docs: '+str(results.hits))
    healthcheck(cluster,collection)
    
def put_file(cluster,local,remotePath=None,num=1):
    """
    Upload a local file to one or more hosts in the specified cluster.
    Basically, a helper function on top of scp
    """    
    localFile = os.path.expanduser(local)    
    if os.path.isfile(localFile) is False:
        _fatal('File %s not found on local workstation!' % localFile)
            
    hosts = _lookup_hosts(cluster, False)
    scp_hosts = [hosts[0]] if int(num) <= 0 else hosts[0:int(num)]
    homeDir = _env(cluster,'user_home')
    remoteDir = homeDir if remotePath is None else remotePath
    for host in scp_hosts:    
        with settings(host_string=host):
            put(local, remoteDir+'/')

def get_file(cluster,remote,n=0,local='./'):
    """
    Get a file from one of the remote hosts in your cluster.
    """
    localDir = os.path.expanduser(local)    
    if os.path.isdir(localDir) is False:
        _fatal('Directory %s not found on local workstation!' % localDir)            
    idx = int(n)
    hosts = _lookup_hosts(cluster, False)
    with settings(host_string=hosts[idx]):
        get(remote,str(localDir))
    
def index_docs(cluster,collection,numDocs=20000):
    """
    Index synthetic documents into a collection; mostly only useful for demos.
    """
    zkHost = _read_cloud_env(cluster)['ZK_HOST'] # get the zkHost from the env on the server
    local('./tools.sh indexer -collection=%s -zkHost=%s -numDocsToIndex=%d' % (collection, zkHost, int(numDocs)))

def restart_solr(cluster,wait=0,waitAfterKill=30):
    """
    Initiates a rolling restart of a Solr cluster.
    
    Pass wait=N (N > 0) to set a max wait for nodes to come back online;
    default behavior is to poll the node status until it is up with a max
    wait of 180 seconds.
    """
    cloud = _provider_api()    
    _rolling_restart_solr(cloud, cluster, None, wait, waitAfterKill)
    cloud.close()

def bunch_of_collections(cluster,prefix,num=2,shards=4,rf=3,conf='cloud',numDocs=10000,offset=0):
    """
    create a bunch of collections and index some docs
    """
    zkHost = _read_cloud_env(cluster)['ZK_HOST'] # get the zkHost from the env on the server
    offsetIndex = int(offset)
    numCollections = int(num)
    if offsetIndex < 0 or offsetIndex > numCollections:
        _fatal('Invalid offset! '+offset)
    for n in range(offsetIndex,numCollections):
        name = prefix + str(n)
        new_collection(cluster,name=name, rf=int(rf), shards=int(shards), conf=conf)
        #local('./tools.sh indexer -collection=%s -zkHost=%s -numDocsToIndex=%d' % (name, zkHost, numDocs))
        
def jconsole(cluster):
    """
    Launch and attach JConsole to the JVM bound to port 8984 on the first host in the cluster.
    """
    hosts = _lookup_hosts(cluster, False)
    local('jconsole %s:1084' % hosts[0])
    
def grep_logs(cluster,match,n=None,port=None):
    """
    Quick way to grep for a value across all Solr logs in a cluster.
    """
    hosts = _lookup_hosts(cluster)
    # all hosts or just one specified as an arg
    if n is not None:
        hosts = [hosts[int(n)]]
        
    # all ports or just one specified as as arg
    ports = []
    if port is not None:
        ports.append(int(port))
    else:
        for p in range(0,_num_solr_nodes_per_host(cluster)):
            ports.append(84 + int(p))        

    remoteSolrDir = _env(cluster, 'solr_tip')
    for host in hosts:    
        with settings(host_string=host):
            for p in ports:
                logFile = '%s/cloud%d/logs/solr.log' % (remoteSolrDir, p)
                run("grep $'%s' %s || true" % (match, logFile))
                
def proc(cluster,proc,kill=False):
    """
    Find and optionally kill a process running on all hosts on the cluster.
    """
    killEm = bool(kill)
    hosts = _lookup_hosts(cluster)
    for host in hosts:    
        with settings(host_string=host), hide('running'):
            procCmd = '(for ID in `ps waux | grep '+proc+' | grep -v grep | awk \'{print $2}\' | sort -r`\n'
            if killEm:
                procCmd += 'do\nkill -9 $ID;echo "killed %s:$ID on host %s"\ndone) || true' % (proc, host)
            else:
                procCmd += 'do\necho "%s:$ID running on %s"\ndone) || true' % (proc, host)
            run(procCmd)

def healthcheck(cluster,collection):
    """
    Perform a healthcheck against a collection running in a SolrCloud cluster.
    """
    zkHost = _read_cloud_env(cluster)['ZK_HOST'] # get the zkHost from the env on the server
    local('./tools.sh healthcheck -collection=%s -zkHost=%s' % (collection, zkHost))

def setup_instance_stores(cluster,numInstanceStores=1):
    """
    Setup instance stores on an existing cluster; this approach is tightly coupled to the EC2 way.
    """
    cloud = _provider_api()    
    hosts = _cluster_hosts(cloud, cluster)
    if _verify_ssh_connectivity(hosts, 180) is False:
        _fatal('Failed to verify SSH connectivity to all hosts!')
    xdevs = ['xvdb','xvdc','xvdd','xvde']
    _setup_instance_stores(hosts, int(numInstanceStores), _env(cluster, 'AWS_HVM_AMI_ID'), xdevs)
    cloud.close()    

def reload_collection(cluster,collection):
    """
    Reload a collection using the Collections API; run the healthcheck after reloading.
    """
    cloud = _provider_api()    
    hosts = _cluster_hosts(cloud, cluster)
    urllib2.urlopen('http://%s:8984/solr/admin/collections?action=RELOAD&name=%s' % (hosts[0], collection))
    time.sleep(10)        
    solr = pysolr.Solr('http://%s:8984/solr/%s' % (hosts[0], collection), timeout=10)
    results = solr.search('*:*')                
    _info('Restore '+collection+' complete. Docs: '+str(results.hits))
    healthcheck(cluster,collection)
    
def jmeter(solrCluster,jmxfile,jmeterCluster=None,collection=None,propertyfile=None,depsfolder=None,runname='jmeter-test'):
    """
    Upload and run a JMeter test plan against your cluster.
    """
    homeDir = _env(solrCluster, 'user_home')
    jmeterDir = '%s/jmeter-2.11' % homeDir
    jmeterTestDir = '%s/%s' % (homeDir, runname)
    jmeterTestLogDir = '%s/logs' % jmeterTestDir
    jmeterTestLogFile = '%s/jmeter.log' % jmeterTestLogDir

    if collection is None:
        _warn('No collection name specified, assuming same as solr cluster name')
        collection = solrCluster
    if jmeterCluster is None:
        _warn('No jmeter cluster name specified, assuming same as solr cluster name')
        jmeterCluster = solrCluster

    cloud = _provider_api()
    hosts = _cluster_hosts(cloud, jmeterCluster)
    _info('Preparing to execute jmeter tests on cluster = %s and host = %s' % (jmeterCluster, hosts[0]))

    zkHostStr = _read_cloud_env(solrCluster)['ZK_HOST'] # get the zkHost from the env on the server
    _info("Using ZK host string: %s" % zkHostStr)

    with settings(host_string=hosts[0]):
        run ('mkdir -p %s' % jmeterTestLogDir)
        # put ('target/solr-scale-tk-0.1.jar', '%s/lib/ext/' % jmeterDir)
        if propertyfile is not None:
            put (propertyfile, '%s/.' % jmeterTestDir)
        if depsfolder is not None:
            local ('tar -cvf dependencies.tar.gz -C %s %s' % (os.path.dirname(os.path.abspath(depsfolder)), os.path.basename(os.path.abspath(depsfolder))))
            put ('dependencies.tar.gz', jmeterTestDir)
            with cd(jmeterTestDir):
                run ('tar -xvf dependencies.tar.gz')
            local ('rm dependencies.tar.gz')
        put(jmxfile, jmeterTestDir)
        jmxFileName = os.path.basename(jmxfile)
        if propertyfile is not None:
            propFileName = os.path.basename(propertyfile)
            with cd(jmeterTestDir):
                run('export JVM_ARGS=\'-Dcommon.defaultCollection=%s -Dcommon.endpoint=%s\';%s/bin/jmeter -q %s -n -t %s -j %s' % (collection, zkHostStr, jmeterDir, propFileName, jmxFileName, jmeterTestLogFile))
        else:
            with cd(jmeterTestDir):
                run('export JVM_ARGS=\'-Dcommon.defaultCollection=%s -Dcommon.endpoint=%s\';%s/bin/jmeter -n -t %s -j %s' % (collection, zkHostStr, jmeterDir, jmxFileName, jmeterTestLogFile))
        get(jmeterTestLogDir, '.')

def attach_to_meta_node(cluster,meta):    
    """
    Configures existing SolrCloud nodes to attach to an existing meta node. This command
    is useful if you add a meta node after starting a SolrCloud cluster.
    """
    cloud = _provider_api()
    metaHost = _cluster_hosts(cloud, meta)[0]
    if metaHost is None:
        _fatal('Meta node '+meta+' not found!')

    print 'metaHost='+metaHost

    hosts = _cluster_hosts(cloud, cluster)
    for host in hosts:
        with settings(host_string=host): #, hide('output', 'running', 'warnings'):
            _integ_host_with_meta(cluster, host, metaHost)
    # restart each Solr node, waiting to see them come back up
    _rolling_restart_solr(cloud, cluster, None, 0)
    cloud.close()
    
def restart_node(cluster,port,n=0):
    """
    Restart a specific Solr node by specifying the cluster, port and node index.
    """
    hosts = _lookup_hosts(cluster)    
    hostIdx = int(n)
    if hostIdx < 0 or hostIdx >= len(hosts):
        _fatal('Invalid value '+n+' for host index!')        
    host = hosts[int(n)]
    
    if len(port) == 4:
        port = port[2:]
    
    with settings(host_string=host):
        _status('Restarting Solr on '+host+':89'+port)
        _restart_solr(cluster, host, port)

def add_overseer_role(cluster, n, port):
    """
    Add the overseer role to a node
    """
    hosts = _lookup_hosts(cluster, False)
    host = hosts[int(n)]
    nodeName = "%s:89%d_solr" % (host, int(port))
    _add_overseer_role(hosts, nodeName)

def cluster_status(cluster, collection=None, shard=None):
    """
    Retrieve status for the specified cluster.

    Arg Usage:
      cluster: Identifies the SolrCloud cluster you want to get status for.
      collection (optional): restricts status info to this collection.
      shard (optional, comma-separated list): restricts status info to this shard/set of shards.
    """

    hosts = _lookup_hosts(cluster, False)

    params = '&wt=json&indent=on'
    if collection is not None:
        params += '&collection=%s' % collection
    if shard is not None:
        params += '&shard=%s' % shard

    statusAction = 'http://%s:8984/solr/admin/collections?action=CLUSTERSTATUS%s' % (hosts[0], params)

    _info('Retrieving cluster status using:\n%s' % statusAction)
    try:
        response = urllib2.urlopen(statusAction)
        solr_resp = response.read()
        _info('Cluster status retrieval succeeded\n' + solr_resp)
    except urllib2.HTTPError as e:
        _error('Cluster status retrieval failed due to: %s' % str(e) + '\n' + e.read())

def clusterprop(cluster, name, value):
    """
    Sets cluster wide properties using Solr's CLUSTERPROP Collections API
    :param cluster: the cluster name on which the property is to be set
    :param name: the name of the property
    :param value: the value of the property
    """
    hosts = _lookup_hosts(cluster, False)
    statusAction = 'http://%s:8984/solr/admin/collections?action=clusterprop&name=%s&val=%s&wt=json' % (hosts[0], name, value)
    try:
        response = urllib2.urlopen(statusAction)
        solr_resp = response.read()
        _info('Cluster property set successfully:\n' + solr_resp)
        return True
    except urllib2.HTTPError as e:
        _error('Cluster property could not be set due to: %s' % str(e) + '\n' + e.read())
        return False

def stats(cluster):
    """
    Get cluster stats such as collections, total docs, replicas/node, replicas/host etc
    """
    hosts = _lookup_hosts(cluster, False)
    # number of collections, total docs, replicas/node, replicas/host, docs/collection
    collections = []
    listAction = 'http://%s:8984/solr/admin/collections?action=list&wt=json' % hosts[0]
    try:
        response = urllib2.urlopen(listAction)
        solr_resp = response.read()
        collectionList = json.loads(solr_resp)
        collections = collectionList['collections']
        _status('Collections: %d' % len(collections))
    except urllib2.HTTPError as e:
        _error('Unable to fetch collections list due to: HTTP status %d: %s' % (e.code, str(e.reason)))
    except: # catch all exceptions
        e = sys.exc_info()[0]
        _error('Unable to fetch collections list due to: %s' % str(e))

    numNodesPerHost = _num_solr_nodes_per_host(cluster)
    ports = [84 + x for x in range(0, numNodesPerHost)]

    replicasPerHost = dict({})
    replicasPerNode = dict({})
    print('')
    _info('{0:50s} {1:>10s}'.format('Node', 'Replicas'))
    for h in hosts:
        replicasPerHost[h] = []
        for p in ports:
            coreAdminStatus = 'http://%s:89%d/solr/admin/cores?action=status&wt=json' % (h, p)
            try:
                response = urllib2.urlopen(coreAdminStatus)
                solr_resp = response.read()
                coreStatus = json.loads(solr_resp)
                replicas = coreStatus['status'].keys()
                hostPort = '%s:89%d' % (h, p)
                replicasPerNode[hostPort] = replicas
                replicasPerHost[h].extend(replicas)
                #_status('Node: %s has %d replicas' % (hostPort, len(replicas)))
                _status('{0:50s} {1:10d}'.format(hostPort, len(replicas)))
            except urllib2.HTTPError as e:
                _error('Unable to fetch core status for %s:89%d due to: HTTP status %d: %s' % (h, p, e.code, str(e.reason)))
            except: # catch all exceptions
                e = sys.exc_info()[0]
                _error('Unable to fetch core status for %s:89%d due to: %s' % (h, p, str(e)))

    print('')
    _info('{0:50s} {1:>10s}'.format('Host', 'Replicas'))
    for h in hosts:
        _status('{0:50s} {1:10d}'.format(h, len(replicasPerHost[h])))
        #_status('Host: %s has %d replicas' % (h, len(replicasPerHost[h])))

    print('')
    _info('{0:25s} {1:>10s}'.format('Collection', 'Documents'))
    docsPerCollection = dict({})
    for c in sorted(collections):
        solr = pysolr.Solr('http://%s:8984/solr/%s' % (hosts[0], c), timeout=10)
        results = solr.search('*:*', **{'rows' : 0})
        docsPerCollection[c] = results.hits
        _status('{0:25s} {1:10d}'.format(c, results.hits))
    print('')
    _info('{0:25s} {1:10d}'.format('Total Documents', sum(docsPerCollection.values())))

### The following tasks are for working with Lucidworks Fusion services

def _fusion_api(host, apiEndpoint, method='GET', json=None):
    dashd = "" if json is None else " -d '"+json+"'"
    local("curl -u admin:password123 -H 'Content-type: application/json' -X %s%s http://%s:8765/lucid/api/v1/%s" % (method, dashd, host, apiEndpoint))

def fusion_nodes(cluster):
    hosts = _lookup_hosts(cluster)
    _fusion_api(hosts[0], 'nodes/hosts')

def fusion_new_search_cluster(cluster, name):
    # just use curl for now
    hosts = _lookup_hosts(cluster)
    zkHost = _read_cloud_env(cluster)['ZK_HOST'] # get the zkHost from the env on the server
    createClusterJson = '{"id":"'+name+'", "connectString":"'+zkHost+'", "cloud":true}'
    _fusion_api(hosts[0], 'searchCluster', 'POST', createClusterJson)

def fusion_new_collection(cluster, name, rf=1, shards=1, conf='cloud'):
    hosts = _lookup_hosts(cluster)
    numNodes = _num_solr_nodes_per_host(cluster)
    nodes = len(hosts) * numNodes
    replicas = int(rf) * int(shards)
    maxShardsPerNode = int(max(1, round(replicas / nodes)))
    json = '{ "id":"'+name+'", "solrConfName":"'+conf+'", "solrParams": { "replicationFactor":'+rf+', "numShards":'+shards+', "maxShardsPerNode":'+str(maxShardsPerNode)+' }}'
    zkHost = _read_cloud_env(cluster)['ZK_HOST'] # get the zkHost from the env on the server
    _fusion_api(hosts[0], 'collections', 'POST', json)

def fusion_start(cluster,fusion=1,ui=1,connectors=1):
    hosts = _lookup_hosts(cluster)

    fusionHome = _env(cluster, 'fusion_home')
    fusionBin = fusionHome+'/bin'

    # create the fusion.in.sh include file
    zkHost = _read_cloud_env(cluster)['ZK_HOST'] # get the zkHost from the env on the server
    fusionInSh = 'export ZK_HOST='+zkHost+'\n'
    for host in hosts:
        with settings(host_string=host), hide('output','running'):
            run('rm -rf '+fusionBin+'/fusion.in.sh')
            _status('Uploading fusion.in.sh includes file to '+host)
            _fab_append(fusionBin+'/fusion.in.sh', fusionInSh)

    # start fusion, ui, connectors
    numFusionNodes = int(fusion)
    if numFusionNodes > len(hosts):
        _fatal('Cannot start more than %d Fusion nodes!' % len(hosts))
    numUINodes = int(ui)
    if numUINodes > len(hosts):
        _fatal('Cannot start more than %d UI nodes!' % len(hosts))
    numConnectorsNodes = int(connectors)
    if numConnectorsNodes > len(hosts):
        _fatal('Cannot start more than %d Connectors nodes!' % len(hosts))

    if numFusionNodes > 0:
        _status('Starting Fusion service on %d nodes.' % numFusionNodes)
        for host in hosts[0:numFusionNodes]:
            with settings(host_string=host), hide('output', 'running'):
                run(fusionBin+'/stop-fusion.sh || true')
                run('nohup '+fusionBin+'/run-fusion.sh > '+fusionHome+'/logs/fusion/run.out 2>&1 &', pty=False)
                time.sleep(2)
                _info('Started Fusion service on '+host)

    if numUINodes > 0:
        _status('Starting Fusion UI service on %d nodes.' % numUINodes)
        for host in hosts[0:numUINodes]:
            with settings(host_string=host), hide('output', 'running'):
                run(fusionBin+'/stop-ui.sh || true')
                run('nohup '+fusionBin+'/run-ui.sh > '+fusionHome+'/logs/ui/run.out 2>&1 &', pty=False)
                time.sleep(2)
                _info('Started Fusion UI service on '+host)

    if numConnectorsNodes > 0:
        _status('Starting Fusion Connectors service on %d nodes.' % numUINodes)
        for host in hosts[0:numConnectorsNodes]:
            with settings(host_string=host), hide('output', 'running'):
                run(fusionBin+'/stop-connectors.sh || true')
                run('nohup '+fusionBin+'/run-connectors.sh > '+fusionHome+'/logs/connectors/run.out 2>&1 &', pty=False)
                time.sleep(2)
                _info('Started Connectors service on '+host)


def fusion_stop(cluster):
    hosts = _lookup_hosts(cluster)
    fusionHome = _env(cluster, 'fusion_home')
    fusionBin = fusionHome+'/bin'
    for host in hosts:
        with settings(host_string=host), hide('output','running'):
            run('nohup '+fusionBin+'/stop-all.sh > /dev/null 2>&1 &', pty=False)
            _status('Stopped Fusion services on '+host)


def fusion_endpoints(cluster,n,coll):
    str = ''
    hosts = _lookup_hosts(cluster)
    for host in hosts:
        if len(str) > 0:
            str += ','
        str += ('http://%s:8765/lucid/api/v1/index-pipelines/conn_solr/collections/%s/index' % (host, coll))
    print(str)