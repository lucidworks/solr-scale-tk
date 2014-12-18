
API_PORT=8765
API_STOP_PORT=7765
API_STOP_KEY=fusion

CONNECTORS_PORT=8984
CONNECTORS_STOP_PORT=7984
CONNECTORS_STOP_KEY=fusion

SOLR_PORT=8983
SOLR_STOP_PORT=7983
SOLR_STOP_KEY=fusion

UI_PORT=8764
UI_STOP_PORT=7764
UI_STOP_KEY=fusion

FUSION_ZK=localhost:9983
FUSION_SOLR_ZK=localhost:9983

if [ -z "$SCRIPT" ]; then echo "SCRIPT not set"; exit 1; fi
if [ -z "$FUSION_HOME" ]; then echo "FUSION_HOME not set"; exit 1; fi

if [ -r "$FUSION_HOME/bin/fusion.in.sh" ]; then
  . $FUSION_HOME/bin/fusion.in.sh
fi

DATEFORMAT="+%Y-%m-%d %H:%M:%SZ"
function output() {
  echo $(date -u "$DATEFORMAT") "$1"
}

function check_jetty_vars() {
  if [ -z "$FUSION_SERVICE_NAME" ]; then echo "FUSION_SERVICE_NAME not set"; exit 1; fi
  if [ -z "$PID_FILE" ]; then echo "PID_FILE not set"; exit 1; fi
  if [ -z "$JETTY_BASE" ]; then echo "JETTY_BASE not set"; exit 1; fi
  if [ -z "$LOG_DIR" ]; then echo "LOG_DIR not set"; exit 1; fi
}

function do_start() {
  check_java
  check_jar
  check_jetty_vars

  if [ -f "$PID_FILE" ]; then
    pid=`cat "${PID_FILE}"`
    if kill -0 $pid &>/dev/null ; then
      output "process $pid from pid file $PID_FILE is running; not starting"
      return
    fi
  fi

  report_port "Starting"
  nohup "$FUSION_HOME/bin/$SCRIPT" "run" > "$LOG_DIR/run.out" 2>&1 </dev/null &
}

function do_status() {
  check_jetty_vars
  if [ -f "$PID_FILE" ]; then
    pid=`cat "${PID_FILE}"`
    if kill -0 $pid &>/dev/null; then
      output "process $pid from pid file $PID_FILE is running"
    else
      output "process $pid from pid file $PID_FILE is not running"
    fi
  else
    output "no pid file $PID_FILE"
  fi
}

function do_stop() {
  check_jetty_vars
  check_java
  extra_java_options

  if [ ! -f "$PID_FILE" ]; then
    output "no pid file $PID_FILE"
    return
  fi

  pid=`cat "${PID_FILE}"`
  if ! kill -0 $pid &>/dev/null ; then
    output "process $pid from pid file $PID_FILE is not running"
    rm "$PID_FILE"
    return
  fi

  output "Stopping jetty for $FUSION_SERVICE_NAME"

  if "$JAVA" \
    $JAVA_OPTIONS \
    "-DSTOP.PORT=$STOP_PORT" \
    "-DSTOP.KEY=$STOP_KEY" \
    -jar "$JETTY_HOME/start.jar" \
    "jetty.home=$JETTY_HOME" \
    "jetty.base=$JETTY_BASE" \
    "$JETTY_BASE/etc/jetty-logging.xml" \
    "--stop" ; then
    sleep 6 # give the process time to exit after shutting the port
  fi

  if kill -0 $pid &>/dev/null ; then
    output "process $pid from pid file $PID_FILE is running. Sending TERM signal"
    kill $pid
    sleep 5
    if kill -0 $pid &>/dev/null ; then
      output "process $pid from pid file $PID_FILE is still running. Sending KILL signal"
      kill -9 $pid &>/dev/null
      sleep 1
    fi
  fi
  if [ -f "$PID_FILE" ]; then
    rm "$PID_FILE"
  fi
}

function do_usage() {
  echo "Usage: $0 [start, stop, status, restart, run]"
  exit 1
}

function set_ports() {
  case "$PORT_NAME" in
    'api')
      HTTP_PORT=$API_PORT;
      STOP_PORT=$API_STOP_PORT;
      STOP_KEY=$API_STOP_KEY
      ;;
    'connectors')
      HTTP_PORT=$CONNECTORS_PORT;
      STOP_PORT=$CONNECTORS_STOP_PORT;
      STOP_KEY=$CONNECTORS_STOP_KEY
      ;;
    'solr')
      HTTP_PORT=$SOLR_PORT;
      STOP_PORT=$SOLR_STOP_PORT;
      STOP_KEY=$SOLR_STOP_KEY
      ;;
    'ui')
      HTTP_PORT=$UI_PORT;
      STOP_PORT=$UI_STOP_PORT;
      STOP_KEY=$UI_STOP_PORT
      ;;
    '')
      echo "PORT_NAME not set";
      exit 1
      ;;
  esac
}

function report_port() {
  msg="$1"
  if [ -z "$msg" ]; then
    msg="Running"
  fi
  output "$msg $FUSION_SERVICE_NAME on port $HTTP_PORT"
}

function check_java() {
  JAVA=$(which java || echo '')
  if [ -z "$JAVA" ]; then
    output "no java"
    exit 1
  fi
  JAVA_VERSION=$(java -version 2>&1 | head -n 1 | sed 's/java version "//' | sed 's/"$//' | sed 's/_.*//')
  if [ -z "$JAVA_VERSION" ]; then
    output "Cannot determine java version"
    exit 1
  fi
  JAVA_MAJOR=$(echo "$JAVA_VERSION" | sed 's/\..*//')
  JAVA_MINOR=$(echo "$JAVA_VERSION" | sed 's/^[0-9].//' | sed 's/\..*//')
  if (( "$JAVA_MAJOR" == "1"  )) && (( "$JAVA_MINOR" < 7 )) ; then
    output "This product requires at least Java 1.7"
    exit 1
  fi
}

function check_jar() {
  JAR_PROG=$(which jar || echo '')
  if [ -z "$JAR_PROG" ]; then
    output "Cannot find the 'jar' program; please install a full Java 1.7 JDK"
    exit
  fi
}

function write_pid_file() {
    echo $$ > "$PID_FILE"
}

function extra_java_options() {
  # Workaround LUCENE-5212 per https://wiki.apache.org/lucene-java/JavaBugs
  JAVA_VERSION=$(java -version 2>&1 | head -n 1 | sed 's/java version "//' | sed 's/"$//' | sed 's/_.*//')
  if [ $JAVA_VERSION = '1.7.0' ]; then
    PATCH_LEVEL=$(java -version 2>&1 | head -n 1 | sed 's/java version "//' | sed 's/"$//' | sed 's/^.*_//')
    if (( $PATCH_LEVEL >= 40 )) && (( $PATCH_LEVEL < 60 )); then
      JAVA_OPTIONS+=" -XX:-UseSuperWord"
    fi
  fi

  if [ ! -z "$JAVA_MAXPERMSIZE" ]; then
    if (( "$JAVA_MAJOR" == "1" )) && (( "$JAVA_MINOR" < 8 )); then
      JAVA_OPTIONS+=" -XX:MaxPermSize=$JAVA_MAXPERMSIZE"
    fi
  fi
}

function expand_war() {
  WAR_FILE=$1
  APP_DIR=$2

  if [ ! -d "$APP_DIR" ]; then
    # expand the WAR
    if [ ! -f "$WAR_FILE" ]; then
        output "Cannot find the $WAR_FILE"
        exit -1
    fi

    mkdir -p "$APP_DIR"
    output "Extracting $WAR_FILE to $APP_DIR"
    (cd "$APP_DIR"; jar -xf "$WAR_FILE")
  fi
}

function main() {
  arg="$1"
  cd "$FUSION_HOME"
  set_ports
  if [ "$arg" = "start" ]; then
    do_start
  elif [ "$arg" = "stop" ]; then
    do_stop
  elif [ "$arg" = "restart" ]; then
    do_stop
    sleep 1
    do_start
  elif [ "$arg" = "status" ]; then
    do_status
  elif [ "$arg" = "run" ]; then
    do_run
  elif [ "$arg" = "help" ]; then
    do_usage
  elif [ -z "$arg" ]; then
    do_run
  else
    echo "Unknown action: $arg"
    do_usage
  fi
}
