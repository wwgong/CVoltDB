#!/usr/bin/env bash
# eoneil: TPCC script, derived simply from older TPCC version
# and here VOLTROOT is a parameter
# Added targets for running with asserts enabled: server-ea, client-ea
# Usage bash run2h.sh <dir for voltdb> command
# See runservers4.sh for the script that uses this one
APPNAME="tpcc"
VOLTROOT=../$1
CLASSPATH=$({ \
    \ls -1 $VOLTROOT/voltdb/voltdbclient-[23].*.jar; \
    \ls -1 "$VOLTROOT"/lib/*.jar; \
    \ls -1 "$VOLTROOT"/lib/extension/*.jar; \
} 2> /dev/null | paste -sd ':' - )
echo CLASSPATH: $CLASSPATH
VOLTDB="$VOLTROOT/bin/voltdb"
VOLTCOMPILER="$VOLTROOT/bin/voltcompiler"
LICENSE="$VOLTROOT/voltdb/license.xml"
LEADER="192.168.2.222"
SERVERS="192.168.2.222,192.168.2.223,192.168.2.224,192.168.2.221"
WAREHOUSES=16
# remove build artifacts
function clean() {
    rm -rf obj debugoutput $APPNAME.jar  plannerlog.txt
}

# compile the source code for procedures and the client
function srccompile() {
    mkdir -p obj
    javac -classpath $CLASSPATH -d obj \
        src/com/*.java \
        src/com/procedures/*.java \
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
}

# build an application catalog
function catalog() {
    srccompile
    $VOLTCOMPILER obj project.xml $APPNAME.jar
    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
}

# run the voltdb server locally
function server() {
	echo running server with tpcc app jar
    # if a catalog doesn't exist, build one
    if [ ! -f $APPNAME.jar ]; then catalog; fi
    # run the server
    $VOLTDB create catalog $APPNAME.jar deployment mh4_deployment.xml \
        license $LICENSE leader $LEADER
}

# run the voltdb server locally, with asserts enabled
function server-ea() {
	echo running server with tpcc app jar
    # if a catalog doesn't exist, build one
    if [ ! -f $APPNAME.jar ]; then catalog; fi
    # add -ea to flags to enable asserts--
    VOLTDB_OPTS=-ea; export VOLTDB_OPTS
    # run the server
    $VOLTDB create catalog $APPNAME.jar deployment mh4_deployment.xml \
        license $LICENSE leader $LEADER
}

# run the client that drives the tpcc example
function client() {
    srccompile
    java -classpath obj:$CLASSPATH:obj com.MyTPCC \
        --servers=$SERVERS \
        --duration=600 \
        --warehouses=$WAREHOUSES
}

# run the client that drives the tpcc example, with asserts on
function client-ea() {
    srccompile
    java -classpath obj:$CLASSPATH:obj -ea com.MyTPCC \
        --servers=localhost \
        --duration=60 \
        --warehouses=$WAREHOUSES
}

function help() {
    echo "Usage: ./run_tpcc2.sh voltrootdir {clean|catalog|server|server-ea|client|client-ea}"
}

# Run the target passed as the last arg on the command line
# for targets needing voltroot spec, use that before the command
if [ $# -gt 2 ]; then help; exit; fi
if [ $# = 2 ]; then $2; fi
if [ $# = 1 ]; then $1; fi
if [ $# = 0 ]; then help; fi
