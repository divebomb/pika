#!/usr/bin/env bash
# ******************************************************
# DESC    : zookeeper devops script
# AUTHOR  : Alex Stocks
# VERSION : 1.0
# LICENCE : LGPL V3
# EMAIL   : alexstocks@foxmail.com
# MOD     : 2016-05-13 02:01
# FILE    : load.sh
# ******************************************************

name="zookeeper"

usage() {
    echo "Usage: $0 start"
    echo "       $0 stop"
    echo "       $0 restart"
    echo "       $0 list"
    echo "       $0 status  zk-host zk-port"
    echo "       $0 client  zk-host zk-port"
    exit
}

start() {
	bin/zkServer.sh start
	PID=`ps aux | grep -w $name | grep "zoo.cfg" | grep -v grep | awk '{print $2}'`
    if [ "$PID" != "" ];
    then
        for p in $PID
        do
            echo "start $name ( pid =" $p ")"
        done
    fi
}

stop() {
	PID=`ps aux | grep -w $name | grep "zoo.cfg" | grep -v grep | awk '{print $2}'`
    if [ "$PID" != "" ];
    then
        for ps in $PID
        do
            echo "bin/zkServer.sh stop $name ( pid =" $ps ")"
        done
    fi
	bin/zkServer.sh stop
}

list() {
	PID=`ps aux | grep -w $name | grep -v grep | awk '{printf("%s,%s,%s,%s\n", $1, $2, $9, $10)}'`
	if [ "$PID" != "" ];
	then
		echo "list ${name} $role"
		echo "index: user, pid, start, duration"
		idx=0
		for ps in $PID
		do
			echo "$idx: $ps"
			((idx ++))
		done
	fi
}

status() {
	local host=$1
	local port=$2
	echo "-----------------jps-------------------"
	jps
	echo "-----------------stat------------------"
	echo stat | nc $host $port
	echo "-----------------status----------------"
	bin/zkServer.sh status
}

client() {
	local host=$1
	local port=$2
	sh bin/zkCli.sh -server $host:$port
}

opt=$1
case C"$opt" in
    Cstart)
		start
        ;;
    Cstop)
		stop
        ;;
    Crestart)
		stop
		start
        ;;
    Clist)
		list
        ;;
	Cstatus)
		if [ $# != 3 ]; then
			usage
		fi
		status $2 $3
        ;;
    Cclient)
		if [ $# != 3 ]; then
			usage
		fi
		client $2 $3
        ;;
    C*)
        usage
        ;;
esac

