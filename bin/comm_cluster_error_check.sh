#!/usr/bin/env bash
login_user=???

slave_num=???
thread_num=???
echo "slave num:${slave_num}, thread num:${thread_num}"

# *master host
master_host=$(hostname)
# *master port
master_port=61235
echo "master host:${master_host}, master port:${master_port}"


# multi slaves split with space
slave_hosts=(? ? ?)
slave_main_path=???
echo "slave hosts:${slave_hosts}"
echo "slave main path:${slave_main_path}"

arr_size=10000000
obj_size=1000
run_time=3
# mode=process or thread
mode="process"
compress=false
test_rpc=false

# kill old task
echo "kill old task..."
kill $(cat master_${master_port}.pid)
sh "kill_"${master_port}".sh"


# copy lib and config
echo "copy lib and config..."
for slave_host in ${slave_hosts[@]}
do
    ssh ${login_user}@${slave_host} "mkdir "${slave_main_path}
    ssh ${login_user}@${slave_host} "mkdir "${slave_main_path}"/log"
    ssh ${login_user}@${slave_host} "rm -rf "${slave_main_path}"/log/*"
    ssh ${login_user}@${slave_host} "cd "${slave_main_path}"; kill $(cat slave_"${master_port}".pid)"
    scp -r lib ${login_user}@${slave_host}":"${slave_main_path}
    scp -r config ${login_user}@${slave_host}":"${slave_main_path}
    scp -r bin ${login_user}@${slave_host}":"${slave_main_path}
done

# start master
echo "start master..."
nohup java -server -Xmx600m -classpath .:lib/*:config -Dlog4j.configuration=file:config/log4j_master.properties com.fenbi.mp4j.comm.CommMaster ${slave_num} ${master_port} > log/master_startup.log 2>&1 & echo $! > master_${master_port}.pid

# start slaves
cmd="cd ${slave_main_path};nohup java -server -Xmx60000m -classpath .:lib/*:config -Dlog4j.configuration=file:config/log4j_slave.properties com.fenbi.mp4j.check.CommCheckTool ${login_user} ${master_host} ${master_port} ${arr_size} ${obj_size} ${run_time} ${thread_num} ${mode} ${compress} ${test_rpc} > log/slave_startup.log 2>&1 & echo \$! > slave_${master_port}.pid"

echo "cmd:${cmd}"

echo "start slavers..."
for slave_host in ${slave_hosts[@]}
do
    ssh ${login_user}@${slave_host} "${cmd}"
done
