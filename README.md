Ytk-mp4j is a fast, user-friendly, cross-platform, multi-process, multi-thread collective message passing java library for distributed machine learning. It's similar to MPI but it has several important features:

1. Supports multi-process, multi-thread message passing at the same time. So you needn't combine ```MPI``` with ```OpenMP``` to use multi-process, multi-thread. Serial codes can easily be paralleled and distributed.
2. Supports most primitive types and Java Object(```double, float, long, int, short, byte, String, Object```).
3. Supports any object message passing as long as you implement ```Serializer``` interface of [Kryo](https://github.com/EsotericSoftware/kryo).
4. Supports not only ```array``` data container, but also ```map``` container(used for sparse communication).
5. Using map container and Object type, you can realize complex operations(e.g. Set Intersection, Set Union,  List concat…).
6. Supports data compression that can can be used to reduce communication traffic.

Ytk-mp4j uses master-slave mode, only one master, one or more slaves(workers).  Responsibilities of master are: 

1. Coordinates and synchronizes all communication of slaves.
2. Receives logs from slaves and prints.

Communicative slaves are embedded in real workers,  which can lodge with any kinds of workers(e.g. reduce of **hadoop**,  executors of **spark**, …).

Ytk-mp4j implements state-of-art collective algorithms descrited in [1,2].

### Master Startup

You can use shell command below to start master directly. 

```shell
nohup java -server -Xmx600m -classpath .:lib/*:config -Dlog4j.configuration=file:config/log4j_master.properties com.fenbi.mp4j.comm.CommMaster ${slave_num} ${master_port} > log/master_startup.log 2>&1 & echo $! > master_${master_port}.pid
```

You can also assemble codes below into your project.

```java
CommMaster master = null;
try {
    int nWorker = Integer.parseInt(args[0]);
    int rpcPort = Integer.parseInt(args[1]);
    LOG.info("worker:" + nWorker);
    LOG.info("port:" + rpcPort);
    master = new CommMaster(nWorker, rpcPort);
    master.start();

    LOG.info("server hostname:" + master.getHostName());
    LOG.info("server hostport:" + master.getHostPort());
} catch (IOException e) {
    LOG.error("IO exception", e);
} finally {
    int code;
    if (master == null) {
        code = 1;
        LOG.error("master start error, failed!");
    } else {
        code = master.stop();
    }
    LOG.info("exit code:" + code);
    System.exit(code);
}  
```

### Usage of Slave


- In multi-process environment, use code template below to start multi-process communication: 

```java
int errorCode = 0;
ProcessCommSlave comm = null;
try {
    comm = new ProcessCommSlave(loginName, hostName, hostPort);
    // ...
    // array = comm.allreduceArray(array, ......)
    // ...

} catch (Exception e) {
    errorCode = 1;
    LOG.error("existed exception!", e);
    if (comm != null) {
        try {
            comm.exception(e);
        } catch (Mp4jException e1) {
            LOG.error("comm send exception message error!", e);
        }
    }
} finally {
    try {
        comm.close(errorCode);
    } catch (Mp4jException e) {
        errorCode = 1;
        LOG.error("comm close exception!", e);
    }
}

return errorCode == 0;
```

- In multi-process, multi-thread environment, use code template below to start multi-process, multi-thread communication(ThreadCommSlave supports multi-process and multi-thread，it contains all of functions of ProcessCommSlave, so you can just use ThreadCommSlave.): 

```java
int errorCode = 0;
ThreadCommSlave comm = null;
try {
    comm = new ThreadCommSlave(loginName, threadNum, hostName, hostPort);
    final ThreadCommSlave finalComm = comm;

    Thread []threads = new Thread[threadNum];
    for (int t = 0; t < threadNum; t++) {
        final int tidx = t;
        threads[t] = new Thread(t + "") {
            @Override
            public void run() {
                finalComm.setThreadId(tidx);
                try {
                    // ...
                    // array = comm.allreduceArray(array, ......)
				  // ...
                } catch (Exception e) {
                    try {
                        finalComm.exception(e);
                        finalComm.close(1);
                    } catch (Mp4jException e1) {
                        LOG.error("comm send exception message error!", e);
                    }
                    System.exit(1);
                }
            }
        };
        threads[t].start();
    }

    for (int t = 0; t < threadNum; t++) {
        threads[t].join();
    }
} catch (Exception e) {
    errorCode = 1;
    LOG.error("existed exception!", e);
    if (comm != null) {
        try {
            comm.exception(e);
        } catch (Mp4jException e1) {
            LOG.error("comm send exception message error!", e);
        }
    }
} finally {
    try {
        comm.close(errorCode);
    } catch (Mp4jException e) {
        errorCode = 1;
        LOG.error("comm close exception!", e);
    }
}

return errorCode == 0;
```

### Collective Communications

Collective communication is a method of communication which involves participation of all processes(all threads). Ytk-mp4j doesn't contain point-to-point communication(using collective comunication can realize it, but it is not recommended). A communication operation in ytk-mp4j usually contains an Operand. In reduction operation, it also contains a Operator.

##### Slaves

Each process will be assigned a special number—rank which is encoded with lexicographical order of hostname from 0 to slaveNum - 1. Use getRank() to get rank. Use getSlaveNum() to get the total number of processes.

##### Operand

Ytk-mp4j provides 8 predefined Operands, which almost meets all your needs. If you want to define a new Operand, you can extend ```Operand``` abstract class. ```compress``` is to decide whether the communication uses compression(default is false). To create a ObjectOperand, you must provide a ```serializer``` of Kryo and class type for your object. [Kryo](https://github.com/EsotericSoftware/kryo) is a fast and efficient object graph serialization framework for java. [ytk-learn](https://github.com/yuantiku/ytk-learn) uses Kryo to serialize and compress objects. More details about predefined Operands see [Operands](docs/ytk-mp4j-docs/index.html).

Using KryoUtils.getDefaultSerializer(Class type) function, you can get serializer easily for most simple objects. 

| Operand       | Support Java Types                       | How to Create?                           |
| :------------ | :--------------------------------------- | :--------------------------------------- |
| DoubleOperand | single double, double array, Double object | Operands.DOUBLE_OPERAND(boolean compress) |
| FloatOperand  | single float, double array, Float object | Operands.FLOAT_OPERAND(boolean compress) |
| LongOperand   | single long, long array, Long object     | Operands.LONG_OPERAND(boolean compress)  |
| IntOperand    | single int, int array, Integer object    | Operands.INT_OPERAND(boolean compress)   |
| ShortOperand  | single short, short array, Short object  | Operands.SHORT_OPERAND(boolean compress) |
| ByteOperand   | single byte, type array, Byte object     | Operands.BYTE_OPERAND(boolean compress)  |
| StringOperand | String object                            | Operands.STRING_OPERAND(boolean compress) |
| ObjectOperand | Object                                   | Operands.OBJECT_OPERAND(Serializer<T> serializer, Class type, boolean compress) |

##### Operator(reduction operation)

Different Operands support different Operators. Ytk-mp4j provides some predefined reduction operations.  The reduction operations should be commutative and associative. You can implement interface of different operands to get custom reduction operation.More details about predefined Operands see [Operators](docs/ytk-mp4j-docs/index.html).

| Operand       | Predefined Reduction IOperators          | Interface       |
| :------------ | :--------------------------------------- | :-------------- |
| DoubleOperand | Operators.Double.SUM/MAX/MIN/PROD/FLOAT_MAX_LOC/FLOAT_MIN_LOC | IDoubleOperator |
| FloatOperand  | Operators.Float.SUM/MAX/MIN/PROD         | IFloatOperator  |
| LongOperand   | Operators.Long.SUM/MAX/MIN/PROD/BITS_AND/BITS_OR/BITS_XOR/INT_MAX_LOC/INT_MIN_LOC | ILongOperator   |
| IntOperand    | Operators.Int.SUM/MAX/MIN/PROD/BITS_AND/BITS_OR/BITS_XOR | IIntOperator    |
| ShortOperand  | Operators.Short.SUM/MAX/MIN/PROD/BITS_AND/BITS_OR/BITS_XOR | IShortOperator  |
| ByteOperand   | Operators.Byte.SUM/MAX/MIN/PROD/BITS_AND/BITS_OR/BITS_XOR | IByteOperator   |
| StringOperand | null                                     | IStringOperator |
| ObjectOperand | null                                     | IObjectOperator |

##### Special Operators

Set union, intersection and List concat(partial) are commutative and associative. Those operations are often  used. More details see [ThreadCommSlave](docs/ytk-mp4j-docs/index.html), [ProcessCommSlave](docs/ytk-mp4j-docs/index.html).

##### Collective Operations

```gather```: Before the gather, each process node owns a piece of the data. After the gather, root process owns the entire data.

<p align="center"> <img src="docs/pics/gather.png" align="center" width="257" height="514" /></p>
<p align="center"> <img src="docs/pics/t_gather.png" align="center" width="517" height="802" /></p>
<p align="center"> <img src="docs/pics/map_gather.png" align="center" width="400" height="640" /></p>
<p align="center"> <img src="docs/pics/t_map_gather.png" align="center" width="550" height="568" /></p>

```allgather```: Before the allgather, each process node owns a piece of the data. After the allgather, all processes own all of the data.

<p align="center"> <img src="docs/pics/allgather.png" align="center" width="257" height="514" /></p>
<p align="center"> <img src="docs/pics/t_allgather.png" align="center" width="517" height="802" /></p>
<p align="center"> <img src="docs/pics/map_allgather.png" align="center" width="400" height="614" /></p>
<p align="center"> <img src="docs/pics/t_map_allgather.png" align="center" width="550" height="647" /></p>

```broadcast```: Before the broadcast, only root process owns entire data. After the broadcast, all processes own all of the data.

<p align="center"> <img src="docs/pics/broadcast.png" align="center" width="272" height="527" /></p>
<p align="center"> <img src="docs/pics/t_broadcast.png" align="center" width="517" height="802" /></p>
<p align="center"> <img src="docs/pics/map_broadcast.png" align="center" width="400" height="623" /></p>
<p align="center"> <img src="docs/pics/t_map_broadcast.png" align="center" width="550" height="568" /></p>

```scatter```: Before the scatter, only root process owns entire data. After scatter, each process owns a piece of the data.

<p align="center"> <img src="docs/pics/scatter.png" align="center" width="257" height="477" /></p>
<p align="center"> <img src="docs/pics/t_scatter.png" align="center" width="517" height="788" /></p>
<p align="center"> <img src="docs/pics/map_scatter.png" align="center" width="400" height="599" /></p>
<p align="center"> <img src="docs/pics/t_map_scatter.png" align="center" width="550" height="665" /></p>

```reduce```: Before the reduce, each process owns the data x(i). After the reduce, only root process owns the data of (x(0) + x(1) + … + x(p-1)). "+" is a general reduction operation.

<p align="center"> <img src="docs/pics/reduce.png" align="center" width="257" height="514" /></p>
<p align="center"> <img src="docs/pics/t_reduce.png" align="center" width="517" height="802" /></p>
<p align="center"> <img src="docs/pics/map_reduce.png" align="center" width="400" height="623" /></p>
<p align="center"> <img src="docs/pics/t_map_reduce.png" align="center" width="550" height="568" /></p>

```allreduce```: Identical to the reduce, except all the processes own the data of reduced.

<p align="center"> <img src="docs/pics/allreduce.png" align="center" width="257" height="514" /></p>
<p align="center"> <img src="docs/pics/t_allreduce.png" align="center" width="517" height="802" /></p>
<p align="center"> <img src="docs/pics/map_allreduce.png" align="center" width="400" height="623" /></p>
<p align="center"> <img src="docs/pics/t_map_allreduce.png" align="center" width="550" height="568" /></p>

```reducescatter```:  Identical to the ```reduce```, except each process owns a piece of reduced result.

<p align="center"> <img src="docs/pics/reduce-scatter.png" align="center" width="257" height="514" /></p>
<p align="center"> <img src="docs/pics/t_reduce-scatter.png" align="center" width="517" height="802" /></p>
<p align="center"> <img src="docs/pics/map_reduce-scatter.png" align="center" width="400" height="647" /></p>
<p align="center"> <img src="docs/pics/t_map_reduce-scatter.png" align="center" width="550" height="665" /></p>

##### Rpc-based allreduce

When the array size for allreduce is very small, using the allreduce algorithm in [1,2] may be not efficient, because most of time spend in network connection,  so rpc-based allreduce is better for this situation.

More details of collective operations, see  [ThreadCommSlave](docs/ytk-mp4j-docs/index.html), [ProcessCommSlave](process_comm_interface.md).

##### Container

Ytk-mp4j supports not only ```array``` data container in which data are arranged in different processes/threads in order, but also supports ```map``` data container which is more powerful and flexible. But it is not efficient, and the order of data in ```map``` data container will not be emphasized. In ThreadCommSlave, all threads in the same process shared the same result(reduce memory use and gc) when using ```map``` container, and if you want to modify in different threads, you must clone a duplicate ahead of modification.

##### Information

Master can receive information from slaves then print them out. Ytk-mp4j provides 4 different information-sending interfaces: info, debug, error, exception. More details see [ThreadCommSlave](docs/ytk-mp4j-docs/index.html), [ProcessCommSlave](process_comm_interface.md).

##### Barrier

ProcessCommSlave provides ```barrier``` interface to synchronize all processes. Likewise, ThreadCommSlave also provides ```barrier``` interface to synchronize all processes and all threads, and provides ```threadBarrier``` interface to synchronize all threads in special process.  

#### More Test Cases

In the package ```com.fenbi.mp4j.check```, almost all interfaces have a detailed use case. The bin/comm_cluster_error_check.sh is an integrated test script, and you can use it to test all interfaces.

### Applications

Your serial programs can be easily parallelized and distributed with ytk-mp4j.

[ytk-learn](https://github.com/yuantiku/ytk-learn) is a general machine learning library which uses ytk-mp4j to realize distributed version: 

1. Uses allreduce to aggregate the count of instances.
2. Uses allreduceMap to count frequency of occurrence of all features.
3. Uses allreduceArray/reduceScatterArray to realize distributed L-BFGS optimizer.
4. Uses allredueMap to realize distributed weighted approximate quantile.
5. Uses allreduceArray/reduceScatterArray ... to realize distributed GBDT.

  …..

### Communication Complexity

##### Complexity of Process Collective Communications（Using [GitHub with MathJax](https://chrome.google.com/webstore/detail/github-with-mathjax/ioemnmodlmafdkllaclgeombjnmnbima/related) to render Latex）

| Communication  | Connection                             | Transfer               | Computation            | Total                                    |
| -------------- | -------------------------------------- | ---------------------- | ---------------------- | ---------------------------------------- |
| gather         | $ \lfloor lg(p) \rfloor \alpha$        | $\frac{p-1}{p}n\beta$  | 0                      | $ \lfloor lg(p) \rfloor \alpha + \frac{p-1}{p}n\beta$ |
| scatter        | $ \lfloor lg(p) \rfloor \alpha$        | $\frac{p-1}{p}n\beta$  | 0                      | $ \lfloor lg(p) \rfloor \alpha + \frac{p-1}{p}n\beta$ |
| allgather      | $(p-1)\alpha$                          | $\frac{p-1}{p}n\beta$  | 0                      | $(p-1)\alpha + \frac{p-1}{p}n\beta$      |
| reduce-scatter | $(p-1)\alpha$                          | $\frac{p-1}{p}n\beta$  | $\frac{p-1}{p}n\gamma$ | $(p-1)\alpha + \frac{p-1}{p}n\beta +\frac{p-1}{p}n\gamma$ |
| broadcast      | $ (\lfloor lg(p) \rfloor + p-1)\alpha$ | $2\frac{p-1}{p}n\beta$ | 0                      | $ (\lfloor lg(p) \rfloor + p-1)\alpha + 2\frac{p-1}{p}n\beta $ |
| reduce         | $ (\lfloor lg(p) \rfloor + p-1)\alpha$ | $2\frac{p-1}{p}n\beta$ | $\frac{p-1}{p}n\gamma$ | $ (\lfloor lg(p) \rfloor + p-1)\alpha + 2\frac{p-1}{p}n\beta + \frac{p-1}{p}n\gamma $ |
| allreduce     | $2(p-1)\alpha$                         | $2\frac{p-1}{p}n\beta$ | $\frac{p-1}{p}n\gamma$ | $2(p-1)\alpha + 2\frac{p-1}{p}n\beta + \frac{p-1}{p}n\gamma $ |

where $\alpha$ is network connection latency, $\beta$ is transfer time per byte,  $n$ is the number of bytes transferred, $\gamma$ is the computation cost per byte for performing the reduction operation.

### Experiments

We test the costs of collective communications in ytk-mp4j with different double array sizes and numbers of slaves using a 1 Gigabit Ethernet in milliseconds.

**slaves(#2)**

|                | **gather** | **scatter** | **allgather** | **reduce-scatter** | **broadcast** | **reduce** | **allreduce** |
| -------------- | ---------- | ----------- | ------------- | ------------------ | ------------- | ---------- | ------------- |
| **100000**     | 6          | 6           | 6             | 7                  | 12            | 12         | 12            |
| **1000000**    | 48         | 43          | 45            | 38                 | 95            | 105        | 110           |
| **5000000**    | 190        | 185         | 210           | 230                | 410           | 400        | 430           |
| **10000000**   | 397        | 355         | 445           | 447                | 850           | 840        | 955           |
| **50000000**   | 1780       | 1751        | 2261          | 2508               | 4310          | 4482       | 4744          |
| **100000000**  | 3526       | 3555        | 4667          | 4611               | 8002          | 8061       | 9547          |
| **500000000**  | 17604      | 17659       | 22565         | 21375              | 40683         | 40323      | 46224         |
| **1000000000** | 34228      | 34464       | 47179         | 47361              | 80846         | 78442      | 93624         |

**slaves(#4)**

|                | **gather** | **scatter** | **allgather** | **reduce-scatter** | **broadcast** | **reduce** | **allreduce** |
| -------------- | ---------- | ----------- | ------------- | ------------------ | ------------- | ---------- | -------------
| **100000**     | 12    | 8     | 10    | 11    | 19     | 20     | 23     |
| **1000000**    | 57    | 56    | 63    | 65    | 118    | 121    | 130    |
| **5000000**    | 271   | 285   | 307   | 289   | 557    | 626    | 603    |
| **10000000**   | 538   | 534   | 630   | 618   | 1149   | 1237   | 1230   |
| **50000000**   | 2610  | 2599  | 3455  | 3616  | 6268   | 6124   | 6964   |
| **100000000**  | 5223  | 5266  | 6601  | 7322  | 11973  | 11894  | 13283  |
| **500000000**  | 25849 | 26390 | 33883 | 34759 | 60338  | 60016  | 63912  |
| **1000000000** | 51396 | 52517 | 65308 | 62918 | 117354 | 118999 | 129174 |

**slaves(#6)**

|                | **gather** | **scatter** | **allgather** | **reduce-scatter** | **broadcast** | **reduce** | **allreduce** |
| -------------- | ---------- | ----------- | ------------- | ------------------ | ------------- | ---------- | -------------
| **100000**     | 13    | 13    | 14    | 14    | 24     | 24     | 24     |
| **1000000**    | 62    | 62    | 75    | 84    | 155    | 146    | 159    |
| **5000000**    | 315   | 380   | 326   | 331   | 705    | 630    | 679    |
| **10000000**   | 602   | 762   | 681   | 731   | 1533   | 1561   | 1529   |
| **50000000**   | 2949  | 3530  | 3890  | 3846  | 7442   | 6837   | 7657   |
| **100000000**  | 5770  | 6934  | 7584  | 7408  | 14734  | 15791  | 15226  |
| **500000000**  | 28572 | 34762 | 37372 | 39021 | 72570  | 76837  | 77544  |
| **1000000000** | 56946 | 57042 | 74376 | 69005 | 140572 | 126827 | 143813 |

**slaves(#8)**

|                | **gather** | **scatter** | **allgather** | **reduce-scatter** | **broadcast** | **reduce** | **allreduce** |
| -------------- | ---------- | ----------- | ------------- | ------------------ | ------------- | ---------- | -------------
| **100000**     | 16    | 11    | 15    | 14    | 24     | 25     | 28     |
| **1000000**    | 69    | 67    | 92    | 85    | 152    | 155    | 183    |
| **5000000**    | 323   | 332   | 332   | 345   | 666    | 685    | 681    |
| **10000000**   | 645   | 648   | 741   | 749   | 1449   | 1431   | 1641   |
| **50000000**   | 3068  | 3048  | 4276  | 4459  | 7470   | 7595   | 8534   |
| **100000000**  | 6050  | 6143  | 8716  | 8931  | 14800  | 14561  | 16332  |
| **500000000**  | 30021 | 30479 | 40019 | 38916 | 70190  | 69255  | 77593  |
| **1000000000** | 59974 | 61426 | 75464 | 74495 | 136574 | 134260 | 154090 |



**All collective communications of 1e9 double array size**

<p align="center"> <img src="docs/pics/performance.png" align="center" width="619.74" height="753.72" /></p>

### Integration with Maven

```xml
<dependency>
  <groupId>com.fenbi.mp4j</groupId>
  <artifactId>ytk-mp4j</artifactId>
  <version>0.0.1</version>
</dependency>
```

### Reference

1. Thakur, Rajeev, Rolf Rabenseifner, and William Gropp. "Optimization of collective communication operations in MPICH." *The International Journal of High Performance Computing Applications* 19.1 (2005): 49-66.
2. Faraj, Ahmad, Pitch Patarasuk, and Xin Yuan. "Bandwidth efficient all-to-all broadcast on switched clusters." *Cluster Computing, 2005. IEEE International*. IEEE, 2005.
