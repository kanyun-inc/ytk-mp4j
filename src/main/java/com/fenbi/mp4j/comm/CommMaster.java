/**
*
* Copyright (c) 2017 ytk-mp4j https://github.com/yuantiku
*
* Permission is hereby granted, free of charge, to any person obtaining a copy
* of this software and associated documentation files (the "Software"), to deal
* in the Software without restriction, including without limitation the rights
* to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
* copies of the Software, and to permit persons to whom the Software is
* furnished to do so, subject to the following conditions:

* The above copyright notice and this permission notice shall be included in all
* copies or substantial portions of the Software.

* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
* AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
* OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
* SOFTWARE.
*/

package com.fenbi.mp4j.comm;

import com.fenbi.mp4j.rpc.IServer;
import com.fenbi.mp4j.rpc.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Rpc master server. usually used as stand-along program.
 * @author xialong
 */
public class CommMaster {

    public static final Logger LOG = LoggerFactory.getLogger(CommMaster.class);

    private RPC.Server rpcServer = null;

    private final Server server;

    private final int rpcServerPort;

    private final int slaveNum;

    public CommMaster(int nSlave, int rpcPort) {
        rpcServerPort = rpcPort;
        slaveNum = nSlave;
        server = new Server(nSlave, rpcPort);

        LOG.info("slave num:" + nSlave + ", port:" + rpcPort);
    }


    /**
     * start rpc server
     * 
     * @throws java.io.IOException
     */
    public void start() throws IOException {
        rpcServer = new RPC.Builder(new Configuration()).setProtocol(IServer.class)
                .setInstance(server).setBindAddress(InetAddress.getLocalHost().getHostAddress()).setPort(rpcServerPort)
                .setNumHandlers(slaveNum * 2).build();
        rpcServer.start();
        LOG.info("rpc server started!, rpcport=" + rpcServer.getPort());
    }

    /**
     * stop rpc server
     */
    public int stop() {
        int code = ((Server) server).stop();

        if (rpcServer != null) {
            rpcServer.stop();
        }

        return code;
    }

    /**
     * get host name
     * @return
     * @throws java.net.UnknownHostException
     */
    public String getHostName() throws UnknownHostException {
        InetAddress ip = InetAddress.getLocalHost();
        return ip.getHostName();
    }

    /**
     * get host port
     * @return
     */
    public int getHostPort() {
        return rpcServerPort;
    }

    public static void main(String[] args) {

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
    }
}
