package com.ruiyuan.upspark.utils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.hdfs.server.namenode.ha.proto.HAZKInfoProtos;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

public class ZkUtils {

    //    public static void main(String[] args) {
//
//        try {
//            ZkUtils zu = new ZkUtils();
////            zu.updateOffset("abc",1,-1,-1);
////            System.out.println(zu.getOffset("abc",1)[1]);
//
//            System.out.println(zu.getActiveNamenode());
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//    }
    private CuratorFramework cur;
    String basePath = "/SparkAiEngine";
    String offsetBase = basePath + "/offset";

    private void connect() {

        if (null == cur || cur.getState().equals(CuratorFrameworkState.STOPPED)) {
            String zkserver = "storage1.bigdata:2181,storage2.bigdata:2181,storage3.bigdata:2181";
            cur = CuratorFrameworkFactory.newClient(zkserver, 5000, 3000,
                    new ExponentialBackoffRetry(1000, 3));
            cur.start();//连接
            System.out.println("zk connected");
        }
    }

    public String getActiveNamenode() {
        connect();
        try {
            String hadoopZkNode = "/hadoop-ha/nameservice1/ActiveStandbyElectorLock";
            byte[] bs = cur.getData().forPath(hadoopZkNode);
            HAZKInfoProtos.ActiveNodeInfo name = HAZKInfoProtos.ActiveNodeInfo.parseFrom(bs);
            return name.getHostname();
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        } finally {
//            close();
        }
    }

    public void updateOffset(String topic, int partition, long offset) {
        connect();

        StringBuilder sb1 = new StringBuilder();
        String offsetPath = sb1
                .append(offsetBase)
                .append("/" + topic)
                .append("/" + partition)
                .append("/offset")
                .toString();
        try {
            //update fromOffset
            Stat stat_fromOffset_path = cur.checkExists().forPath(offsetPath);
            if (null == stat_fromOffset_path) {
                System.out.println("fromOffset节点不存在，创建节点");
                cur.create().creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(offsetPath, String.valueOf(offset).getBytes());
            } else {
                cur.setData().forPath(offsetPath, String.valueOf(offset).getBytes());
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
//            close();
        }
    }

    public String getOffset(String topic, int partition) {
        connect();
        try {
            byte[] offset = cur.getData().forPath(offsetBase + "/" + topic + "/" + partition + "/offset");
            return new String(offset);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
//            close();
        }
    }

    public void updateMysqlCount(String UAVid, String Taskid, long count) {
        connect();

        StringBuilder sb1 = new StringBuilder();
        String offsetPath = sb1
                .append(basePath)
                .append("/imageCount")
                .append("/" + UAVid)
                .append("/" + Taskid)
                .append("/count")
                .toString();
        try {
            //update fromOffset
            Stat stat_fromOffset_path = cur.checkExists().forPath(offsetPath);
            if (null == stat_fromOffset_path) {
                System.out.println("fromOffset节点不存在，创建节点");
                cur.create().creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(offsetPath, String.valueOf(count).getBytes());
            } else {
                cur.setData().forPath(offsetPath, String.valueOf(count).getBytes());
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
//            close();
        }
    }

    public int getMysqlCount(String UAVid, String Taskid) {
        connect();
        try {
            byte[] offset = cur.getData().forPath(basePath + "/imageCount" + "/" + UAVid + "/" + Taskid + "/count");
            return Integer.parseInt(new String(offset));
        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        } finally {
//            close();
        }
    }

    public void close() {
        cur.close();
    }


}
