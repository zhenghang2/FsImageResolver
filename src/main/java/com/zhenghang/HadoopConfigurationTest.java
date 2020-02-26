package com.zhenghang;

import com.zhenghang.fsck.DFSck;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Collection;

public class HadoopConfigurationTest {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path path = new Path("/home/zhenghang/hadoop-current/etc/hadoop/core-site.xml");

        HdfsConfiguration hdfsconf = new HdfsConfiguration();

        hdfsconf.addResource(path);

        int res = ToolRunner.run(new DFSck(hdfsconf),args);

    }
}
