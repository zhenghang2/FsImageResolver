package com.zhenghang.editlog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.ha.EditLogTailer;

import java.io.IOException;

public class EditLogService {

    private final FSImage fsImage;
    private Configuration conf;
    private final EditLogTailerThread editLogTailerThread;


    public EditLogService(Configuration conf) throws IOException {
        this.conf = conf;
        this.fsImage = new FSImage(conf);
        this.editLogTailerThread = new EditLogTailerThread(fsImage.getEditLog());
    }

    public void startStandbyServices() {
        editLogTailerThread.start();
    }
}
