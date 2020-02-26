package com.zhenghang.editlog;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.apache.hadoop.hdfs.server.namenode.FSImage;

import java.io.IOException;
import java.util.Collection;

public class EditLogTailerThread extends Thread {
    public static final Log LOG = LogFactory.getLog(EditLogTailerThread.class);
    private FSEditLog editLog;

    private volatile boolean shouldRun = true;

    public EditLogTailerThread(FSEditLog editLog) {
        super("Edit log tailer");
        this.editLog = editLog;

    }

    private void setShouldRun(boolean shouldRun) {
        this.shouldRun = shouldRun;
    }

    @Override
    public void run() {
        doWork();
    }

    private void doWork() {
        while (shouldRun) {
            if (!shouldRun) {
                break;
            }
            doTailEdits();

            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                LOG.warn("Edit log tailer interrupted", e);
            }
        }
    }

    private void doTailEdits() {
        long lastTxnId = 123123123;

        if (LOG.isDebugEnabled()) {
            LOG.debug("lastTxnId: " + lastTxnId);
        }
        Collection<EditLogInputStream> streams;
        try {
            streams = editLog.selectInputStreams(lastTxnId + 1, 0, null, false);
        } catch (IOException ioe) {
            // This is acceptable. If we try to tail edits in the middle of an edits
            // log roll, i.e. the last one has been finalized but the new inprogress
            // edits file hasn't been started yet.
            LOG.warn("Edits tailer failed to find any streams. Will try again " +
                    "later.", ioe);
            return;
        }
        long editsLoaded = 0;
        loadEdits(streams,lastTxnId);
    }

    private void loadEdits(Collection<EditLogInputStream> streams, long lastTxnId) {
        EditLoader loader = new EditLoader(lastTxnId);

        for (EditLogInputStream editIn : streams) {
            LOG.info("Reading " + editIn + " expecting start txid #" +
                    (lastTxnId + 1));

            try {
                loader.loadFSEdits(editIn,lastTxnId+1);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
