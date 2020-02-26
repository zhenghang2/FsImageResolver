package com.zhenghang.editlog;

import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp;

import java.io.IOException;

public class EditLoader {
    private long lastAppliedTxId;

    public EditLoader(long lastAppliedTxId) {
        this.lastAppliedTxId = lastAppliedTxId;
    }

    public long loadFSEdits(EditLogInputStream edits, long expectedStartingTxId) throws IOException {
        try {
            long numEdits = loadEditRecords(edits,expectedStartingTxId);
            return numEdits;
        } finally {
            edits.close();
        }
    }

    private long loadEditRecords(EditLogInputStream edits, long expectedStartingTxId) throws IOException {
        long expectedTxId = expectedStartingTxId;
        long numEdits = 0;
        long lastTxId = edits.getLastTxId();
        long numTxns = (lastTxId - expectedStartingTxId) + 1;

        try {
            while (true) {
                try {
                    FSEditLogOp op;
                    op = edits.readOp();
                    if (op == null) {
                        break;
                    }
                    applyEditLogOp(op);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } finally {
            edits.close();
        }
        return numEdits;
    }
    //
    private void applyEditLogOp(FSEditLogOp op) {
        switch (op.opCode) {
            case OP_ADD:
                break;
            case OP_CLOSE:
                break;
            case OP_MKDIR:
                break;
            case OP_TIMES:
                break;
            case OP_APPEND:
                break;
            case OP_DELETE:
                break;
            case OP_RENAME:
                break;
            case OP_INVALID:
                break;
            case OP_SET_ACL:
                break;
            case OP_SYMLINK:
                break;
            case OP_TRUNCATE:
                break;
            case OP_ADD_BLOCK:
                break;
            default:
                break;
        }
    }
}
