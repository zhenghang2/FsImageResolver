package com.zhenghang;

import com.google.common.collect.ImmutableList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto;
import org.apache.hadoop.hdfs.server.namenode.SerialNumberManager;

import java.io.IOException;
import java.io.InputStream;

public class FSImageLoader {

    public static final Log LOG = LogFactory.getLog(FSImageLoader.class);


    static SerialNumberManager.StringTable loadStringTable(InputStream in)
            throws IOException {
        FsImageProto.StringTableSection s = FsImageProto.StringTableSection
                .parseDelimitedFrom(in);
        LOG.info("Loading " + s.getNumEntry() + " strings");
        SerialNumberManager.StringTable stringTable =
                SerialNumberManager.newStringTable(s.getNumEntry(), s.getMaskBits());
        for (int i = 0; i < s.getNumEntry(); ++i) {
            FsImageProto.StringTableSection.Entry e = FsImageProto
                    .StringTableSection.Entry.parseDelimitedFrom(in);
            stringTable.put(e.getId(), e.getStr());
        }
        return stringTable;
    }

    static ImmutableList<Long> loadINodeReferenceSection(InputStream in)
            throws IOException {
        LOG.info("Loading inode references");
        ImmutableList.Builder<Long> builder = ImmutableList.builder();
        long counter = 0;
        while (true) {
            FsImageProto.INodeReferenceSection.INodeReference e =
                    FsImageProto.INodeReferenceSection.INodeReference
                            .parseDelimitedFrom(in);
            if (e == null) {
                break;
            }
            ++counter;
            builder.add(e.getReferredId());
        }
        LOG.info("Loaded " + counter + " inode references");
        return builder.build();
    }

}
