package com.zhenghang;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocol.proto.XAttrProtos;
import org.apache.hadoop.hdfs.server.namenode.*;
import org.apache.hadoop.hdfs.tools.offlineImageViewer.IgnoreSnapshotException;
import org.apache.hadoop.hdfs.util.XMLUtils;
import org.apache.hadoop.util.LimitInputStream;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

import static org.apache.hadoop.fs.QuotaUsage.getHeader;
import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.*;
import static org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode.XATTR_NAME_OFFSET;

public class MyImageWritter {

    private static final Logger LOG =
            LoggerFactory.getLogger(MyImageWritter.class);

    public static final String NAME_SECTION_NAME = "NameSection";
    public static final String INODE_SECTION_NAME = "INodeSection";
    public static final String SECRET_MANAGER_SECTION_NAME =
            "SecretManagerSection";
    public static final String CACHE_MANAGER_SECTION_NAME = "CacheManagerSection";
    public static final String SNAPSHOT_DIFF_SECTION_NAME = "SnapshotDiffSection";
    public static final String INODE_REFERENCE_SECTION_NAME =
            "INodeReferenceSection";
    public static final String INODE_DIRECTORY_SECTION_NAME =
            "INodeDirectorySection";
    public static final String FILE_UNDER_CONSTRUCTION_SECTION_NAME =
            "FileUnderConstructionSection";
    public static final String SNAPSHOT_SECTION_NAME = "SnapshotSection";

    public static final String SECTION_ID = "id";
    public static final String SECTION_REPLICATION = "replication";
    public static final String SECTION_PATH = "path";
    public static final String SECTION_NAME = "name";

    public static final String NAME_SECTION_NAMESPACE_ID = "namespaceId";
    public static final String NAME_SECTION_GENSTAMPV1 = "genstampV1";
    public static final String NAME_SECTION_GENSTAMPV2 = "genstampV2";
    public static final String NAME_SECTION_GENSTAMPV1_LIMIT = "genstampV1Limit";
    public static final String NAME_SECTION_LAST_ALLOCATED_BLOCK_ID =
            "lastAllocatedBlockId";
    public static final String NAME_SECTION_TXID = "txid";
    public static final String NAME_SECTION_ROLLING_UPGRADE_START_TIME =
            "rollingUpgradeStartTime";
    public static final String NAME_SECTION_LAST_ALLOCATED_STRIPED_BLOCK_ID =
            "lastAllocatedStripedBlockId";

    public static final String INODE_SECTION_LAST_INODE_ID = "lastInodeId";
    public static final String INODE_SECTION_NUM_INODES = "numInodes";
    public static final String INODE_SECTION_TYPE = "type";
    public static final String INODE_SECTION_MTIME = "mtime";
    public static final String INODE_SECTION_ATIME = "atime";
    public static final String INODE_SECTION_PREFERRED_BLOCK_SIZE =
            "preferredBlockSize";
    public static final String INODE_SECTION_PERMISSION = "permission";
    public static final String INODE_SECTION_BLOCKS = "blocks";
    public static final String INODE_SECTION_BLOCK = "block";
    public static final String INODE_SECTION_GENSTAMP = "genstamp";
    public static final String INODE_SECTION_NUM_BYTES = "numBytes";
    public static final String INODE_SECTION_FILE_UNDER_CONSTRUCTION =
            "file-under-construction";
    public static final String INODE_SECTION_CLIENT_NAME = "clientName";
    public static final String INODE_SECTION_CLIENT_MACHINE = "clientMachine";
    public static final String INODE_SECTION_ACL = "acl";
    public static final String INODE_SECTION_ACLS = "acls";
    public static final String INODE_SECTION_XATTR = "xattr";
    public static final String INODE_SECTION_XATTRS = "xattrs";
    public static final String INODE_SECTION_STORAGE_POLICY_ID =
            "storagePolicyId";
    public static final String INODE_SECTION_NS_QUOTA = "nsquota";
    public static final String INODE_SECTION_DS_QUOTA = "dsquota";
    public static final String INODE_SECTION_TYPE_QUOTA = "typeQuota";
    public static final String INODE_SECTION_QUOTA = "quota";
    public static final String INODE_SECTION_TARGET = "target";
    public static final String INODE_SECTION_NS = "ns";
    public static final String INODE_SECTION_VAL = "val";
    public static final String INODE_SECTION_VAL_HEX = "valHex";
    public static final String INODE_SECTION_INODE = "inode";

    private final PrintStream out;
    private final Configuration conf;
    private SerialNumberManager.StringTable stringTable;
    private MetadataMap metadataMap;

    public MyImageWritter(PrintStream out, Configuration conf) {
        this.out = out;
        this.conf = conf;
        this.metadataMap = new InMemoryMetadataDB();
    }

    public void visit(RandomAccessFile file) throws IOException {

        FsImageProto.FileSummary summary = FSImageUtil.loadSummary(file);

        try (FileInputStream fin = new FileInputStream(file.getFD())) {
            ArrayList<FsImageProto.FileSummary.Section> sectionsList = Lists.newArrayList(summary
                    .getSectionsList());

            Collections.sort(sectionsList, new Comparator<FsImageProto.FileSummary.Section>() {
                @Override
                public int compare(FsImageProto.FileSummary.Section s1, FsImageProto.FileSummary.Section s2) {
                    FSImageFormatProtobuf.SectionName n1 = FSImageFormatProtobuf.SectionName.fromString(s1.getName());
                    FSImageFormatProtobuf.SectionName n2 = FSImageFormatProtobuf.SectionName.fromString(s2.getName());
                    if (n1 == null) {
                        return n2 == null ? 0 : -1;
                    } else if (n2 == null) {
                        return -1;
                    } else {
                        return n1.ordinal() - n2.ordinal();
                    }
                }
            });
            ImmutableList<Long> refIdList = null;
            InputStream is;
            for (FsImageProto.FileSummary.Section s : sectionsList) {
                fin.getChannel().position(s.getOffset());
                is = FSImageUtil.wrapInputStreamForCompression(conf,
                        summary.getCodec(), new BufferedInputStream(new LimitInputStream(
                                fin, s.getLength())));

                switch (FSImageFormatProtobuf.SectionName.fromString(s.getName())) {
                    case STRING_TABLE:
                        loadStringTable(is);
                        break;
                     //show blockID
                    case INODE:
                        dumpINodeSection(is);
                        break;
                    //show path
                    case INODE_REFERENCE:
                        // Load INodeReference so that all INodes can be processed.
                        // Snapshots are not handled and will just be ignored for now.
                        LOG.info("Loading inode references");
                        refIdList = FSImageLoader.loadINodeReferenceSection(is);
                        break;
                    default:
                        break;
                }
//                System.out.println(" ");
//                System.out.println("name=" + s.getName());
//                System.out.println("length=" + s.getLength());
//                System.out.println("offset=" + s.getOffset());
            }
            showPath(fin,sectionsList,summary,refIdList);
        }

    }

    public void showPath(FileInputStream fin, ArrayList<FsImageProto.FileSummary.Section> sectionsList,
                        FsImageProto.FileSummary summary, ImmutableList<Long> refIdList) throws IOException {
        loadDirectories(fin, sectionsList, summary, conf);
        loadINodeDirSection(fin, sectionsList, summary, conf, refIdList);
        metadataMap.sync();
        output(conf, summary, fin, sectionsList);
    }

    /**
     * Load the directories in the INode section.
     */
    private void loadDirectories(
            FileInputStream fin, List<FsImageProto.FileSummary.Section> sections,
            FsImageProto.FileSummary summary, Configuration conf)
            throws IOException {
        LOG.info("Loading directories");
        long startTime = Time.monotonicNow();
        for (FsImageProto.FileSummary.Section section : sections) {
            if (FSImageFormatProtobuf.SectionName.fromString(section.getName())
                    == FSImageFormatProtobuf.SectionName.INODE) {
                fin.getChannel().position(section.getOffset());
                InputStream is = FSImageUtil.wrapInputStreamForCompression(conf,
                        summary.getCodec(), new BufferedInputStream(new LimitInputStream(
                                fin, section.getLength())));
                loadDirectoriesInINodeSection(is);
            }
        }
        long timeTaken = Time.monotonicNow() - startTime;
        LOG.info("Finished loading directories in {}ms", timeTaken);
    }

    private void loadDirectoriesInINodeSection(InputStream in) throws IOException {
        FsImageProto.INodeSection s = FsImageProto.INodeSection.parseDelimitedFrom(in);
        LOG.info("Loading directories in INode section.");
        int numDirs = 0;
        for (int i = 0; i < s.getNumInodes(); ++i) {
            FsImageProto.INodeSection.INode p = FsImageProto.INodeSection.INode.parseDelimitedFrom(in);
            if (LOG.isDebugEnabled() && i % 10000 == 0) {
                LOG.debug("Scanned {} inodes.", i);
            }
            if (p.hasDirectory()) {
                metadataMap.putDir(p);
                numDirs++;
            }
        }
        LOG.info("Found {} directories in INode section.", numDirs);
    }

    private void loadINodeDirSection(
            FileInputStream fin, List<FsImageProto.FileSummary.Section> sections,
            FsImageProto.FileSummary summary, Configuration conf, List<Long> refIdList)
            throws IOException {
        LOG.info("Loading INode directory section.");
        long startTime = Time.monotonicNow();
        for (FsImageProto.FileSummary.Section section : sections) {
            if (FSImageFormatProtobuf.SectionName.fromString(section.getName())
                    == FSImageFormatProtobuf.SectionName.INODE_DIR) {
                fin.getChannel().position(section.getOffset());
                InputStream is = FSImageUtil.wrapInputStreamForCompression(conf,
                        summary.getCodec(), new BufferedInputStream(
                                new LimitInputStream(fin, section.getLength())));
                buildNamespace(is, refIdList);
            }
        }
        long timeTaken = Time.monotonicNow() - startTime;
        LOG.info("Finished loading INode directory section in {}ms", timeTaken);
    }

    private void buildNamespace(InputStream in, List<Long> refIdList)
            throws IOException {
        int count = 0;
        while (true) {
            FsImageProto.INodeDirectorySection.DirEntry e =
                    FsImageProto.INodeDirectorySection.DirEntry.parseDelimitedFrom(in);
            if (e == null) {
                break;
            }
            count++;
            if (LOG.isDebugEnabled() && count % 10000 == 0) {
                LOG.debug("Scanned {} directories.", count);
            }
            long parentId = e.getParent();
            for (int i = 0; i < e.getChildrenCount(); i++) {
                long childId = e.getChildren(i);
                metadataMap.putDirChild(parentId, childId);
            }
            for (int i = e.getChildrenCount();
                 i < e.getChildrenCount() + e.getRefChildrenCount(); i++) {
                int refId = e.getRefChildren(i - e.getChildrenCount());
                metadataMap.putDirChild(parentId, refIdList.get(refId));
            }
        }
        LOG.info("Scanned {} INode directories to build namespace.", count);
    }

    private void output(Configuration conf, FsImageProto.FileSummary summary,
                        FileInputStream fin, ArrayList<FsImageProto.FileSummary.Section> sections)
            throws IOException {
        InputStream is;
        long startTime = Time.monotonicNow();
//        out.println(getHeader());
        for (FsImageProto.FileSummary.Section section : sections) {
            if (FSImageFormatProtobuf.SectionName.fromString(section.getName()) == FSImageFormatProtobuf.SectionName.INODE) {
                fin.getChannel().position(section.getOffset());
                is = FSImageUtil.wrapInputStreamForCompression(conf,
                        summary.getCodec(), new BufferedInputStream(new LimitInputStream(
                                fin, section.getLength())));
                outputINodes(is);
            }
        }
        long timeTaken = Time.monotonicNow() - startTime;
        LOG.debug("Time to output inodes: {}ms", timeTaken);
    }


    private void outputINodes(InputStream in) throws IOException {
        FsImageProto.INodeSection s = FsImageProto.INodeSection.parseDelimitedFrom(in);
        LOG.info("Found {} INodes in the INode section", s.getNumInodes());
        long ignored = 0;
        long ignoredSnapshots = 0;
        for (int i = 0; i < s.getNumInodes(); ++i) {
            FsImageProto.INodeSection.INode p = FsImageProto.INodeSection.INode.parseDelimitedFrom(in);
            try {
                StringBuilder sb = new StringBuilder();
                String parentPath = metadataMap.getParentPath(p.getId());
//                out.println(getEntry(parentPath, p));

//                System.out.println(getEntry(parentPath, p));
                if (p.hasFile()) {
                    sb.append("path: " + getEntry(parentPath, p)+ " blockIds:{");
                    FsImageProto.INodeSection.INodeFile f = p.getFile();
                    for (HdfsProtos.BlockProto b : f.getBlocksList()) {
                        sb.append("blk_" + b.getBlockId() + ",");
                    }
                    sb.append("}");
                } else {
                    sb.append("path: " + getEntry(parentPath, p));
                }
                System.out.println(sb.toString());
            } catch (IOException ioe) {
                ignored++;
                if (!(ioe instanceof IgnoreSnapshotException)) {
                    LOG.warn("Exception caught, ignoring node:{}", p.getId(), ioe);
                } else {
                    ignoredSnapshots++;
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Exception caught, ignoring node:{}.", p.getId(), ioe);
                    }
                }
            }

            if (LOG.isDebugEnabled() && i % 100000 == 0) {
                LOG.debug("Outputted {} INodes.", i);
            }
        }
        if (ignored > 0) {
            LOG.warn("Ignored {} nodes, including {} in snapshots. Please turn on"
                    + " debug log for details", ignored, ignoredSnapshots);
        }
        LOG.info("Outputted {} INodes.", s.getNumInodes());
    }

    private void dumpINodeSection(InputStream in) throws IOException {
        FsImageProto.INodeSection s = FsImageProto.INodeSection.parseDelimitedFrom(in);
        out.print("<" + INODE_SECTION_NAME + ">");
        o(INODE_SECTION_LAST_INODE_ID, s.getLastInodeId());
        o(INODE_SECTION_NUM_INODES, s.getNumInodes());
        for (int i = 0; i < s.getNumInodes(); ++i) {
            FsImageProto.INodeSection.INode p = FsImageProto.INodeSection.INode.parseDelimitedFrom(in);
            out.print("<" + INODE_SECTION_INODE + ">");
            dumpINodeFields(p);
            out.print("</" + INODE_SECTION_INODE + ">\n");
        }
        out.print("</" + INODE_SECTION_NAME + ">\n");
    }

    private MyImageWritter o(final String e, final Object v) {
        if (v instanceof Boolean) {
            // For booleans, the presence of the element indicates true, and its
            // absence indicates false.
            if ((Boolean) v != false) {
                out.print("<" + e + "/>");
            }
            return this;
        }
        out.print("<" + e + ">" +
                XMLUtils.mangleXmlString(v.toString(), true) + "</" + e + ">");
        return this;
    }

    private void dumpINodeFields(FsImageProto.INodeSection.INode p) {
        o(SECTION_ID, p.getId()).o(INODE_SECTION_TYPE, p.getType())
                .o(SECTION_NAME, p.getName().toStringUtf8());
        if (p.hasFile()) {
            dumpINodeFile(p.getFile());
        } else if (p.hasDirectory()) {
            dumpINodeDirectory(p.getDirectory());
        } else if (p.hasSymlink()) {
            dumpINodeSymlink(p.getSymlink());
        }
    }

    private void dumpINodeFile(FsImageProto.INodeSection.INodeFile f) {
        o(SECTION_REPLICATION, f.getReplication())
                .o(INODE_SECTION_MTIME, f.getModificationTime())
                .o(INODE_SECTION_ATIME, f.getAccessTime())
                .o(INODE_SECTION_PREFERRED_BLOCK_SIZE, f.getPreferredBlockSize())
                .o(INODE_SECTION_PERMISSION, dumpPermission(f.getPermission()));
        if (f.hasXAttrs()) {
            dumpXattrs(f.getXAttrs());
        }
        dumpAcls(f.getAcl());
        if (f.getBlocksCount() > 0) {
            out.print("<" + INODE_SECTION_BLOCKS + ">");
            for (HdfsProtos.BlockProto b : f.getBlocksList()) {
                out.print("<" + INODE_SECTION_BLOCK + ">");
                o(SECTION_ID, b.getBlockId())
                        .o(INODE_SECTION_GENSTAMP, b.getGenStamp())
                        .o(INODE_SECTION_NUM_BYTES, b.getNumBytes());
//                System.out.println(b.getBlockId());
                out.print("</" + INODE_SECTION_BLOCK + ">\n");
            }
            out.print("</" + INODE_SECTION_BLOCKS + ">\n");
        }
        if (f.hasStoragePolicyID()) {
            o(INODE_SECTION_STORAGE_POLICY_ID, f.getStoragePolicyID());
        }

        if (f.hasFileUC()) {
            FsImageProto.INodeSection.FileUnderConstructionFeature u = f.getFileUC();
            out.print("<" + INODE_SECTION_FILE_UNDER_CONSTRUCTION + ">");
            o(INODE_SECTION_CLIENT_NAME, u.getClientName())
                    .o(INODE_SECTION_CLIENT_MACHINE, u.getClientMachine());
            out.print("</" + INODE_SECTION_FILE_UNDER_CONSTRUCTION + ">\n");
        }
    }

    private void dumpINodeDirectory(FsImageProto.INodeSection.INodeDirectory d) {
        o(INODE_SECTION_MTIME, d.getModificationTime())
                .o(INODE_SECTION_PERMISSION, dumpPermission(d.getPermission()));
        if (d.hasXAttrs()) {
            dumpXattrs(d.getXAttrs());
        }
        dumpAcls(d.getAcl());
        if (d.hasDsQuota() && d.hasNsQuota()) {
            o(INODE_SECTION_NS_QUOTA, d.getNsQuota())
                    .o(INODE_SECTION_DS_QUOTA, d.getDsQuota());
        }
        FsImageProto.INodeSection.QuotaByStorageTypeFeatureProto typeQuotas =
                d.getTypeQuotas();
        if (typeQuotas != null) {
            for (FsImageProto.INodeSection.QuotaByStorageTypeEntryProto entry :
                    typeQuotas.getQuotasList()) {
                out.print("<" + INODE_SECTION_TYPE_QUOTA + ">");
                o(INODE_SECTION_TYPE, entry.getStorageType().toString());
                o(INODE_SECTION_QUOTA, entry.getQuota());
                out.print("</" + INODE_SECTION_TYPE_QUOTA + ">");
            }
        }
    }

    private void dumpINodeSymlink(FsImageProto.INodeSection.INodeSymlink s) {
        o(INODE_SECTION_PERMISSION, dumpPermission(s.getPermission()))
                .o(INODE_SECTION_TARGET, s.getTarget().toStringUtf8())
                .o(INODE_SECTION_MTIME, s.getModificationTime())
                .o(INODE_SECTION_ATIME, s.getAccessTime());
    }

    private String dumpPermission(long permission) {
        PermissionStatus permStatus = FSImageFormatPBINode.Loader.
                loadPermission(permission, stringTable);
        return String.format("%s:%s:%04o", permStatus.getUserName(),
                permStatus.getGroupName(), permStatus.getPermission().toExtendedShort());
    }

    private void dumpXattrs(FsImageProto.INodeSection.XAttrFeatureProto xattrs) {
        out.print("<" + INODE_SECTION_XATTRS + ">");
        for (FsImageProto.INodeSection.XAttrCompactProto xattr : xattrs.getXAttrsList()) {
            out.print("<" + INODE_SECTION_XATTR + ">");
            int encodedName = xattr.getName();
            int ns = (XATTR_NAMESPACE_MASK & (encodedName >> XATTR_NAMESPACE_OFFSET)) |
                    ((XATTR_NAMESPACE_EXT_MASK & (encodedName >> XATTR_NAMESPACE_EXT_OFFSET)) << 2);
            o(INODE_SECTION_NS, XAttrProtos.XAttrProto.
                    XAttrNamespaceProto.valueOf(ns).toString());
            o(SECTION_NAME, SerialNumberManager.XATTR.getString(
                    XATTR_NAME_MASK & (encodedName >> XATTR_NAME_OFFSET),
                    stringTable));
            ByteString val = xattr.getValue();
            if (val.isValidUtf8()) {
                o(INODE_SECTION_VAL, val.toStringUtf8());
            } else {
                o(INODE_SECTION_VAL_HEX, Hex.encodeHexString(val.toByteArray()));
            }
            out.print("</" + INODE_SECTION_XATTR + ">");
        }
        out.print("</" + INODE_SECTION_XATTRS + ">");
    }

    private void dumpAcls(FsImageProto.INodeSection.AclFeatureProto aclFeatureProto) {
        ImmutableList<AclEntry> aclEntryList = FSImageFormatPBINode.Loader
                .loadAclEntries(aclFeatureProto, stringTable);
        if (aclEntryList.size() > 0) {
            out.print("<" + INODE_SECTION_ACLS + ">");
            for (AclEntry aclEntry : aclEntryList) {
                o(INODE_SECTION_ACL, aclEntry.toString());
            }
            out.print("</" + INODE_SECTION_ACLS + ">");
        }
    }

    private void loadStringTable(InputStream in) throws IOException {
        stringTable = FSImageLoader.loadStringTable(in);
    }

    public String getEntry(String parent, FsImageProto.INodeSection.INode inode) {
        StringBuffer buffer = new StringBuffer();
        String inodeName = inode.getName().toStringUtf8();
        Path path = new Path(parent.isEmpty() ? "/" : parent,
                inodeName.isEmpty() ? "/" : inodeName);
        return path.toString();
    }

    private static interface MetadataMap extends Closeable {
        /**
         * Associate an inode with its parent directory.
         */
        public void putDirChild(long parentId, long childId) throws IOException;

        /**
         * Associate a directory with its inode Id.
         */
        public void putDir(FsImageProto.INodeSection.INode dir) throws IOException;

        /**
         * Get the full path of the parent directory for the given inode.
         */
        public String getParentPath(long inode) throws IOException;

        /**
         * Synchronize metadata to persistent storage, if possible
         */
        public void sync() throws IOException;
    }

    /**
     * Maintain all the metadata in memory.
     */
    private static class InMemoryMetadataDB implements MetadataMap {
        /**
         * Represent a directory in memory.
         */
        private static class Dir {
            private final long inode;
            private InMemoryMetadataDB.Dir parent = null;
            private String name;
            private String path = null;  // cached full path of the directory.

            Dir(long inode, String name) {
                this.inode = inode;
                this.name = name;
            }

            private void setParent(InMemoryMetadataDB.Dir parent) {
                Preconditions.checkState(this.parent == null);
                this.parent = parent;
            }

            /**
             * Returns the full path of this directory.
             */
            private String getPath() {
                if (this.parent == null) {
                    return "/";
                }
                if (this.path == null) {
                    this.path = new Path(parent.getPath(), name.isEmpty() ? "/" : name).
                            toString();
                    this.name = null;
                }
                return this.path;
            }

            @Override
            public boolean equals(Object o) {
                return o instanceof InMemoryMetadataDB.Dir && inode == ((InMemoryMetadataDB.Dir) o).inode;
            }

            @Override
            public int hashCode() {
                return Long.valueOf(inode).hashCode();
            }
        }

        /**
         * INode Id to Dir object mapping
         */
        private Map<Long, InMemoryMetadataDB.Dir> dirMap = new HashMap<>();

        /**
         * Children to parent directory INode ID mapping.
         */
        private Map<Long, InMemoryMetadataDB.Dir> dirChildMap = new HashMap<>();

        InMemoryMetadataDB() {
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public void putDirChild(long parentId, long childId) {
            InMemoryMetadataDB.Dir parent = dirMap.get(parentId);
            InMemoryMetadataDB.Dir child = dirMap.get(childId);
            if (child != null) {
                child.setParent(parent);
            }
            Preconditions.checkState(!dirChildMap.containsKey(childId));
            dirChildMap.put(childId, parent);
        }

        @Override
        public void putDir(FsImageProto.INodeSection.INode p) {
            Preconditions.checkState(!dirMap.containsKey(p.getId()));
            InMemoryMetadataDB.Dir dir = new InMemoryMetadataDB.Dir(p.getId(), p.getName().toStringUtf8());
            dirMap.put(p.getId(), dir);
        }

        @Override
        public String getParentPath(long inode) throws IOException {
            if (inode == INodeId.ROOT_INODE_ID) {
                return "";
            }
            InMemoryMetadataDB.Dir parent = dirChildMap.get(inode);
            if (parent == null) {
                // The inode is an INodeReference, which is generated from snapshot.
                // For delimited oiv tool, no need to print out metadata in snapshots.
//                ignoreSnapshotName(inode);
            }
            return parent.getPath();
        }

        @Override
        public void sync() {
        }
    }
}
