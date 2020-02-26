package com.zhenghang;

import org.apache.hadoop.hdfs.server.namenode.FsImageProto;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;

public class TestImageUtil {

    public static void main(String[] args) {

    }

    public void testImage() {

        // 文件头字符串HDFSIMG1对应byte[]
        byte[] fileHead = "HDFSIMG1".getBytes();

        RandomAccessFile raFile = null;
        try {

            // 创建文件file，对应为f盘下FSImage文件fsimage_0000000000002311798
            File file = new File("f:/fsimage_0000000000002311798");

            raFile = new RandomAccessFile(file, "r");

            // 文件summary长度域所占大小为4
            final int FILE_LENGTH_FIELD_SIZE = 4;
            System.out.println("文件summary长度域大小：FILE_LENGTH_FIELD_SIZE=" + FILE_LENGTH_FIELD_SIZE);

            // 获取FSImage文件长度
            long fileLength = raFile.length();
            System.out.println("获取FSImage文件长度：fileLength=" + fileLength);

            // 创建文件头byte[]数组fileHeadTmp，用于存储文件头byte[]数组，大小为上述fileHead数组大小
            byte[] fileHeadTmp = new byte[fileHead.length];

            // 读入文件头至byte[]数组fileHeadTmp
            System.out.println("文件从头开始读取" + fileHeadTmp.length + "个byte至byte[]数组fileHeadTmp");
            raFile.readFully(fileHeadTmp);

            // 获取文件头长度
            System.out.println("获取文件头长度：fileHeadLength=" + fileHead.length);

            // 将byte[]数组fileHeadTmp转换成字符串fileHeadString
            String fileHeadString = new String(fileHeadTmp);
            // 验证文件头字符串
            System.out.println("fileHeadString=" + fileHeadString);

            // 文件file通过raFile.seek()方法定位到文件summary长度字段起始处，即文件大小减去文件summary长度域所占字节数4
            raFile.seek(fileLength - FILE_LENGTH_FIELD_SIZE);
            System.out.println("文件定位到文件summary长度开始处：" + (fileLength - FILE_LENGTH_FIELD_SIZE));

            // 读入一个int，即文件长度summaryLength
            int summaryLength = raFile.readInt();
            System.out.println("获取文件summary部分长度：summaryLength=" + summaryLength);

            // 文件file通过raFile.seek()方法定位到文件summary部分开始处，即文件大小减去文件长度所占字节数4，再减去文件内容总长度
            raFile.seek(fileLength - FILE_LENGTH_FIELD_SIZE - summaryLength);
            System.out.println("文件定位到文件summary部分开始处：" + (fileLength - FILE_LENGTH_FIELD_SIZE - summaryLength));

            // 再从当前位置开始读入文件summary部分内容
            // 构造文件长度summaryLength大小的byte[]数组
            byte[] summaryBytes = new byte[summaryLength];

            // 读取文件内容至数组summaryBytes
            raFile.readFully(summaryBytes);
            System.out.println("从当前位置开始读入文件summary部分内容至summaryBytes数组");

            FsImageProto.FileSummary summary = FsImageProto.FileSummary
                    .parseDelimitedFrom(new ByteArrayInputStream(summaryBytes));

            System.out.println("解析文件summary部分内容如下：");
            System.out.println("1、ondiskVersion=" + summary.getOndiskVersion());
            System.out.println("2、layoutVersion=" + summary.getLayoutVersion());
            System.out.println("3、codec=" + summary.getCodec());

            System.out.println("4、section");
            List<FsImageProto.FileSummary.Section> sectionsList = summary.getSectionsList();
            for (FsImageProto.FileSummary.Section section : sectionsList) {
                System.out.println(" ");
                System.out.println("name=" + section.getName());
                System.out.println("length=" + section.getLength());
                System.out.println("offset=" + section.getOffset());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (raFile != null) {
                try {
                    raFile.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    /**
     * Supported section name. The order of the enum determines the order of
     * loading.
     */
    public enum SectionName {
        NS_INFO("NS_INFO"), STRING_TABLE("STRING_TABLE"), EXTENDED_ACL(
                "EXTENDED_ACL"), INODE("INODE"), INODE_REFERENCE(
                "INODE_REFERENCE"), SNAPSHOT("SNAPSHOT"), INODE_DIR("INODE_DIR"), FILES_UNDERCONSTRUCTION(
                "FILES_UNDERCONSTRUCTION"), SNAPSHOT_DIFF("SNAPSHOT_DIFF"), SECRET_MANAGER(
                "SECRET_MANAGER"), CACHE_MANAGER("CACHE_MANAGER");

        private static final SectionName[] values = SectionName.values();

        public static SectionName fromString(String name) {
            for (SectionName n : values) {
                if (n.name.equals(name))
                    return n;
            }
            return null;
        }

        private final String name;

        private SectionName(String name) {
            this.name = name;
        }
    }

}
