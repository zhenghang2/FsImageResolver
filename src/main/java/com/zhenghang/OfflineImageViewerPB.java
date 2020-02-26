package com.zhenghang;

import org.apache.hadoop.conf.Configuration;

import java.io.*;

public class OfflineImageViewerPB {


    public static void main(String[] args) throws Exception {
        int status = run(args);
        System.exit(status);
    }

    private static int run(String[] args) throws IOException {
        String inputFile = "/home/zhenghang/bigdata/dfs/name/current/fsimage_0000000000000004794";
        String outputFile = "/home/zhenghang/fsimage2.xml";

        Configuration conf = new Configuration();

        try (PrintStream out = outputFile.equals("-") ?
                System.out : new PrintStream(outputFile, "UTF-8")) {
            new MyImageWritter(out,conf).visit(new RandomAccessFile(inputFile,
                    "r"));
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return -1;
    }
}
