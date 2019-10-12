package com.flow.hdfsutil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Scanner;


public class HdfsUtil {
    private final Log logger = LogFactory.getLog(HdfsUtil.class);
    FileSystem fs = null;

    public HdfsUtil() throws IOException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        //fs = FileSystem.get(conf);
        //conf.set("fs.defaultFS", "hdfs://hadoop101:9000");
        fs = FileSystem.get(new URI("hdfs://hadoop51:9000/"), conf, "gxm");
    }

    public void upload(String srcFile, String dstFile) throws IOException {
        Path dstPath = new Path(dstFile);
        Path srcPath = new Path(srcFile);
        if (!fs.exists(srcPath)) {
            logger.error("upload fail + srcFile: " + srcFile + " not exist");
            return;
        }

        fs.copyFromLocalFile(srcPath, dstPath);//????<---->???
//		FSDataOutputStream os = fs.create(dstPath);
//		FileInputStream isLocal = new FileInputStream(srcFile);
//		IOUtils.copy(isLocal,os);
//		isLocal.close();
//		os.close();


    }

    public void download(String srcFile, String dstFile) throws IOException {
        Path srcPath = new Path(srcFile);
        Path dstPath = new Path(dstFile);
        if (!fs.exists(srcPath)) {
            logger.error("download fail srcFile: " + srcFile + "not exist");
            return;
        }
        fs.copyToLocalFile(false, srcPath, dstPath, true);//????? false ?? true ????????????????????
//		FSDataInputStream is = fs.open(srcPath);
//		FileOutputStream osLocal = new FileOutputStream(dstFile);
//		IOUtils.copy(is,osLocal);
//		is.close();
//		osLocal.close();

    }

    public void listFiles(String srcFile, boolean containDir) throws IOException {
        Path srcPath = new Path(srcFile);
        if (!fs.exists(srcPath)) {
            logger.warn("listFiles fail srcFile: " + srcFile + " not exist");
            return;
        }
        RemoteIterator<LocatedFileStatus> remoteFileLiterator = fs.listFiles(srcPath, containDir);
        while (remoteFileLiterator.hasNext()) {
            LocatedFileStatus lfs = remoteFileLiterator.next();
            Path fullPath = lfs.getPath();
            System.out.println(fullPath);
        }

    }

    public boolean mkdir(String srcFile) throws IOException {
        Path srcPath = new Path(srcFile);//???，??
        if (!fs.exists(srcPath)) {
            logger.warn("mkdir fail srcFile: " + srcFile + " not exist");
            return false;
        }
        return fs.mkdirs(srcPath);
    }


    public boolean rmdir(String srcFile) throws IOException {

        Path srcPath = new Path(srcFile);//???，??
        if (!fs.exists(srcPath)) {
            logger.warn("rmdir fail srcDir:" + srcFile + " not exist");
            return false;
        }
        return fs.delete(srcPath, true);
    }

    public boolean rm(String srcFile) throws IOException {
        Path srcPath = new Path(srcFile);//???，??
        return fs.delete(srcPath, false);
    }

    public void printFileContent(String srcFile, int lines) throws IOException {
        Path srcPath = new Path(srcFile);//???，??
        FSDataInputStream in = fs.open(srcPath);//?????，??
        Scanner scanner = new Scanner(in);
        int line = 0;
        if (!fs.exists(srcPath)) {
            logger.warn("printFileContent fail file not exist");
            return;
        }
        while (scanner.hasNext()) {
            System.out.println(scanner.nextLine());
            if (line == lines)
                break;
            line++;
        }
        scanner.close();
    }

    public void printFileContent(String srcFile) throws IOException {
        Path srcPath = new Path(srcFile);//???，??
        FSDataInputStream in = fs.open(srcPath);//?????，??
        Scanner scanner = new Scanner(in);
        while (scanner.hasNext()) {
            System.out.println(scanner.nextLine());
        }
        scanner.close();
    }
}
