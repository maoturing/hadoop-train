package com.imooc.hadoop.hdfs;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HDFSApp {
	public static final String HDFS_PATH = "hdfs://192.168.247.128:8020";
	
	FileSystem fileSystem = null;
	Configuration configuration = null;

	/*
	 * 创建文件夹
	 */
	@Test
	public void mkdir() throws Exception{
		Boolean result = fileSystem.mkdirs(new Path("/hdfsapi2"));
		System.out.println(result);
	}
	
	
	/**
     * 创建文件
     */
    @Test
    public void create() throws Exception {
        FSDataOutputStream output = fileSystem.create(new Path("/hdfsapi/a.txt"));
        output.write("hello hadoop".getBytes());
        output.flush();
        output.close();
    }
    
    /*
     * 查看文件内容
     */
    @Test
    public void cat() throws Exception{
    	FSDataInputStream in = fileSystem.open(new Path("/hdfsapi/a.txt"));
    	IOUtils.copyBytes(in,System.out,1024);
    	in.close();
    }
    

    /**
     * 重命名
     */
    @Test
    public void rename() throws Exception {
        Path oldPath = new Path("/hdfsapi/test/a.txt");
        Path newPath = new Path("/hdfsapi/test/b.txt");
        fileSystem.rename(oldPath, newPath);
    }

    
    /**
     * 查看某个目录下的所有文件
     */
    @Test
    public void listFiles() throws Exception {
    	
    	Path hdfsPath = new Path("/hdfsapi");
//    	Path hdfsPath = new Path("/");
		FileStatus[] fileStatuses = fileSystem.listStatus(hdfsPath);

        for(FileStatus fileStatus : fileStatuses) {
            String isDir = fileStatus.isDirectory() ? "文件夹" : "文件";
            short replication = fileStatus.getReplication();
            long len = fileStatus.getLen();
            String path = fileStatus.getPath().toString();

            System.out.println(isDir + "\t" + replication + "\t" + len + "\t" + path);
        }

    }

	
    @Before
    public void setUp() throws Exception {
    	configuration = new Configuration();
    	fileSystem = FileSystem.get(new URI(HDFS_PATH),configuration,"hadoop");  //hdfs可替换为hadoop
        System.out.println("HDFSApp.setUp");
    }

    @After
    public void tearDown() throws Exception {
    	configuration = null;
    	fileSystem = null;
        System.out.println("HDFSApp.tearDown");
    }
}
