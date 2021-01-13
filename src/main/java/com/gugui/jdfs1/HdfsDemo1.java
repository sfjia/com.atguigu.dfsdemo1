package com.gugui.jdfs1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.validator.PublicClassValidator;
import sun.nio.ch.IOUtil;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.net.URI;
import java.util.Arrays;


/**
 * @Author 贾
 * @Date 2020/8/1523:51
 *
 *
 * hdfs 的增删改查
 *
 */
public class HdfsDemo1 {

    private FileSystem fileSystem;

    private Configuration configuration;
    @Test
    public void getClient(){
        try {
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS","hdfs://192.168.37.140:9000");
            FileSystem fileSystem = FileSystem.get(configuration);

            fileSystem.mkdirs(new Path("/0308/66"));
            fileSystem.close();
            System.out.println("56789");
        } catch (Exception e) {
            e.printStackTrace();
        }finally {

        }
    }


    @Test
    public void getClient2(){
        try {
//            Configuration configuration = new Configuration();
//            FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.37.140:9000"),configuration,"root");

            fileSystem.mkdirs(new Path("/0308/6633"));
            fileSystem.close();
            System.out.println("56789");
        } catch (Exception e) {
            e.printStackTrace();
        }finally {

        }
    }
    public static void main(String[] args) {
        try {
            Configuration configuration = new Configuration();
            configuration.set("fs.defaultFS","hdfs://192.168.37.140:9000");
            FileSystem fileSystem = FileSystem.get(configuration);

            fileSystem.mkdirs(new Path("/0308/66"));
            fileSystem.close();
            System.out.println("56789");
        } catch (Exception e) {
            e.printStackTrace();
        }finally {

        }
    }

    @Test
    public void putFile() throws Exception {
//        Configuration configuration = new Configuration();
//        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.37.140:9000"),configuration,"root");
        Path src = new Path("D:/site-1.10.11.zip");
        Path dst = new Path("/0380/66331");
        fileSystem.copyFromLocalFile(src,dst);
        fileSystem.close();
        System.out.println(" over");

    }

    @Test
    public void removeFile() throws Exception {
//        Configuration configuration = new Configuration();
//        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.37.140:9000"),configuration,"root");

        Path dst = new Path("/0308/");
        fileSystem.delete(dst,true);
        fileSystem.close();
        System.out.println(" over");

    }

    @Test
    public void getFile() throws Exception{
//        Configuration configuration = new Configuration();
//        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.37.140:9000"),configuration,"root");
//        /boolean delSrc,是否删除源文件
//        Path src, 原路径
//        Path dst, 目标路径
//        boolean useRawLocalFileSystem 是否使用原始本地文件系统（文本校验）
        Path dst = new Path("D:/dashujutest/site-1.10.11.zip");
        Path src = new Path("/0380/66331");
        fileSystem.copyToLocalFile(false,src,dst,true);
        fileSystem.close();
        System.out.println(" over" );
    }

    @Before
    public void init() throws Exception{
         configuration = new Configuration();
         fileSystem = FileSystem.get(new URI("hdfs://192.168.37.140:9000"),configuration,"root");
    }

    @Test
    public void rename() throws Exception{

        fileSystem.rename(new Path("/0380/66331"),new Path("/0380/test.data"));
        fileSystem.close();
        System.out.println("over" );
    }

    /**
     * 查询 文件信息
     */
    @Test
    public void findFileInfo() throws Exception{
        RemoteIterator<LocatedFileStatus> locatedFileIterator = fileSystem.listFiles(new Path("/"), true);
        while (locatedFileIterator.hasNext()){
            LocatedFileStatus next = locatedFileIterator.next();
            System.out.print("next.getLen() = " + next.getLen()+"     ");
            System.out.print("next = " + next.getPath().getName()+"    ");
            System.out.print("next.getBlockLocations() = " + Arrays.toString(next.getBlockLocations())+"   ");
            System.out.println("next.getPermission() = " + next.getPermission());

        }
    }

    @Test
    public void FileOrDir() throws Exception{
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
        for (FileStatus status : fileStatuses) {
            System.out.print("status.getPath().getName() = " + status.getPath().getName());
            System.out.print("status.isFile = " + status.isFile());
            System.out.println("status.isDirectory = " + status.isDirectory());
        }
        fileSystem.close();
    }

    @Test
    public  void  ioPut() throws Exception{
        //获取hdfs客户端

        //创建输入流
        FileInputStream fileInputStream = new FileInputStream("D:/site-1.10.11.zip");
        //创建hdfs输出流
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/0380/io/site.zip"));
        // 写磁盘 内部已关流
        IOUtils.copyBytes(fileInputStream,fsDataOutputStream,configuration);
//        关流
        fileSystem.close();
        System.out.println("over " );
    }

    @Test
    public void ioGet()throws Exception{
        //获取输入流
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/0380/io/site.zip"));
        //获取输出流
        FileOutputStream fileOutputStream = new FileOutputStream("D:/dashujutest/iosite1.zip");
        //流拷贝
        IOUtils.copyBytes(fsDataInputStream,fileOutputStream,configuration);
        fileSystem.close();
        System.out.println(" over " );
    }
}
