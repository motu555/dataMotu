package main.parser;

import parser.FollowsParser;
import util.FileOperation;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by wangkeqiang on 2016/11/26.
 */
public class FollowsParserMain {
    static int count=0;
    static String destpath="D:\\cbd\\！！毕业论文\\给贺小木的数据处理代码+数据\\userfollowdata\\";

    public static void main(String[] args) throws IOException {
        //将匹配的user的follow文件从磁盘复制到本机
/*        String userpath = "./rawdata/Index/DianpingCheckinfalse2525_userMap";
        List<String> userList;
        userList=ReadUserlist(userpath);

        String rootpath = "G:\\王科强交接\\dianping_follows";
        ReadFile(rootpath,userList);
        System.out.println("复制文件数量"+count);*/

        //从user的follow文件中抽取其follow，并记录为一行
        String path = "D:\\cbd\\！！毕业论文\\给贺小木的数据处理代码+数据\\userfollowdata\\";
        List<String> filePaths = FileOperation.readAllFilePathCons(path, "txt");
        FollowsParser followsParser = new FollowsParser("./rawdata/followsInfo2525.txt", filePaths);
        followsParser.getAllFollowsInfo();
    }

    private static void ReadFile(String fileDir,List<String> userList) throws IOException {
        File file = new File(fileDir);
        if(file.isDirectory()){
            File[] fs=file.listFiles();
            for(int i=0;i<fs.length;i++){
                String fsPath=fs[i].getAbsolutePath();
                ReadFile(fsPath,userList);
            }
        }else if(file.isFile()){
            String fname=file.getAbsolutePath();
            String user=file.getName();
//            System.out.println(file.getName());
            if(userList.contains(user.substring(0,user.length()-4))){
                File source=new File(fname);
                File dest = new File(destpath+user);
                copyFileUsingFileChannels(source,dest);
                System.out.println("yes");
                count++;
            }
          }else{
            System.out.println("路径不正确!");
       }
    }

    private static void copyFileUsingFileChannels(File source, File dest) throws IOException {

        FileChannel inputChannel = null;
        FileChannel outputChannel = null;
        try {
            inputChannel = new FileInputStream(source).getChannel();
            outputChannel = new FileOutputStream(dest).getChannel();
            outputChannel.transferFrom(inputChannel, 0, inputChannel.size());
        } finally {
            inputChannel.close();
            outputChannel.close();
        }
    }

    private static List<String> ReadUserlist(String userpath) {
        FileInputStream file;
        String read, temp1;
        String[] temp;
        List<String> userList = new ArrayList<>();
        BufferedReader bufferedReader;
        try {
            file = new FileInputStream(userpath);
            bufferedReader = new BufferedReader(new InputStreamReader(file, "UTF-8"));
            while ((read = bufferedReader.readLine()) != null && !read.equals("")) {
//                temp1=read.trim().replaceAll("[ {|}]", "");//去除字符串中所包含的空格（包括:空格(全角，半角)、制表符、换页符等）
                temp = read.substring(1, read.length() - 1).split("[ \t,]+");
                for (String pair : temp) {
                    userList.add(pair.split("=")[0]);
                }
//                System.out.print("111");
//                .split("[ \t,]+");
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    return userList;
    }
}
