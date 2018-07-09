package dataProcess;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import util.FileOperation;
/**
 * Created by motu on 2018/6/17.
 */
public class process {
/*
读取原始数据checkinWithTimestamp.txt 格式"userid+\t+poiid+\t+poicategory+time"
并以<user,time,category,time>存储，time转换为unix时间戳
*/

    //加上对train与test数据集中的user，item,tag数据统计
//    return dataPosts;


    /*将str指定格式时间转换为unix时间戳*/
    public static String Date2TimeStamp(String dateStr, String format) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(format);
            return String.valueOf(sdf.parse(dateStr).getTime() / 1000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }
    /*将unix时间戳转换为指定格式*/
    public static String TimeStamp2Date(String timestampString, String formats) {
//        formats = "yyyy-MM-dd HH:mm:ss";
        Long timestamp = Long.parseLong(timestampString);//* 1000
        String date = new SimpleDateFormat(formats, Locale.CHINA).format(new Date(timestamp));
        return date;
    }

    /**
     * 将文件mapping编号，并分为训练集和测试集
     *随机
     * TODO
     * 按照每个用户的最后几个切分？
     */
    public static void DivideData(String filepath){
        String filename = "DianpingCheckinfalse1010";
        String readpath = filepath+filename +".txt";
        String despth = "./demo/data/UTP/";

        FileInputStream file;
        BufferedReader bufferedReader;
        StringBuilder trainStr = new StringBuilder();
        StringBuilder testStr = new StringBuilder();
        String read;
        Map<String, Integer> userIds, poiIds, timeIds;//用于编号的映射关系
        userIds = new HashMap<>();
        poiIds = new HashMap<>();
        timeIds = new HashMap<>();
        int trainnum=0;
        int testnum=0;
        try {
            file = new FileInputStream(readpath);
            bufferedReader = new BufferedReader(new InputStreamReader(file, "UTF-8"));
            while ((read = bufferedReader.readLine()) != null) {
                String[] contents = read.trim().split("[ \t,]+");
                String tempUser = contents[0];
                String tempItem = contents[1];
                String tempTime = contents[2];
                //重新映射编号
                int innerUser = userIds.containsKey(tempUser) ? userIds.get(tempUser) : userIds.size();
                userIds.put(tempUser, innerUser);
                int innerPoi = poiIds.containsKey(tempItem) ? poiIds.get(tempItem) : poiIds.size();
                poiIds.put(tempItem, innerPoi);
                int innerTime = timeIds.containsKey(tempTime) ? timeIds.get(tempTime) : timeIds.size();
                timeIds.put(tempTime, innerTime);
                String writeStr = innerUser+"\t"+innerTime+"\t"+innerPoi+"\n";
                if(Math.random()<0.8){
                    trainStr.append(writeStr);
                    trainnum++;
                }
                else {
                    testStr.append(writeStr);
                    testnum++;
                }

            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("trainnum:"+trainnum+" testnum:   "+testnum);
        System.out.println("usernum:"+userIds.size()+" poinum:"+poiIds.size()+" timenum:"+timeIds.size());
        FileOperation.writeNotAppdend(despth + filename + "_train.txt", trainStr.toString());
        FileOperation.writeNotAppdend(despth + filename + "_test.txt", trainStr.toString());
        //id映射文件记录
        FileOperation.writeNotAppdend(filepath+filename+"_userMapIndex",userIds.toString());
        FileOperation.writeNotAppdend(filepath+filename+"_poiMapIndex",poiIds.toString());
        FileOperation.writeNotAppdend(filepath+filename+"_timeMapIndex",timeIds.toString());

    }
    public static void getuserMap(String filepath){
        String filename = "DianpingCheckinfalse2525";
        String readpath = filepath+"checkinWithTime_filter/"+filename +".txt";
        String despth = "./rawdata/Index/";
        FileInputStream file;
        BufferedReader bufferedReader;
        StringBuilder trainStr = new StringBuilder();
        StringBuilder testStr = new StringBuilder();
        String read;
        Map<String, Integer> userIds;//用于编号的映射关系
        userIds = new HashMap<>();
        try {
            file = new FileInputStream(readpath);
            bufferedReader = new BufferedReader(new InputStreamReader(file, "UTF-8"));
            while ((read = bufferedReader.readLine()) != null) {
                String[] contents = read.trim().split("[ \t,]+");
                String tempUser = contents[0];
                //重新映射编号
                int innerUser = userIds.containsKey(tempUser) ? userIds.get(tempUser) : userIds.size();
                userIds.put(tempUser, innerUser);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        FileOperation.writeNotAppdend(despth+filename+"_userMap",userIds.toString());

    }


    public static void main(String args[]){
//        timestampTrans("./rawdata");
//            DivideData("./rawdata/");
        getuserMap("./rawdata/");
//        String date = Date2TimeStamp("15-09-24" , "yy-MM-dd");// HH:mm:ss
//        String date =TimeStamp2Date( "1288291694000","yy-MM-dd");
    }

}
