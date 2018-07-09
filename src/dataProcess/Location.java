package dataProcess;

import util.FileOperation;

import java.io.*;
import java.util.*;

/**
 * Created by motu on 2018/7/6.
 */
public class Location {
    /**
     * 路径配置
     * D:\cbd\！！毕业论文\给贺小木的数据处理代码+数据\wkqdianping\rawdata\
     * D:\cbd\！！毕业论文\给贺小木的数据处理代码+数据\wkqdianping\rawdata\checkinWithTime_filter
     */
    public static String rootPath = "D:\\cbd\\！！毕业论文\\给贺小木的数据处理代码+数据\\wkqdianping\\rawdata\\";
    //    public static String shopInfoPath = rootPath + "shopInfo_deduplicate.json";
    public static String desPath = rootPath+"poiLoationMap\\";
    public static String filename = "DianpingCheckintrue2020.txt";
    public static String readPath = rootPath+"checkinWithTime_filter\\"+"DianpingCheckintrue2020.txt" ;
//    public static String categoryFilterPath = "";//只取重庆火锅这类菜

    public static void main(String[] args) {
        File desFile = new File(desPath);//新建输出文件
        if (!desFile.exists()) {
            desFile.mkdir();
        }
        System.out.println("start " + new Date());
        getPoiLoctionMap(readPath);
        System.out.println("end " + new Date());
    }

    /**
     * 获取过滤后、已经映射过id的 poi与经纬度坐标之间的关系
     * !!ID已经经过映射！！
     * @param readPath
     */
    public static void getPoiLoctionMap(String readPath) {
        Map<String, String> poiLocationMap;
//        poiLocationMap = new HashMap<>();
        Set<String> poiSet =new HashSet<>();
        System.out.println("checkin-location-Mapping开始" + readPath);
        String read;
        FileInputStream file;
        BufferedReader bufferedReader;
        StringBuilder poiLocationStr = new StringBuilder();
        try {
            System.out.println("PoiLoctionMap解析开始读文件");
            file = new FileInputStream(readPath);
            bufferedReader = new BufferedReader(new InputStreamReader(file, "UTF-8"));
            while ((read = bufferedReader.readLine()) != null) {
                String[] contents = read.trim().split("[ \t,]+");
                String poiId = contents[1];
                String categoryId = contents[2];
                String location = contents[4] + "\t" + contents[5];//cat, lng,lat
                if(!poiSet.contains(poiId)){
                    poiLocationStr.append(poiId+"\t"+location+"\t"+categoryId+"\n");
                    poiSet.add(poiId);
                }

            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        FileOperation.writeNotAppdend(desPath +filename, poiLocationStr.toString());
    }
}
