package process.others;

import util.FileOperation;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by motu on 2018/7/9.
 * 将原本的user-time-poi。改为user-time-category
 */
public class ModelInput {
    public static void main(String[] args){
        String shoppath="./rawdata/shopInfo_15.txt";//所有shop的info信息
        String filepath="./rawdata/newFilter_utp/DianpingCheckintrue0.3_false1510.txt";
        Map<String,String> poicateMap = new HashMap<>();
        poicateMap=PoiCateMap(shoppath);
        category(filepath,poicateMap);

    }

    /**
     * 原有：：将原本的user-poi-time。匹配user-category-time
     * 现在，filter中已经是user-time-poi,匹配为 user-time-category
     * 匹配poi对应的category
     */
    public static void category(String filepath,Map<String,String> poicateMap){
        StringBuilder PoiCatStr = new StringBuilder();
        FileInputStream file ;
        BufferedReader bufferedReader;
        String read;
        try {
            System.out.println("checkinrecord解析开始读文件");
            file = new FileInputStream(filepath);
            bufferedReader = new BufferedReader(new InputStreamReader(file, "UTF-8"));
            while ((read = bufferedReader.readLine()) != null) {
                String[] contents = read.trim().split("[ \t,]+");
                String tempUser = contents[0];
                String tempTime = contents[1];
                String tempItem = contents[2];

                if(poicateMap.containsKey(tempItem)){
                    String tempcate=poicateMap.get(tempItem);
                    PoiCatStr.append(tempUser+"\t"+tempTime+"\t"+tempcate+"\n");
                }
                else{
                    System.out.println("poi信息查找失败");
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        String filename=filepath.substring(filepath.lastIndexOf("/") + 1).replace(".txt", "");
        FileOperation.writeNotAppdend( "./rawdata/modelInput/" + filename+"_cate" +".txt", PoiCatStr.toString());
    }

    //读取所有poi以及categeory的map并返回
    public static Map<String,String > PoiCateMap(String  shoppath){
        Map<String,String> poicateMap = new HashMap<>();
        FileInputStream file ;
        BufferedReader bufferedReader;
        String read;
        try {
            System.out.println("shopinfo解析开始读文件");
            file = new FileInputStream(shoppath);
            bufferedReader = new BufferedReader(new InputStreamReader(file, "UTF-8"));
            while ((read = bufferedReader.readLine()) != null) {
                String[] contents = read.trim().split("[ \t,]+");
                String tempItem = contents[0];
                String tempCate = contents[1];
                poicateMap.put(tempItem,tempCate);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return poicateMap;
    }


}
