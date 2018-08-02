package process.dataProcess;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import com.alibaba.fastjson.JSON;
import data.Review;
import data.ShopInfo;
import dataProcess.CircularFilter;
import org.apache.commons.lang.StringUtils;
import util.FileOperation;
/**
 * Created by motu on 2018/6/26.
 * 处理后的数据是user,poi,time，每一条表示用户，餐厅，种类，签到时间
 * 两个过滤条件user访问poi的数量，poi被user访问的数量
 *
 */
public class Filter {
    /**
     * shopInfo.json中的统计信息
     */
    static int originShopNum;
    //shopInfo.json中的shopid集合
    static public Set<String> shopInfoIdSet;
    static public Map<String, ShopInfo> shopInfoMap;
    static public Map<String, String> shopCategoryMap;
    /**
     * 原始的shopReviewExtend.json？？中的统计信息
     */
    static int originReviewCount;
    static Set<String> originUserSet;
    static Set<String> originShopSet;
    /**
     * shopReviewExtend.json与shopInfo.json取交集，并经过一定条件过滤后的统计信息
     */
    static int tripleNum;
    static int reviewCount;
    static Set<String> userSet = new HashSet<>();
    static Set<String> shopSet = new HashSet<>();
    /**
     * !!过滤评论数据会用到的数据结构
     */
    //结构需要修改！！
    static Map<String, Map<String, Integer>> checkInRecordMap; //-user-<shop, record>
    static Map<String, Set<String>> globalUserItemSetMap; // -<user,-<shops>>
//    static List<String> categoryFilterList;
//    static public Map<String, String> shopCategoryMap;//<shop,category>outputResultWithMapWithTime(globalUserItemSetMap, resultSet, userLeastCount, itemLeastCount, isMapping);

    /**
     * 读取整条记录所用结构
     *
     */
    static Map<String, Map<String, Map<String,String>>> checkInRecordMapWithTime;//-user-shop,-time <cat+lng+lat>
    static Set<String> originUserSetWithTime;
    static Set<String> originShopSetWithTime;
    static Set<String> userSetWithTime ;
    static Set<String> shopSetWithTime ;
    /**
     * 路径配置
     * D:\cbd\！！毕业论文\给贺小木的数据处理代码+数据\wkqdianping\rawdata\checkinWithTimestamp_15.txt
     */
    public static String rootPath = "./rawdata/";
//    public static String shopInfoPath = rootPath + "shopInfo_deduplicate.json";
    public static String desPath = rootPath + "newFilter_utp/";
//    public static String reviewPath = rootPath +"test_15.txt";
    public static String reviewPath =rootPath+"checkinWithTimestamp_15false.txt";
//    public static String categoryFilterPath = "";//只取重庆火锅这类菜

    public static void main(String[] args) {
        System.out.println("start " + new Date());
        int userLeastCount = 15; //每个用户去过10个以上的餐厅
        int itemLeastCount = 10; //每家餐厅被10个以上的用户访问过
        boolean isMapping = false; //保存的签到记录中id是否映射为顺序id, true为映射，false为保留原来的id

        /**
         * 随机抽样一部分数据，然后再进行过滤
         */
        boolean issample = true;
        if(issample){
            double randomRatio = 0.3;
            System.out.println("sample ratio\t" + randomRatio);
            randomSample(randomRatio);
            reviewPath=rootPath+"sample/" + "DianpingCheckin_sample" +"_"  + randomRatio  + ".txt";

        }



//        System.out.println("shopInfoPath: " + shopInfoPath);
        System.out.println("reviewPath: " + reviewPath);
        System.out.println("despath: " + desPath);
        System.out.println("filter condition " + userLeastCount + " " + itemLeastCount );

        /**
         * 获取原始文件数据 shop信息和checkin记录
         */

//        boolean filterByCategory = false;
        shopCategoryMap = new HashMap<>();
//        getShopInfo(shopInfoPath);//shopInfoIdSet,shopInfoMap,shopCategoryMap
        getCheckInRecord(reviewPath);
//        getCheckInRecordWithTime(reviewPath);

        /**
         *得到原始的userItem映射关系
         */
        Map<String, Set<String>> userItemSetMap = getUserItemSetMapofRecord();
//        Map<String, Map<String, Integer>> shopDishSetMap = getShopDishSetMapofRecord();
//        Map<String, Map<String, Set<String>>> dishPostSetMap = getDishPostSetMap();

        int iteration = -1;//表示会不断迭代直到同时满足least限制条件
        //返回userItemSetMap和itemUserSetMap
        Set<String>[] resultSet = CircularFilter.shopCountFilterNodish(checkInRecordMap, userItemSetMap,  userLeastCount, itemLeastCount, iteration);

        globalUserItemSetMap = CircularFilter.get_userItemSetMap();


//        outputResultWithMapWithTime(globalUserItemSetMap, resultSet, userLeastCount, itemLeastCount, isMapping);
        outputResultWithMap(globalUserItemSetMap, resultSet, userLeastCount, itemLeastCount,issample, isMapping);
        System.out.println("end " + new Date());
    }
    /**
     * user-shop-time-category-lng-lat
     */
    public static void getCheckInRecordWithTime(String reviewPath) {
        System.out.println("checkinrecordWithTime解析开始====整条记录"+ reviewPath);
        String read;
//        int dupilcate=0;
        FileInputStream file;
        BufferedReader bufferedReader;
        checkInRecordMapWithTime = new HashMap<>();
        originUserSetWithTime = new HashSet<>();
        originShopSetWithTime = new HashSet<>();
        userSetWithTime = new HashSet<>();
        shopSetWithTime = new HashSet<>();
        try {
            System.out.println("checkinrecordWithTime===解析开始读文件");
            file = new FileInputStream(reviewPath);
            System.out.print("````````");
            bufferedReader = new BufferedReader(new InputStreamReader(file, "UTF-8"));
            System.out.print(".......");
            while ((read = bufferedReader.readLine()) != null) {
                System.out.print("=======");
                String[] contents = read.trim().split("[ \t,]+");
//                String record = read;
                String userId = contents[0];
                String shopId = contents[1];
                String timeId = contents[2];
                String info = contents[3]+"\t"+contents[4]+"\t"+contents[5];//cat, lng,lat
                originUserSetWithTime.add(userId);
                originShopSetWithTime.add(shopId);
//                reviewCount++;
                if (!checkInRecordMapWithTime.containsKey(userId))//将新的userid加入
                    checkInRecordMapWithTime.put(userId, new HashMap<>());

                if (!checkInRecordMapWithTime.get(userId).containsKey(shopId)) {//将新的shopid加入
                    checkInRecordMapWithTime.get(userId).put(shopId, new HashMap<>());
                }
                if (!checkInRecordMapWithTime.get(userId).get(shopId).containsKey(timeId)) {//将新的shopid加入
                    checkInRecordMapWithTime.get(userId).get(shopId).put(timeId,info);
                }

            }bufferedReader.close();
            System.out.println("checkInRecordMapWithTim----finish");
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * user-shop-次数
     *统计<userid-shopid-dishid>三元组的个数？
     * 无需解析review.json
     * @param reviewPath
     */
    public static void getCheckInRecord(String reviewPath) {
        System.out.println("checkinrecord解析开始"+ reviewPath);
        String read;
        FileInputStream file;
        BufferedReader bufferedReader;
        checkInRecordMap = new HashMap<>();
        originUserSet = new HashSet<>();
        originShopSet = new HashSet<>();
        userSet = new HashSet<>();
        shopSet = new HashSet<>();
        //记录重复的<user,shop,dish>三元组的个数
/*        int duplicateTriplePairNum = 0;
        Map<String, Set<String>> userDishPair = new HashMap<>();
        Map<String, Set<String>> shopDishPair = new HashMap<>();*/

        try {
            System.out.println("checkinrecord解析开始读文件");
            file = new FileInputStream(reviewPath);
            bufferedReader = new BufferedReader(new InputStreamReader(file, "UTF-8"));
            while ((read = bufferedReader.readLine()) != null) {
//                System.out.print("bbb");
//                originReviewCount++;
                String[] contents = read.trim().split("[ \t,]+");
//                String record = read;
                String userId = contents[0];
                String shopId = contents[1];
//                String categoryId = contents[2];
//                String time = contents[3];
                originUserSet.add(userId);
                originShopSet.add(shopId);
//                if (shopInfoIdSet.contains(shopId) && StringUtils.isNumeric(userId) && StringUtils.isNumeric(shopId) ) {
                    reviewCount++;
                    if (!checkInRecordMap.containsKey(userId))//将新的userid加入
                        checkInRecordMap.put(userId, new HashMap<>());

                    if (!checkInRecordMap.get(userId).containsKey(shopId)) {//将新的shopid加入
                        checkInRecordMap.get(userId).put(shopId, 1);
                        shopSet.add(shopId);
                    }else {
                        checkInRecordMap.get(userId).put(shopId,checkInRecordMap.get(userId).get(shopId)+ 1);
//                      checkInRecordMap.get(userId).get(shopId).put(checkInRecordMap.get(userId).get(shopId)+ 1);
                    }

//                }
            }bufferedReader.close();
//            System.out.println(checkInRecordMap.toString());
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * 计算ueser-shop pair的个数 map输出
     * @return
     */
    public static Map<String, Set<String>> getUserItemSetMapofRecord() {
        int userShopPairNum = 0;
        Map<String, Set<String>> userItemSetMap = new HashMap<>();
        for (Map.Entry<String, Map<String, Integer>> userItemRecEntry : checkInRecordMap.entrySet()) {
            String tempUser = userItemRecEntry.getKey();
            Set<String> tempItemSet = new HashSet<>();
            Map<String, Integer> tempItemRecMap = userItemRecEntry.getValue();
            for (String tempItem : tempItemRecMap.keySet()) {
                userShopPairNum += 1;
                tempItemSet.add(tempItem);
            }
            if (tempItemSet.size() > 0)
                userItemSetMap.put(tempUser, tempItemSet);
        }
        System.out.println("统计15年review中user-shop Pair的数量 " + userShopPairNum);
        return userItemSetMap;
    }

    /**
     * 读取文件中只有userid-poiid-timestamp
     * poi的category和lng lat信息单独写在文件内，不做id的mapping
     * @param userItemSetMap
     * @param resultSet
     * @param userLeastCount
     * @param itemLeastCount
     * @param isMapping
     */
    public static void outputResultWithMap(Map<String, Set<String>> userItemSetMap,
                                                   Set<String>[] resultSet,  int userLeastCount, int itemLeastCount,boolean issample, boolean isMapping) {
        Map<String, Integer> userMap = new ConcurrentHashMap<>();
        Map<String, Integer> itemMap = new ConcurrentHashMap<>();
        Set<String> userSet = resultSet[0];
        Set<String> itemSet = resultSet[1];
        Map<String, Integer> userIds, poiIds, timeIds;//用于编号的映射关系
        userIds = new HashMap<>();
        poiIds = new HashMap<>();
        timeIds = new HashMap<>();

        int filteredTripleNum = 0;



        //太占用内存 因此替换上一段直接对文件过滤
        StringBuilder userItemFreStr = new StringBuilder();
        FileInputStream file;
        BufferedReader bufferedReader;
        String read;
        try {
            file = new FileInputStream(reviewPath);
            bufferedReader = new BufferedReader(new InputStreamReader(file, "UTF-8"));
//            System.out.println("output");
            while ((read = bufferedReader.readLine()) != null) {
                String[] contents = read.trim().split("[ \t,]+");
                String tempUser = contents[0];
                String tempItem = contents[1];
                String tempTime = contents[2];
//                String tempinfo = contents[3] + "\t" + contents[4] + "\t" + contents[5];//cat, lng,lat
                if (itemSet.contains(tempItem) & (userSet.contains(tempUser))) {
                //   用于重新编号,统计数量
                    int innerUser = userIds.containsKey(tempUser) ? userIds.get(tempUser) : userIds.size();
                    userIds.put(tempUser, innerUser);

                    int innerPoi = poiIds.containsKey(tempItem) ? poiIds.get(tempItem) : poiIds.size();
                    poiIds.put(tempItem, innerPoi);

                    int innerTime = timeIds.containsKey(tempTime) ? timeIds.get(tempTime) : timeIds.size();
                    timeIds.put(tempTime, innerTime);
                    if (isMapping) {
                        //模型正确的输入顺序
                        userItemFreStr.append(innerUser  + "\t" + innerTime + "\t" + innerPoi +"\n");
                        filteredTripleNum++;
                    } else {
                        //模型正确的输入顺序
                        userItemFreStr.append(tempUser +  "\t" + tempTime +"\t" + tempItem + "\n");
                        filteredTripleNum++;
                    }
                }

            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("filter condition " + userLeastCount + " " + itemLeastCount );
        System.out.println("user number:"+userIds.size()+" shop number:"+poiIds.size()+" time number:"+timeIds.size()+" total number:"+filteredTripleNum);
        FileOperation.writeNotAppdend(desPath + "DianpingCheckin"+ issample +"_" + isMapping + userLeastCount + itemLeastCount + ".txt", userItemFreStr.toString());
//        FileOperation.writeNotAppdend(checkWithTimePath + "checkinWithTimestamp.txt", checkinWithTimestampBuilder.toString());
    }

    /**
     * //做映射用的map
     * 输出结果带cat以及lng和lat
     * @param userItemSetMap
     * @param resultSet
     * @param desPath
     * @param userLeastCount
     * @param itemLeastCount
     * @param isMapping
     */
    public static void outputResultWithMapWithTime(Map<String, Set<String>> userItemSetMap,
                                           Set<String>[] resultSet,  int userLeastCount, int itemLeastCount, boolean isMapping) {
        Map<String, Integer> userMap = new ConcurrentHashMap<>();
        Map<String, Integer> itemMap = new ConcurrentHashMap<>();
        Set<String> userSet = resultSet[0];
        Set<String> itemSet = resultSet[1];
        Map<String, Integer> userIds, poiIds, timeIds;//用于编号的映射关系
        userIds = new HashMap<>();
        poiIds = new HashMap<>();
        timeIds = new HashMap<>();

        int filteredTripleNum = 0;


        //输出时直接使用最终确定的userset和itemset来过滤checkInRecordMap
        /*StringBuilder userItemFreStr = new StringBuilder();
        for (Map.Entry<String, Map<String, Map<String,String>>> userItemFreEntry : checkInRecordMapWithTime.entrySet()) {
            String tempUser = userItemFreEntry.getKey();
            if (userSet.contains(tempUser)) {
                Map<String, Map<String,String>> tempItemFreMap = userItemFreEntry.getValue();
                for (String tempItem : tempItemFreMap.keySet()) {
                    if (itemSet.contains(tempItem)) {
                        Map<String,String> InfoFreMap = tempItemFreMap.get(tempItem);
                        for (Map.Entry<String, String> tempInfo : InfoFreMap.entrySet()) {
                            String tempTime = tempInfo.getKey();
                            String tempcat = tempInfo.getValue();//category和lng lat
                                if (isMapping) {
                                    //        用于重新编号
                                    int innerUser = userIds.containsKey(tempUser) ? userIds.get(tempUser) : userIds.size();
                                    userIds.put(tempUser, innerUser);

                                    int innerPoi = poiIds.containsKey(tempItem) ? poiIds.get(tempItem) : poiIds.size();
                                    poiIds.put(tempItem, innerPoi);

                                    int innerTime = timeIds.containsKey(tempTime) ? timeIds.get(tempTime) : timeIds.size();
                                    timeIds.put(tempTime, innerTime);

                                    userItemFreStr.append(innerUser + "\t" + innerPoi + "\t" + innerTime + "\t" + tempcat + "\n");

                                    filteredTripleNum++;
                                } else {

                                    filteredTripleNum++;
                                }
                            }
                        }
                    }
                }
            }*/
        /*//太占用内存 因此替换上一段直接对文件过滤
        StringBuilder userItemFreStr = new StringBuilder();
        FileInputStream file;
        BufferedReader bufferedReader;
        String read;
        try {
            file = new FileInputStream(reviewPath);
            bufferedReader = new BufferedReader(new InputStreamReader(file, "UTF-8"));
            System.out.println("output");
            while ((read = bufferedReader.readLine()) != null) {
                String[] contents = read.trim().split("[ \t,]+");
                String tempUser = contents[0];
                String tempItem = contents[1];
                String tempTime = contents[2];
                String tempinfo = contents[3] + "\t" + contents[4] + "\t" + contents[5];//cat, lng,lat
                if (itemSet.contains(tempItem) & (userSet.contains(tempUser))) {
                    if (isMapping) {
                        //        用于重新编号
                        int innerUser = userIds.containsKey(tempUser) ? userIds.get(tempUser) : userIds.size();
                        userIds.put(tempUser, innerUser);

                        int innerPoi = poiIds.containsKey(tempItem) ? poiIds.get(tempItem) : poiIds.size();
                        poiIds.put(tempItem, innerPoi);

                        int innerTime = timeIds.containsKey(tempTime) ? timeIds.get(tempTime) : timeIds.size();
                        timeIds.put(tempTime, innerTime);

                        userItemFreStr.append(innerUser + "\t" + innerPoi + "\t" + innerTime + "\t" + tempinfo + "\n");

                        filteredTripleNum++;
                    } else {
                        userItemFreStr.append(tempUser + "\t" + tempItem + "\t" + tempTime + "\t" + tempinfo + "\n");
                        filteredTripleNum++;
                    }
                }

            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }*/
        /*System.out.println("filter condition " + userLeastCount + " " + itemLeastCount );
        System.out.println("user number:"+userIds.size()+" shop number:"+poiIds.size()+" time number:"+timeIds.size()+" total number:"+filteredTripleNum);
        FileOperation.writeNotAppdend(desPath + "DianpingCheckin" + isMapping + userLeastCount + itemLeastCount + ".txt", userItemFreStr.toString());
//        FileOperation.writeNotAppdend(checkWithTimePath + "checkinWithTimestamp.txt", checkinWithTimestampBuilder.toString());*/
    }
    public static boolean isDouble(String str) {
        Pattern pattern = Pattern.compile("^[-\\+]?[.\\d]*$");
        return pattern.matcher(str).matches();
    }

    /**
     * sample一部分数据写入文件，然后在进行过滤
     * @param randomRatio
     */
    public static void randomSample(double randomRatio) {

        //太占用内存 因此替换上一段直接对文件过滤
        StringBuilder userItemFreStr = new StringBuilder();
        FileInputStream file;
        BufferedReader bufferedReader;
        String read;
        StringBuilder sampleStr=new StringBuilder();
        try {
            file = new FileInputStream(reviewPath);
            bufferedReader = new BufferedReader(new InputStreamReader(file, "UTF-8"));
//            System.out.println("output");
            Random random = new Random(1);
            while ((read = bufferedReader.readLine()) != null) {
                if (random.nextDouble() < randomRatio) {
                    sampleStr.append(read+"\n");
                }
            }
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("after sample\t" + randomRatio);
        FileOperation.writeNotAppdend(rootPath+ "sample/"+ "DianpingCheckin_sample" +"_"  + randomRatio  + ".txt", sampleStr.toString());

    }
}

