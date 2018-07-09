package process.others;

import com.alibaba.fastjson.JSON;
import data.Review;
import data.Shop;
import org.apache.commons.lang.StringUtils;
import util.FileOperation;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jinyuanyuan on 2018/1/7.
 * TODO special notice: 这是给贺小木的POI实验处理的数据
 * 输入：带有用户评价时间戳的reviews.json文件   共有21446863行
 * 例子：
 * {"reviewId":"63385536","shopId":"10004660","userId":"18482787","price":-1,"taste":4,"condition":4,"service":4,
 * "commentType":"团购点评","text":"不错不错啦～蛮喜欢这家的环境～","thumbs":-1,"time":"14-08-17"}
 *
 * @JSONType(orders =
 * {"reviewId", "shopId", "userId", "price", "taste", "condition", "service", "favDishes", "characteristic", "commentType", "text", "thumbs", "time"})
 * 输出：将字段缩减为<userid, poiid, poicategory, timestamp></>
 * 并输入用户签到时间统计文件，包含以下内容：
 * 总时间跨度：开始日期-结束日期
 * 对每个用户：
 * userid:  开始日期-结束日期    在此期间的总评论数    有评论行为的天数
 * <p>
 * 2.处理好这些统计信息后，再决定时间跨度，以及过滤条件，最终需要的数据是某一段时间内用户签到行为比较密集的签到数据
 */
public class reviewTimeProcess {
    private static String shopInfoPath = "D:\\cbd\\！！毕业论文\\给贺小木的数据处理代码+数据\\shopInfo_deduplicate.json";

    public static void reviewProcess(boolean mapping) throws FileNotFoundException, UnsupportedEncodingException {
        String reviewJsonFile = "D:\\cbd\\！！毕业论文\\给贺小木的数据处理代码+数据\\review_deduplicate.json"; //非水贴源文件
        String checkWithTimePath = "D:\\cbd\\！！毕业论文\\给贺小木的数据处理代码+数据\\output\\";
        String encoding = "UTF-8";


        Map<String, Integer> userIds, poiIds, categoryIds;
        userIds = new HashMap<>();
        poiIds = new HashMap<>();
        categoryIds = new HashMap<>();
        /**
         * 读取shopInfo文件，获得每个商店的category
         */
        Map<String, String> shopCategoryMap = getShopCategory(shopInfoPath);

        StringBuilder shopinfoBuilder = new StringBuilder();
        StringBuilder checkinWithTimestampBuilder = new StringBuilder();

        //记录所有用户的签到记录中的不同日期集合，取最大值和最小值作为总的时间跨度
        Set<String> totalTimeSpan = new HashSet<>();
        //记录每个用户的签到日期集合
        Map<String, Set<String>> userTimeSpan = new HashMap<>();
        String read;
        FileInputStream file;
        BufferedReader bufferedReader;
        Review rev;
        int count = 0;
        System.out.println("解析review.json开始");
        Set<String> poiInfoset=new HashSet<>();
        try {
            file = new FileInputStream(reviewJsonFile);
            bufferedReader = new BufferedReader(new InputStreamReader(file, "UTF-8"));
            while ((read = bufferedReader.readLine()) != null && !read.equals("")) {
//            while (count==0){
//                read = "{\"reviewId\":\"63385536\",\"shopId\":\"10004660\",\"userId\":\"18482787\",\"price\":-1,\"taste\":4,\"condition\":4,\"service\":4,\n" +
//                        " \"commentType\":\"团购点评\",\"text\":\"不错不错啦～蛮喜欢这家的环境～\",\"thumbs\":-1,\"time\":\"14-08-17\"}";
//                System.out.println(read);
                rev = JSON.parseObject(read, Review.class);
                if (rev.getTime() != null) {
                    String timeStamp = getTimeStamp(rev.getTime());
                    String userId = rev.getUserId();
                    String poiId = rev.getShopId();
//                        System.out.println(userId + "\t" + poiId + "\t" + shopCategoryMap.get(poiId) + "\t" + timeStamp + "\n");
                    String year = timeStamp.substring(0, 2);

                    /**
                     * 对时间进行过滤
                     */
                    if (year.equals("15")) {
                        if(shopCategoryMap.get(poiId)!=null) {
//                            String category = shopCategoryMap.get(poiId);
                            String Info = shopCategoryMap.get(poiId);
                            String[] contents = Info.trim().split("\t");
                            String category =contents[0];
                            String lng =contents[1];
                            String lat =contents[2];
                            if (!userTimeSpan.containsKey(userId)) {
                                userTimeSpan.put(userId, new HashSet<>());
                            }
                            userTimeSpan.get(userId).add(timeStamp);
                            totalTimeSpan.add(timeStamp);

                            if(mapping){
                                // inner id starting from 0
                                int innerUser = userIds.containsKey(userId) ? userIds.get(userId) : userIds.size();
                                userIds.put(userId, innerUser);

                                int innerPoi = poiIds.containsKey(poiId) ? poiIds.get(poiId) : poiIds.size();
                                poiIds.put(poiId, innerPoi);

                                int innerCat = categoryIds.containsKey(category) ? categoryIds.get(category) : categoryIds.size();
                                categoryIds.put(category, innerCat);

                                checkinWithTimestampBuilder.append(innerUser + "\t" + innerPoi + "\t" + timeStamp +"\n" );
                                if(!poiInfoset.contains(poiId))
                                {shopinfoBuilder.append(innerPoi+"\t"+ innerCat + "\t"+lng+"\t"+lat+"\n");
                                poiInfoset.add(poiId);}
                            }
                            else {
                                checkinWithTimestampBuilder.append(userId + "\t" + poiId + "\t" + timeStamp+"\n" );
                                if(!poiInfoset.contains(poiId))
                                {shopinfoBuilder.append(poiId+"\t"+ category + "\t"+lng+"\t"+lat+"\n");
                                    poiInfoset.add(poiId);}
                            }
                        }
                    }
                }
//                }
                count++;
                if (count % 1000000 == 0) {
                    System.out.println("Count up " + count + "th reviews " + new Date());
                }
            }
            bufferedReader.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        /**
         * 将总跨度和每个用户的跨度写入文件
         */
        /*Map<String, Set<String>> timeMap = new HashMap<>();
        String minTime = (String) Collections.min(totalTimeSpan);
        String maxTime = (String) Collections.max(totalTimeSpan);
        timeStatisticsBuilder.append("total time span\t" + minTime + "\tto\t" + maxTime + "\n");
        for (Map.Entry<String, Set<String>> entry : userTimeSpan.entrySet()) {
            String userid = entry.getKey();
            Set<String> timeSet = entry.getValue();
            String min = (String) Collections.min(timeSet);
            String max = (String) Collections.max(timeSet);
            String year = min.substring(0, 2);
            if (!timeMap.containsKey(year)) {
                timeMap.put(year, new HashSet<>());
            }
            timeMap.get(year).add(userid);
            timeStatisticsBuilder.append(userid + "\t" + min + "\t" + max + "\t有签到记录的天数\t" + timeSet.size() + "\n");
        }

        for (Map.Entry<String, Set<String>> entry : timeMap.entrySet()) {
            timeStatisticsBuilder.append(entry.getKey() + "\t" + entry.getValue().size() + "\n");
        }*/
        File desFile = new File(checkWithTimePath);//新建输出文件
        if (!desFile.exists()) {
            desFile.mkdir();
        }

//        FileOperation.writeNotAppdend(checkWithTimePath + "checkinWithTimestampStatistics.txt", timeStatisticsBuilder.toString());
        FileOperation.writeNotAppdend(checkWithTimePath + "checkinWithTimestamp_15"+mapping+".txt", checkinWithTimestampBuilder.toString());
        FileOperation.writeNotAppdend(checkWithTimePath +"shopInfo_15.txt",shopinfoBuilder.toString());
//        FileOperation.writeNotAppdend(checkWithTimePath+"checkinWithTimestamp_userMapIndex",userIds.toString());
//        FileOperation.writeNotAppdend(checkWithTimePath+"checkinWithTimestamp_poiMapIndex",poiIds.toString());
//        FileOperation.writeNotAppdend(checkWithTimePath+"checkinWithTimestamp_cateMapIndex",categoryIds.toString());
        System.out.println("user size\t" + userIds.size() + "\tpoi size\t" + poiIds.size() +"\tcategory size\t" + categoryIds.size());
    }

   
    public static Map<String, String> getShopCategory(String shopInfoPath) {
        System.out.println("解析shopInfo.json开始");

        Map<String, Set<String>>categoryShopsMap = new HashMap<>();
//        Map<String, String> shopCategoryMap = new HashMap<>();
        Map<String, String>shopInfoMap = new HashMap<>();
        String read;
        FileInputStream file;
        BufferedReader bufferedReader;
        Shop shop;

        try {
            file = new FileInputStream(shopInfoPath);
            bufferedReader = new BufferedReader(new InputStreamReader(file, "UTF-8"));
            while ((read = bufferedReader.readLine()) != null && !read.equals("")) {
                shop = JSON.parseObject(read, Shop.class);
                String shopId = shop.getShopId();
                String lng = shop.getLng();
                String lat = shop.getLat();
                String region = shop.getRegion();
                List<String> tags = shop.getTags();
                List<String> breadCrumb = shop.getBreadcrumb();

                if (StringUtils.isNumeric(shopId) && lng != null && !lng.equals("") && lat != null && !lat.equals("") && isDouble(lng) && isDouble(lat)
                        && region != null && !region.equals("") && tags != null && tags != null && breadCrumb != null && breadCrumb != null) {
                    if (breadCrumb.get(0).equals("上海餐厅")) {
                        String category;
                        //沿用彭宏伟代码中的思路
                        if (breadCrumb.size() >= 4)
                            category = breadCrumb.get(3);
                        else
                            category = breadCrumb.get(breadCrumb.size() - 1);
                        //加入shop经纬度信息
                        String info = category+"\t"+ lng +"\t"+ lat;
                        shopInfoMap.put(shopId,info);

                        if(!categoryShopsMap.containsKey(category)){
                            categoryShopsMap.put(category, new HashSet<>());
                        }
                        categoryShopsMap.get(category).add(shopId);
                    }
                }
            }
            bufferedReader.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("解析shopInfo.json结束 origin shop size " + shopInfoMap.size());
        System.out.println("category的个数：" + categoryShopsMap.size());
        /**
         * 输出每个category下的shop个数
         */
        System.out.println("输出每个category下的shop个数");
        for(Map.Entry<String, Set<String>>me : categoryShopsMap.entrySet()){
            System.out.println(me.getKey() + " " + me.getValue().size());
        }
        System.out.println("解析shopInfo.json结束 ");
        return shopInfoMap;
    }

    /**
     * 统计彭宏伟实验所用数据的category分布
     * 根目录：
     * D:\我的工作\投稿论文\彭宏伟计算机学报\实验所用数据\Dianping_v2\review_social
     * 商店的category描述文件：
     * 1DianpingDescription1010100.txt
     *
     * @param str
     * @return
     */
    public static void processDianpingFilteredData(String filteredDataPath){
        System.out.println("解析过滤后的商店描述文件开始");

        Map<String, Set<String>>categoryShopsMap = new HashMap<>();
        Map<String, String> shopCategoryMap = new HashMap<>();
        String read;
        FileInputStream file;
        BufferedReader bufferedReader;
        String[]splits;
        int lineNum=0;

        try {
            file = new FileInputStream(filteredDataPath);
            bufferedReader = new BufferedReader(new InputStreamReader(file, "UTF-8"));
            while ((read = bufferedReader.readLine()) != null && !read.equals("")) {
                lineNum ++ ;
                splits = read.replaceAll("\\s+","").split("##");
                String shopId = splits[0];
                List<String> breadCrumb = new ArrayList<>();
                for(int i = 1;i < splits.length;i++){
                    breadCrumb.add(splits[i]);
                }

                if (StringUtils.isNumeric(shopId) && !breadCrumb.isEmpty()) {
                    if (breadCrumb.get(0).contains("上海")) {
                        String category;
                        //沿用彭宏伟代码中的思路
                        if (breadCrumb.size() >= 4)
                            category = breadCrumb.get(3);
                        else
                            category = breadCrumb.get(breadCrumb.size() - 1);

                        shopCategoryMap.put(shopId, category);

                        if(!categoryShopsMap.containsKey(category)){
                            categoryShopsMap.put(category, new HashSet<>());
                        }
                        categoryShopsMap.get(category).add(shopId);
                    }else{
                        System.out.println(breadCrumb.get(0));
                    }
                }
            }
            bufferedReader.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(lineNum);
        System.out.println("解析shopInfo.json结束 origin shop size " + shopCategoryMap.size());
        System.out.println("category的个数：" + categoryShopsMap.size());
        /**
         * 输出每个category下的shop个数
         */
        System.out.println("输出每个category下的shop个数");
        for(Map.Entry<String, Set<String>>me : categoryShopsMap.entrySet()){
            System.out.println(me.getKey() + " " + me.getValue().size());
        }
    }


    public static boolean isDouble(String str) {
        Pattern pattern = Pattern.compile("^[-\\+]?[.\\d]*$");
        return pattern.matcher(str).matches();
    }

    public static String getTimeStamp(String string) {
        Pattern pattern = Pattern.compile("[0-9]{2}[-][0-9]{2}[-][0-9]{2}");
        Matcher matcher = pattern.matcher(string);

        String dateStr = null;
        if (matcher.find()) {
            dateStr = matcher.group(0);
        }

        String str = dateStr.toString();
//        System.out.println(str);
        return str;
    }

    public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
        boolean mapping=false;
        reviewProcess(mapping);
        /**
         * 统计所用
         */
//        test();
//        getShopCategory(shopInfoPath);
//        String filteredPOIDescriptionPath = "D:\\我的工作\\投稿论文\\彭宏伟计算机学报\\实验所用数据\\Dianping_v2\\review_social\\1DianpingDescription1010100.txt";
//        processDianpingFilteredData(filteredPOIDescriptionPath);
    }
    public static void test(){
        Map<String,String> a=new HashMap<>();
        a.put("a","123");
        a.put("b","4545");
        FileOperation.writeNotAppdend("D:\\cbd\\！！毕业论文\\给贺小木的数据处理代码+数据\\wkqdianping\\test.txt",a.toString());
    }
}
