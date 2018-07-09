package process.others;

import com.alibaba.fastjson.JSON;
import data.Review;
import data.ShopInfo;
import util.*;
import data.*;
import org.apache.commons.lang.StringUtils;
import util.FileOperation;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * 该类目的是统计某个城市中各
 * 个菜系的统计数据：包括餐厅总数，菜品总数以及用户评论总数，排序后找出热门菜系，与官网上的“点评大数据”对比，然后
 * 选热门菜系来进行推荐(这里是不经过过滤的)
 * shopinfo.json的编码问题，在notepad++中设置转为utf-8（有BOM）编码
 * 1.按频率过滤前按菜系统计数据
 * 2.选出若干菜系构成新的子集
 */
public class CategoryStatistic {

    static public Set<String> shopIdSet;
    //    static public Map<String,Map<String,String>> checkInFrequenceMap;
    static public Map<String, ShopInfo> shopInfoMap;
    static public Map<String, String> shopCategoryMap;
    static  Map<String, Map<String, Map<String, Integer>>> checkInRecordMap;//user-shop-dish-frequency record
    static Map<String, Map<String,Integer>> globalshopDishSetMap;// shop-dish-评论数据中shop中的这道dish出现的次数
    static Map<String, Set<String>> globalUserItemSetMap; // <user,<shop>>
    static Map<String,Map<String,Set<String>>>globalDishPostSetMap; //dish-<user,<shop>>
    static Set [][] categoryStatisticArray ; //每个菜系对应三个hashset,依次：记录评论id,餐厅id和dish id
    static int [][] categoryRatingArray ;
    static  Map<String, Integer>categoryMap ;
    static  Map<Integer, String>categoryMapInverse ;

    public static void main(String[] args){

        String cityCode = "1_categoryFiltered";


        String shopInfoPath = "D:\\Data\\OriginData\\wkq_DianpingData\\上海数据_wkq预处理过\\shopInfo.json ";
        String reviewPath= "D:\\Data\\OriginData\\wkq_DianpingData\\上海数据_wkq预处理过\\shopReviewExtend.json";
        String desPath = "D:\\Data\\ProcessedData\\Dianping\\" + cityCode + "\\";

//        int userLeastCount = 10; //每个用户去过10个以上的餐厅
//        int itemLeastCount = 10; //每家餐厅被10个以上的用户访问过
//        int dishLeastCount=10; // 每家餐厅至少有10道菜
//        int dishReviewedLeastCount=10;//每道菜至少被10个不同的<user-item>对访问过
        boolean isMapping = false; //保存的签到记录中id是否映射为顺序id, true为映射，false为保留原来的id
        System.out.println("shopInfoPath "+ shopInfoPath  + " reviewPath"+ reviewPath + " despath " + desPath);
//        System.out.println("filter condition "+userLeastCount+" "+itemLeastCount+" "+dishLeastCount+" "+dishReviewedLeastCount);
        
        int iteration = -1;//表示会不断迭代直到同时满足least限制条件

        getShopInfo(shopInfoPath);
        getCategoryStatistics(reviewPath);
//        Map<String, Set<String>> userItemSetMap =  getUserItemSetMapofRecord();
//        Map<String,Map<String,Integer>> shopDishSetMap = getShopDishSetMapofRecord();//Record(shop,dish，dishOccurenceNum)
//        Map<String,Map<String,Set<String>>> dishPostSetMap = getDishPostSetMap();
//
//        Set<String>[] resultSet = dishDataSplit.CircularFilter.shopCountFilter(checkInRecordMap,
//                userItemSetMap, shopDishSetMap, dishPostSetMap, userLeastCount, itemLeastCount, dishLeastCount, dishReviewedLeastCount, iteration);
//        globalshopDishSetMap = dishDataSplit.CircularFilter.get_shopDishSetMap();
//        globalUserItemSetMap = dishDataSplit.CircularFilter.get_userItemSetMap();
//        globalDishPostSetMap = dishDataSplit.CircularFilter.get_dishPostSetMap();

        outputStatistic(categoryStatisticArray, categoryRatingArray, desPath);
    }


    public static Map<String, Set<String>>  getUserItemSetMapofRecord() {
        int userItemPairNum=0;
        Map<String, Set<String>> userItemSetMap = new HashMap<>();
        for (Map.Entry<String, Map<String,Map<String,Integer>>> userItemRecEntry :checkInRecordMap.entrySet()) {
            String tempUser = userItemRecEntry.getKey();
            Set<String> tempItemSet = new HashSet<>();
            Map<String,Map<String,Integer>> tempItemRecMap = userItemRecEntry.getValue();
            for(String tempItem : tempItemRecMap.keySet()){
                userItemPairNum += 1;
                tempItemSet.add(tempItem);
            }
            if(tempItemSet.size() > 0)
                userItemSetMap.put(tempUser, tempItemSet);
        }
        System.out.println("与shopinfo取交集后review中user-item Pair的数量 "+userItemPairNum);
        return userItemSetMap;
    }

    /**
     * 得到每个餐馆对应的菜品集合及每道菜在训练集中出现的总次数
     * @return
     */
    public static Map<String, Map<String,Integer>>  getShopDishSetMapofRecord(){
        Map<String, Map<String,Integer>> shopDishSetMap = new HashMap<>();
        for (Map.Entry<String, Map<String,Map<String,Integer>>> userItemRecEntry :checkInRecordMap.entrySet()) {
            String tempUser = userItemRecEntry.getKey();
            for (Map.Entry<String,  Map<String,Integer>>  itemDishRecEntry :checkInRecordMap.get(tempUser).entrySet()) {
                String tempItem = itemDishRecEntry.getKey();
                if(shopIdSet.contains(tempItem)){
                    if (!shopDishSetMap.containsKey(tempItem))
                        shopDishSetMap.put(tempItem, new HashMap<>());
                    for (Map.Entry<String, Integer> DishRecEntry : checkInRecordMap.get(tempUser).get(tempItem).entrySet()) {
                        String dish = DishRecEntry.getKey();
                        if (!shopDishSetMap.get(tempItem).containsKey(dish))
                            shopDishSetMap.get(tempItem).put(dish,DishRecEntry.getValue());

                        else
                            shopDishSetMap.get(tempItem).put(dish,shopDishSetMap.get(tempItem).get(dish) + DishRecEntry.getValue());

                    }
                }
            }
        }
        return shopDishSetMap;
    }

    /**
     * 记录每道菜被哪些用户在哪家店访问过
     * @return
     */
    public static Map<String,Map<String,Set<String>>> getDishPostSetMap() {
        Map<String,Map<String,Set<String>>>dishPostSetMap=new HashMap<>();
        for (Map.Entry<String, Map<String,Map<String,Integer>>> userItemRecEntry :checkInRecordMap.entrySet()) {
            String tempUser = userItemRecEntry.getKey();
            for (Map.Entry<String, Map<String,Integer>> itemDishRecEntry :checkInRecordMap.get(tempUser).entrySet()) {
                String tempItem = itemDishRecEntry.getKey();
                for (Map.Entry<String, Integer> DishRecEntry : itemDishRecEntry.getValue().entrySet()) {
                    String dish = DishRecEntry.getKey();
                    if (!dishPostSetMap.containsKey(dish))
                        dishPostSetMap.put(dish,new HashMap());
                    if(!dishPostSetMap.get(dish).containsKey(tempUser))
                        dishPostSetMap.get(dish).put(tempUser,new HashSet<>());
                    dishPostSetMap.get(dish).get(tempUser).add(tempItem);
                }
            }
        }
        return  dishPostSetMap;
    }

    /**
     * 这里会把属性不全的shop过滤掉，因此过滤掉了大量的shop
     * \\todo 不用tags来过滤，得到的数据量会比较大，属性不全的shop有很多
     * 不用tag来过滤的结果，引入了很多杂乱的非菜系的类别
     * @param shopInfoPath
     */
    public static void getShopInfo(String shopInfoPath){
        System.out.println("解析shopInfo.json开始");
        shopIdSet = new HashSet<>();
        shopInfoMap = new HashMap<>();
        shopCategoryMap = new HashMap<>();
        categoryMap = new HashMap<>();
        categoryMapInverse = new HashMap<>();

        String read ;
        FileInputStream file ;
        BufferedReader bufferedReader ;
        ShopInfo shop ;

        try {
            file = new FileInputStream(shopInfoPath);
            bufferedReader = new BufferedReader(new InputStreamReader(file, "UTF-8"));
            while ((read = bufferedReader.readLine()) != null) {
                shop = JSON.parseObject(read, ShopInfo.class);
                String shopId = shop.getShopId();
                String lng = shop.getLng();
                String lat = shop.getLat();
                String region = shop.getRegion();
                String tags = shop.getTags();
                String breadcrumb = shop.getBreadcrumb();

                if(StringUtils.isNumeric(shopId) && lng!=null &&!lng.equals("") && lat!=null &&!lat.equals("") && isDouble(lng) && isDouble(lat)
                        && region!=null && !region.equals("") &&tags!=null&& !tags.trim().equals("")&&breadcrumb!=null&& !breadcrumb.trim().equals("")) {
                    //将没有tag属性的餐厅也包含进去
//                if(StringUtils.isNumeric(shopId) && lng!=null &&!lng.equals("") && lat!=null &&!lat.equals("") && isDouble(lng) && isDouble(lat)
//                        && region!=null && !region.equals("")  && breadcrumb!=null && !breadcrumb.trim().equals("")) {
                    String[] breadCrumb = breadcrumb.substring(2, breadcrumb.length() - 2).split("\",\"");
                    if(breadCrumb[0].equals("上海餐厅")){
                    shopIdSet.add(shopId);
                    shopInfoMap.put(shopId, shop);

                    String category;
                    //沿用彭宏伟代码中的思路，
                    if (breadCrumb.length >= 4)
                        category = breadCrumb[3];
                    else
                        category = breadCrumb[breadCrumb.length - 1];
//                    String category = breadCrumb[breadCrumb.length - 1];//取breadcrumb的最后一个类别
                    shopCategoryMap.put(shopId, category);
                    if (!categoryMap.containsKey(category)) {
                        categoryMap.put(category, categoryMap.size());
                        categoryMapInverse.put(categoryMapInverse.size(), category);
                        System.out.println("category " + category);
                    }
                }
                }else{
//                    System.out.println(read);
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

        System.out.println("解析shopInfo.json结束 origin shop size "+shopInfoMap.size());
    }

    /**
     * 从签到数据中得到各个菜系的6个统计数据，然后输出到文本文件中
     * @param reviewPath
     */
    public static  void getCategoryStatistics(String reviewPath){
        System.out.println("checkinrecord解析review.json开始 " + categoryMap.size());
        String read;
        FileInputStream file;
        BufferedReader bufferedReader;
        Review review;
        checkInRecordMap = new HashMap<>();
//        categoryMap = new HashMap<>();
//        categoryMapInverse = new HashMap<>();
        //符合类型要用循环来初始化
        categoryStatisticArray = new HashSet[categoryMap.size()][3]; //每个菜系对应三个hashset,依次：记录评论id,餐厅id和dish id
        Set<String>shopNotInShopInfo = new HashSet<>();
        for(int i = 0; i < categoryMap.size(); i++){
            for(int j = 0; j < 3; j ++){
                categoryStatisticArray[i][j] = new HashSet();
            }
        }
        categoryRatingArray = new int[categoryMap.size()][3];//每个菜系对应三个整数,依次：记录service rating总分，taste rating总分，environment rating总分

        try {
            file = new FileInputStream(reviewPath);
            bufferedReader = new BufferedReader(new InputStreamReader(file, "UTF-8"));
            int reviewNum = 0;
            while ((read = bufferedReader.readLine()) != null) {
                review = JSON.parseObject(read, Review.class);
//                String reviewId = review.getReviewId();
                String userId = review.getUserId();
                String shopId = review.getShopId();
                Set<String> favDishes = review.getFavDishes();
                String category = shopCategoryMap.get(shopId);
                int service = review.getService();
                int taste = review.getTaste();
                int condition = review.getCondition();

                if(!shopIdSet.contains(shopId)){
//                    System.out.println("review中有shopinfo中没有的餐厅 "+shopId);
//                    System.exit(-3);
                    shopNotInShopInfo.add(shopId);
                }

                //这里关注的是每条评论中的店要找到对应的菜系，没有favorite字段也没有关系
                if(shopIdSet.contains(shopId) && StringUtils.isNumeric(userId) && StringUtils.isNumeric(shopId) && !category.equals("")) {
//                    System.out.println(shopId + " " + category);
//                    if(!categoryMap.containsKey(category)){
//                        categoryMapInverse.put(categoryMapInverse.size(), category);
//                        categoryMap.put(category, categoryMap.size());
////                        System.out.println("category " + category);
//                    }
                    categoryRatingArray[categoryMap.get(category)][0] += service;
                    categoryRatingArray[categoryMap.get(category)][1] += taste;
                    categoryRatingArray[categoryMap.get(category)][2] += condition;

                    categoryStatisticArray[categoryMap.get(category)][0].add(String.valueOf(reviewNum));
                    categoryStatisticArray[categoryMap.get(category)][1].add(shopId);

                    if(favDishes != null) {
                        for (String dish : favDishes) {
//                            if(dish.equals("重庆酸辣粉")){
//                                System.out.println("重庆酸辣粉 "+shopId);
//                            }
                            categoryStatisticArray[categoryMap.get(category)][2].add(dish);
                        }
                    }
                }else{

                }
                reviewNum ++;
            }
            bufferedReader.close();
            System.out.println("checkinrecord解析review.json结束");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(shopNotInShopInfo.size() + " " + shopNotInShopInfo.toString());
    }

    /**
     * 输出每个菜系的统计信息，其中菜系映射回原来的string,同时输出总数和源数据并根据指标进行排序，取出数据的子集
     * Notag.txt后缀的文件表示输出的餐厅有些不包含tags字段
     * @param categoryStatisticArray
     * @param categoryRatingArray
     * @param desPath
     */
    public static void outputStatistic(Set[][]categoryStatisticArray, int [][] categoryRatingArray, String desPath){
        StringBuilder categoryStatisticStr = new StringBuilder();
//        categoryStatisticStr.append("category, 评论总数, 评论id, 餐厅总数, 餐厅id, dish总数, dish id\n");
        categoryStatisticStr.append("category\t评论总数\t餐厅总数\tdish总数\n");
        StringBuilder categoryRatingStr = new StringBuilder();
        categoryRatingStr.append("category, service rating平均分，taste rating平均分，environment rating平均分\n");

        for (int i = 0; i < categoryStatisticArray.length; i++) {
            String category = categoryMapInverse.get(i);
            categoryStatisticStr.append("类别\t" + category + "\t");
            for (int j = 0; j < categoryStatisticArray[i].length; j++) {
                //循环遍历数组中的每个元素
                Set stasticSet = categoryStatisticArray[i][j];
                categoryStatisticStr.append("size: " + stasticSet.size() + " ");
//                if(j == 1 || j == 2) {
//                    Iterator<String> it = stasticSet.iterator();
//                    while (it.hasNext()) {
//                        categoryStatisticStr.append(it.next() + " ");
//                    }
//                }
            }
            categoryStatisticStr.append("\n");
        }
//        dataPreprocess.FileOperation.writeNotAppdend(desPath + "categoryStatisticArray_shopNotag.txt", categoryStatisticStr.toString());
//        dataPreprocess.FileOperation.writeNotAppdend(desPath + "categoryOnlyStatisticArray.txt", categoryStatisticStr.toString());
        FileOperation.writeNotAppdend(desPath + "categoryStatisticArray.txt", categoryStatisticStr.toString());
        System.out.println("categoryStatisticArray.txt 写入结束");

        for (int i = 0; i < categoryRatingArray.length; i++) {
            String category = categoryMapInverse.get(i);
            categoryRatingStr.append("类别 " + category + " " +"\n");
            for (int k = 0; k < categoryRatingArray[i].length; k++) {
                double meanRating = categoryRatingArray[i][k] * 1.0 / categoryStatisticArray[i][0].size();
                categoryRatingStr.append(meanRating + " ");
            }
            categoryRatingStr.append("\n");
        }
//        dataPreprocess.FileOperation.writeNotAppdend(desPath + "categoryRatingArray_shopNotag.txt", categoryRatingStr.toString());
        FileOperation.writeNotAppdend(desPath + "categoryOnlyRatingArray.txt", categoryRatingStr.toString());
        System.out.println("categoryRatingArray.txt 写入结束");
    }

    /**
     * 为什么这里关于shopinfo和dianpingcheckin的输出文件中输出的innerid是按顺序的，因为map的遍历本身应该是乱序的
     * 而itemmap,usermap,dishmap中的输出是乱序的
     * @param resultSet
     * @param desPath
     * @param userLeastCount
     * @param itemLeastCount
     * @param dishLeastCount
     */
    public static void outputResultWithMap(Map<String, Set<String>>userItemSetMap,
                                           Set<String>[] resultSet ,String desPath,int userLeastCount,int itemLeastCount,int dishLeastCount,int  dishReviewedLeastCount,boolean isMapping){
        //做映射用的map
        Map<String, Integer> userMap = new ConcurrentHashMap<>();
        Map<String, Integer> itemMap = new ConcurrentHashMap<>();
        Map<String, Integer> dishMap = new ConcurrentHashMap<>();

        Set<String> userSet = resultSet[0] ;
        Set<String> itemSet = resultSet[1] ;
        Set<String> dishSet = resultSet[2] ;
        /**
         * 直接使用userset,itemset和shopdishes来进行映射,映射从0开始
         */
        int id=0;

        for(String user:userSet){
            userMap.put(user,id);
            id++;
        }

        id=0;
        for(String item:itemSet){
            itemMap.put(item,id);
            id++;
        }

        id = 0;
        for(String dish : dishSet){
            dishMap.put(dish,id);
            id++;
        }

        StringBuilder shopDishStr = new StringBuilder();
        for (Map.Entry<String, Map<String,Integer>> shopDishEntry :globalshopDishSetMap.entrySet()){
            String shop = shopDishEntry.getKey();
            if(isMapping) {
                shopDishStr.append(itemMap.get(shop) + " ");
                for (String dish : shopDishEntry.getValue().keySet()) {
                    shopDishStr.append(dishMap.get(dish) + " ");
                }
            }
            else{
                shopDishStr.append(shop + " ");
                for (String dish : shopDishEntry.getValue().keySet()) {
                    shopDishStr.append(dish + " ");
                }
            }
            shopDishStr.append("\n");
        }
        FileOperation.writeNotAppdend(desPath + "DianpingShopDishes" + isMapping + userLeastCount+itemLeastCount+dishLeastCount+dishReviewedLeastCount+".txt", shopDishStr.toString());
        /**
         * 输出dishpostset
         */
        StringBuilder dishPostStr = new StringBuilder();
        for (Map.Entry<String, Map<String,Set<String>>> dishPostEntry :globalDishPostSetMap.entrySet()){
            String dish =  dishPostEntry.getKey();
            dishPostStr.append(dishMap.get(dish)+" ");
            for(Map.Entry<String,Set<String>> PostEntry: dishPostEntry.getValue().entrySet()) {
                if(isMapping) {
                    dishPostStr.append(userMap.get(PostEntry.getKey()) + "<");

                    for (Iterator it = PostEntry.getValue().iterator(); it.hasNext();) {
                        dishPostStr.append(itemMap.get(it.next())+" ");
                    }
                }
                else{
                    dishPostStr.append(PostEntry.getKey() + "<");

                    for (Iterator it = PostEntry.getValue().iterator(); it.hasNext();) {
                        dishPostStr.append(it.next()+" ");
                    }
                }

                dishPostStr.append(">,");
            }
            dishPostStr.append("\n");
        }
        FileOperation.writeNotAppdend(desPath + "DianpingDishPost" + isMapping + userLeastCount+itemLeastCount+dishLeastCount+dishReviewedLeastCount+".txt", dishPostStr.toString());

        /**
         * 输出usermap,itemmap和dishmap
         */
        StringBuilder str = new StringBuilder();

        for (Map.Entry<String, Integer> userMapEntry :userMap.entrySet()) {
            str.append(userMapEntry.getKey()+" "+userMapEntry.getValue()+"\n");
        }
        FileOperation.writeNotAppdend(desPath + "userMap" + isMapping + userLeastCount+itemLeastCount+dishLeastCount+".txt", str.toString());

        str = new StringBuilder();
        for (Map.Entry<String, Integer> itemMapEntry :itemMap.entrySet()) {
            str.append(itemMapEntry.getKey()+" "+itemMapEntry.getValue()+"\n");
        }
        FileOperation.writeNotAppdend(desPath + "itemMap" + isMapping + userLeastCount+itemLeastCount+dishLeastCount+".txt", str.toString());

        str = new StringBuilder();
        for (Map.Entry<String, Integer> dishMapEntry :dishMap.entrySet()) {
            str.append(dishMapEntry.getKey()+" "+dishMapEntry.getValue()+"\n");
        }
        FileOperation.writeNotAppdend(desPath + "dishMap"+ isMapping + userLeastCount+itemLeastCount+dishLeastCount+".txt", str.toString());

        Map<String,Set<String>> userItemPair = new HashMap<>();
        Map<String, Set<String>> itemUserPair = new HashMap<>();
        Map<String, Set<String>> shopDishSetMap = new HashMap<>();
        Map<String,Map<String,Set<String>>> dishPostSetMap = new HashMap<>(); //dish-<user,<shop>>



        //输出时直接使用最终确定的userset和itemset,shopdishset来过滤checkInRecordMap
        Set<String>outputFinalDishSet = new HashSet<>();

        StringBuilder userItemFreStr = new StringBuilder();
        for (Map.Entry<String, Map<String,Map<String,Integer>>> userItemFreEntry :checkInRecordMap.entrySet()) {
            String tempUser = userItemFreEntry.getKey();
            if (userSet.contains(tempUser)) {
                Map<String, Map<String, Integer>> tempItemFreMap = userItemFreEntry.getValue();
                for (String tempItem : tempItemFreMap.keySet()) {
                    if (itemSet.contains(tempItem)) {
                        Map<String, Integer> dishes = tempItemFreMap.get(tempItem);
                        for (Map.Entry<String, Integer> dishesEntry : dishes.entrySet()) {
                            String dish = dishesEntry.getKey();

//                            if(dishSet.contains(dish)){直接用dishSet与globalshopdish比较结果没有差别
                            if(globalshopDishSetMap.get(tempItem).keySet().contains(dish)) {

                                if (!userItemPair.containsKey(tempUser)) {
                                    userItemPair.put(tempUser, new HashSet<>());
                                }
                                userItemPair.get(tempUser).add(tempItem);

                                if (!itemUserPair.containsKey(tempItem)) {
                                    itemUserPair.put(tempItem, new HashSet<>());
                                }
                                itemUserPair.get(tempItem).add(tempUser);

                                if (!shopDishSetMap.containsKey(tempItem)) {
                                    shopDishSetMap.put(tempItem, new HashSet<>());
                                }
                                shopDishSetMap.get(tempItem).add(dish);

                                if (!dishPostSetMap.containsKey(dish)) {
                                    dishPostSetMap.put(dish, new HashMap<>());
                                }

                                if (!dishPostSetMap.get(dish).containsKey(tempUser)) {
                                    dishPostSetMap.get(dish).put(tempUser, new HashSet<>());
                                }

                                dishPostSetMap.get(dish).get(tempUser).add(tempItem);

                                outputFinalDishSet.add(dish);
                                int frequency = dishesEntry.getValue();
                                if (isMapping) {
                                    userItemFreStr.append(userMap.get(tempUser));
                                    userItemFreStr.append(" ");
                                    userItemFreStr.append(itemMap.get(tempItem));
                                    userItemFreStr.append(" ");
                                    userItemFreStr.append(dishMap.get(dish));//key值是dish
                                    userItemFreStr.append(" ");
                                    userItemFreStr.append(frequency);//key值是dish
                                    userItemFreStr.append("\n");
                                } else {
                                    userItemFreStr.append(tempUser);
                                    userItemFreStr.append(" ");
                                    userItemFreStr.append(tempItem);
                                    userItemFreStr.append(" ");
                                    userItemFreStr.append(dish);//key值是dish
                                    userItemFreStr.append(" ");
                                    userItemFreStr.append(frequency);//key值是dish
                                    userItemFreStr.append("\n");
                                }
                            }
                        }
                    }
                }
            }
        }
        FileOperation.writeNotAppdend(desPath + "DianpingCheckin" + isMapping + userLeastCount+itemLeastCount+dishLeastCount+dishReviewedLeastCount+".txt", userItemFreStr.toString());

        //compute user-item pair num in checkInRecordMap after fitering
        int userItemPairNum = 0;

        Set<String>outputFinalItemSet = new HashSet<>();
        for (Map.Entry<String, Set<String>> userItemFreEntry : userItemPair.entrySet()) {
            userItemPairNum +=  userItemFreEntry.getValue().size();
            for(String item : userItemFreEntry.getValue())
                outputFinalItemSet.add(item);
        }

        Iterator<Map.Entry<String, Set<String>>> iterator = shopDishSetMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Set<String>> tempKeyValueSetEntry = iterator.next();
            Set<String> tempValueSet = tempKeyValueSetEntry.getValue();
            if (tempValueSet.size() < dishLeastCount) {
                System.exit(1);
            }
        }
//
//
        iterator = userItemPair.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Set<String>> tempKeyValueSetEntry = iterator.next();
            Set<String> tempValueSet = tempKeyValueSetEntry.getValue();
            if (tempValueSet.size() < userLeastCount) {
                System.exit(2);
            }
        }
//
        iterator = itemUserPair.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Set<String>> tempKeyValueSetEntry = iterator.next();
            Set<String> tempValueSet = tempKeyValueSetEntry.getValue();
            if (tempValueSet.size() < itemLeastCount) {
                System.exit(3);
            }
        }
//
//
//
        Iterator<Map.Entry<String, Map<String, Set<String>>>> iterator2 = dishPostSetMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Map<String, Set<String>>> tempKeyValueSetEntry = iterator2.next();
            Map<String, Set<String>> tempValueSet = tempKeyValueSetEntry.getValue();//dish对应的user-shop对
            int size=0;
            for (Map.Entry<String, Set<String>>userItemEntry : tempValueSet .entrySet()) {
                size += userItemEntry.getValue().size();
            }
            if(size < dishReviewedLeastCount) {
                System.exit(4);
            }
        }

        System.out.println("过滤后checkInRecordMap中的userItemPairNum " + userItemPairNum + " usernum" +
                userItemPair.size() +" shopnum " + outputFinalItemSet.size() + " dishnum " + outputFinalDishSet.size());
        /**
         * 使用留一法来分训练集和测试集,把每个user对应的第一个shop分配给了测试集，
         * 当用户对应的只有一个shop时(不会有这种情况，已经保证每个用户去过的店大于10家了)
         * 将这个shop分给训练集,当有多个时，就把第一个分到测试集中
         */
        Set<String>trainUserSet = new HashSet<>();
        Set<String>testUserSet = new HashSet<>();

        Set<String>trainShopSet = new HashSet<>();
        Set<String>testShopSet = new HashSet<>();

        Set<String>trainDishSet = new HashSet<>();
        Set<String>testDishSet = new HashSet<>();

        boolean flag ;

        StringBuilder userItemDishTrainStr = new StringBuilder();
        StringBuilder userItemDishTestStr = new StringBuilder();
        for (Map.Entry<String, Map<String,Map<String,Integer>>> userItemFreEntry :checkInRecordMap.entrySet()) {
            String tempUser = userItemFreEntry.getKey();
            if (userSet.contains(tempUser)) {
                Map<String, Map<String, Integer>> tempItemFreMap = userItemFreEntry.getValue();
//                if(userItemSetMap.get(tempUser).size() > 1){
                flag = true;
                for (String tempItem : tempItemFreMap.keySet()) {
                    if (itemSet.contains(tempItem)) {
                        if (flag) {
//                                testflag++;
                            Map<String, Integer> dishes = tempItemFreMap.get(tempItem);
                            for (Map.Entry<String, Integer> dishesEntry : dishes.entrySet()) {
                                String dish = dishesEntry.getKey();
//                                    if(dishSet.contains(dish)){
                                if(globalshopDishSetMap.get(tempItem).keySet().contains(dish)){
                                    testUserSet.add(tempUser);
                                    testShopSet.add(tempItem);
                                    testDishSet.add(dish);

                                    int frequency = dishesEntry.getValue();
                                    if(isMapping) {
                                        userItemDishTestStr.append(userMap.get(tempUser));
                                        userItemDishTestStr.append(" ");
                                        userItemDishTestStr.append(itemMap.get(tempItem));
                                        userItemDishTestStr.append(" ");
                                        userItemDishTestStr.append(dishMap.get(dish));
                                        userItemDishTestStr.append(" ");
                                        userItemDishTestStr.append(frequency);
                                    }
                                    else{
                                        userItemDishTestStr.append(tempUser);
                                        userItemDishTestStr.append(" ");
                                        userItemDishTestStr.append( tempItem);
                                        userItemDishTestStr.append(" ");
                                        userItemDishTestStr.append( dish );
                                        userItemDishTestStr.append(" ");
                                        userItemDishTestStr.append(frequency);
                                    }
                                    userItemDishTestStr.append("\n");
                                    flag = false;
                                }
                            }
                        } else {
                            Map<String, Integer> dishes = tempItemFreMap.get(tempItem);
                            for (Map.Entry<String, Integer> dishesEntry : dishes.entrySet()) {
                                String dish = dishesEntry.getKey();
//                                    if(dishSet.contains(dish)) {
                                if(globalshopDishSetMap.get(tempItem).keySet().contains(dish)){
                                    trainUserSet.add(tempUser);
                                    trainShopSet.add(tempItem);
                                    trainDishSet.add(dish);
                                    int frequency = dishesEntry.getValue();
                                    if(isMapping) {
                                        userItemDishTrainStr.append(userMap.get(tempUser));
                                        userItemDishTrainStr.append(" ");
                                        userItemDishTrainStr.append(itemMap.get(tempItem));
                                        userItemDishTrainStr.append(" ");
                                        userItemDishTrainStr.append(dishMap.get(dish));
                                        userItemDishTrainStr.append(" ");
                                        userItemDishTrainStr.append(frequency);
                                    }
                                    else{
                                        userItemDishTrainStr.append( tempUser);
                                        userItemDishTrainStr.append(" ");
                                        userItemDishTrainStr.append( tempItem);
                                        userItemDishTrainStr.append(" ");
                                        userItemDishTrainStr.append( dish);
                                        userItemDishTrainStr.append(" ");
                                        userItemDishTrainStr.append(frequency);
                                    }
                                    userItemDishTrainStr.append("\n");
                                }
                            }
                        }
                    }
                }
//                }
//                //如果只有1个user-shop对，就直接放到训练集中
//                else{
//                    System.exit(-5);
//                    System.out.println("only one post: "+tempUser+": "+userMap.get(tempUser));
//                    for (String tempItem : tempItemFreMap.keySet()) {
//                        if (itemSet.contains(tempItem)) {
//                            Map<String, Integer> dishes = tempItemFreMap.get(tempItem);
//                            for (Map.Entry<String, Integer> dishesEntry : dishes.entrySet()) {
//                                String dish = dishesEntry.getKey();
//                                if(dishSet.contains(dish)) {
//                                    int frequency = dishesEntry.getValue();
//                                    userItemDishTrainStr.append(userMap.get(tempUser));
//                                    userItemDishTrainStr.append(" ");
//                                    userItemDishTrainStr.append(itemMap.get(tempItem));
//                                    userItemDishTrainStr.append(" ");
//                                    userItemDishTrainStr.append(dishMap.get(dish));
//                                    userItemDishTrainStr.append(" ");
//                                    userItemDishTrainStr.append(frequency);
//                                    userItemDishTrainStr.append("\n");
//                                }
//                            }
//                        }
//                    }
//                }
            }
        }

        System.out.println("Train " + trainUserSet.size()+" "+trainShopSet.size()+" "+trainDishSet.size() +
                "Test "+ testUserSet.size()+" "+testShopSet.size()+" "+testDishSet.size());

        //TODO 输出train集合和test集合中user,shop,dish,user-item对的统计信息，并给出train集合中shop对应的dish的最大值和最小值，以及dish对应的pair个数的最大值和最小值
        FileOperation.writeNotAppdend
                (desPath + "DianpingCheckin" +isMapping+
                        userLeastCount + itemLeastCount + dishLeastCount + dishReviewedLeastCount+ "train.txt", userItemDishTrainStr.toString());
        FileOperation.writeNotAppdend
                (desPath + "DianpingCheckin"+isMapping+
                        userLeastCount + itemLeastCount + dishLeastCount + dishReviewedLeastCount + "GenerateFeatures.txt", userItemDishTestStr.toString());



        final int[] filteredShopNum = {0};
        StringBuilder locationStr = new StringBuilder();
        StringBuilder tagStr = new StringBuilder();
        StringBuilder descStr = new StringBuilder();
        StringBuilder regionStr = new StringBuilder();
        shopInfoMap.entrySet().stream().forEach(tempEntry -> {
            ShopInfo shopInfo = tempEntry.getValue();
            if (itemSet.contains(shopInfo.getShopId())) {
                filteredShopNum[0]++;
                if(isMapping) {
                    locationStr.append(itemMap.get(shopInfo.getShopId()));
                }
                else{
                    locationStr.append(shopInfo.getShopId());
                }
                locationStr.append(" ");
                locationStr.append(shopInfo.getLat());
                locationStr.append(" ");
                locationStr.append(shopInfo.getLng());
                locationStr.append("\n");

                String[] tags = shopInfo.getTags().substring(2, shopInfo.getTags().length() - 2).split("\",\"");
                if(isMapping) {
                    tagStr.append(itemMap.get(shopInfo.getShopId()));
                }
                else{
                    tagStr.append(shopInfo.getShopId());
                }
                for (String tag : tags) {
                    tagStr.append(" ## ");
                    tagStr.append(tag);
                }
                tagStr.append("\n");
                //descs:  "breadcrumb":["上海餐厅","闵行区","莘庄龙之梦","咖啡厅","更多咖啡厅"]
                String[] descs = shopInfo.getBreadcrumb().substring(2, shopInfo.getBreadcrumb().length() - 2).split("\",\"");//正则表达式意为以“，”为分隔符
                if(isMapping) {
                    descStr.append(itemMap.get(shopInfo.getShopId())); //descStr是餐厅的类别
                }
                else{
                    descStr.append(shopInfo.getShopId());
                }
                for (String desc : descs) {
                    descStr.append(" ## ");
                    descStr.append(desc);
                }
                descStr.append("\n");

                if(isMapping) {
                    regionStr.append(itemMap.get(shopInfo.getShopId()));
                }
                else{
                    regionStr.append(shopInfo.getShopId());
                }
                regionStr.append(" ");
                regionStr.append(shopInfo.getRegion());
                regionStr.append("\n");

            }
        });

        System.out.println("location num after filtering: " + filteredShopNum[0]);

        FileOperation.writeNotAppdend(desPath + "DianpingLocation" +isMapping + userLeastCount+itemLeastCount +dishLeastCount+dishReviewedLeastCount+ ".txt", locationStr.toString());
        FileOperation.writeNotAppdend(desPath + "DianpingTag" + isMapping + userLeastCount+itemLeastCount +dishLeastCount+dishReviewedLeastCount+ ".txt", tagStr.toString());
        FileOperation.writeNotAppdend(desPath + "DianpingDescription" + isMapping + userLeastCount+itemLeastCount  +dishLeastCount+dishReviewedLeastCount+ ".txt", descStr.toString());
        FileOperation.writeNotAppdend(desPath + "DianpingRegion" +isMapping + userLeastCount+itemLeastCount +dishLeastCount+dishReviewedLeastCount+ ".txt", regionStr.toString());

    }

    public static boolean isDouble(String str) {
        Pattern pattern = Pattern.compile("^[-\\+]?[.\\d]*$");
        return pattern.matcher(str).matches();
    }

}


