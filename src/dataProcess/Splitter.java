package dataProcess;

import com.alibaba.fastjson.JSON;
import data.Review;
import data.ShopInfo;
import org.apache.commons.lang.StringUtils;
import util.FileOperation;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * 处理后的数据是user,restaurant,dish,frequency格式，每一条表示用户，餐厅，菜品，用户在某个餐厅吃了某道菜的次数
 * shopinfo.json的编码问题，在notepad++中设置转为utf-8无BOM格式编码
 * 对shopReviewExtend.json和shopinfo中的shop取了交集
 * <p>
 * 利用用户的评论和商店的评论来进行数据的过滤
 */
public class Splitter {
    /**
     * shopInfo.json中原始和过滤后的统计信息
     */
    static int originShopNum;
    //shopInfo.json中的shopid集合
    static public Set<String> shopInfoIdSet;
    static public Map<String, ShopInfo> shopInfoMap;
    static public Map<String, String> shopCategoryMap;

    /**
     * 原始的shopReviewExtend.json中的统计信息
     */
    static int originReviewCount;
    static Set<String> originUserSet;
    static Set<String> originShopSet;
    static Set<String> originDishSet;

    /**
     * 过滤评论数据会用到的数据结构
     */
    static Map<String, Map<String, Map<String, Integer>>> checkInRecordMap;//user-shop-dish-frequency record
    static Map<String, Map<String, Integer>> globalshopDishSetMap;// shop-dish-评论数据中shop中的这道dish出现的次数
    static Map<String, Set<String>> globalUserItemSetMap; // <user,<shop>>
    static Map<String, Map<String, Set<String>>> globalDishPostSetMap; //dish-<user,<shop>>
    static List<String> categoryFilterList;

    /**
     * shopReviewExtend.json与shopInfo.json取交集，并经过一定条件过滤后的统计信息
     */
    static int tripleNum;
    static int reviewCount;
    static Set<String> userSet = new HashSet<>();
    static Set<String> shopSet = new HashSet<>();
    static Set<String> dishSet = new HashSet<>();

    /**
     * 经过上一步后，再对checkInRecordMap进行随机采样，采样后的统计信息
     */
    static Set<String> sampledUserSet = new HashSet<>();
    static Set<String> sampledShopSet = new HashSet<>();
    static Set<String> sampledDishSet = new HashSet<>();
    static int sampledReviewNum;
    static int sampledTripleNum;


    /**
     * 路径配置
     */
    public static String cityCode = "1";
    public static String rootPath = "D:\\Data\\ProcessedData\\Dianping\\大众点评评论数据集\\点评数据_jyy预处理\\" + cityCode;
    public static String shopInfoPath = rootPath + "\\shopInfo\\shopInfo_deduplicate.json";//一条条的商店信息
    public static String reviewPath = rootPath + "\\review\\先处理水贴，再处理非水贴\\shopReviewExtend.json";//一条条的评论信息
    public static String desPath = rootPath + "\\filteredData\\sample10%\\";
    public static String categoryFilterPath = "./Config/CategoryFilter2";//只取重庆火锅这类菜


    public static void main(String[] args) {
        System.out.println("start " + new Date());

        int userLeastCount = 10; //每个用户去过10个以上的餐厅
        int itemLeastCount = 10; //每家餐厅被10个以上的用户访问过
        int dishLeastCount = 10; // 每家餐厅至少有10道菜
        int dishReviewedLeastCount = 10;//每道菜至少被10个不同的<user-shop>对访问过
        boolean isMapping = true; //保存的签到记录中id是否映射为顺序id, true为映射，false为保留原来的id

        File desFile = new File(desPath);
        if (!desFile.exists()) {
            desFile.mkdir();
        }

        System.out.println("shopInfoPath: " + shopInfoPath);
        System.out.println("reviewPath: " + reviewPath);
        System.out.println("despath: " + desPath);
        System.out.println("filter condition " + userLeastCount + " " + itemLeastCount + " " + dishLeastCount + " " + dishReviewedLeastCount);

        boolean filterByCategory = false;
        shopCategoryMap = new HashMap<>();

        getShopInfo(shopInfoPath, categoryFilterPath, filterByCategory);
        getCheckInRecord(reviewPath);
        /**
         * 先对checkInRecordMap进行random抽样
         */
        double randomRatio = 0.1;
        System.out.println("sample ratio\t" + randomRatio);
        randomSample(randomRatio);

        /**
         * 对抽样后的数据进行过滤
         */
        Map<String, Set<String>> userItemSetMap = getUserItemSetMapofRecord();
        Map<String, Map<String, Integer>> shopDishSetMap = getShopDishSetMapofRecord();
        Map<String, Map<String, Set<String>>> dishPostSetMap = getDishPostSetMap();

        int iteration = -1;//表示会不断迭代直到同时满足least限制条件
        Set<String>[] resultSet = CircularFilter.shopCountFilter(checkInRecordMap,
                userItemSetMap, shopDishSetMap, dishPostSetMap, userLeastCount, itemLeastCount, dishLeastCount, dishReviewedLeastCount, iteration);
        globalshopDishSetMap = CircularFilter.get_shopDishSetMap();
        globalUserItemSetMap = CircularFilter.get_userItemSetMap();
        globalDishPostSetMap = CircularFilter.get_dishPostSetMap();

        outputResultWithMap(globalUserItemSetMap, resultSet, desPath, userLeastCount, itemLeastCount, dishLeastCount, dishReviewedLeastCount, isMapping);
        System.out.println("end " + new Date());
    }

    /**
     * 对与shopInfo取过交集，满足一定条件的评论数据进行采样，得到新的
     * checkInRecord
     *
     * @param randomRatio
     */
    public static void randomSample(double randomRatio) {
        Map<String, Map<String, Map<String, Integer>>> sampledCheckInRecordMap = new HashMap<>();
        sampledUserSet = new HashSet<>();
        sampledShopSet = new HashSet<>();
        sampledDishSet = new HashSet<>();
        Random random = new Random(1);
        for (Map.Entry<String, Map<String, Map<String, Integer>>> userEntry : checkInRecordMap.entrySet()) {
            if (random.nextDouble() < randomRatio) {
                String userid = userEntry.getKey();
                if (!sampledCheckInRecordMap.containsKey(userid)) {
                    sampledCheckInRecordMap.put(userid, new HashMap<>());
                    sampledUserSet.add(userid);
                }
                for (Map.Entry<String, Map<String, Integer>> shopEntry : userEntry.getValue().entrySet()) {
                    String shopid = shopEntry.getKey();
                    if (!sampledCheckInRecordMap.get(userid).containsKey(shopid)) {
                        sampledCheckInRecordMap.get(userid).put(shopid, new HashMap<>());
                        sampledShopSet.add(shopid);
                    }
                    for (Map.Entry<String, Integer> dishEntry : shopEntry.getValue().entrySet()) {
                        String dish = dishEntry.getKey();
                        int fre = dishEntry.getValue();
                        if (!sampledCheckInRecordMap.get(userid).get(shopid).containsKey(dish)) {
                            sampledCheckInRecordMap.get(userid).get(shopid).put(dish, fre);
                            sampledDishSet.add(dish);
                            sampledTripleNum++;
                            sampledReviewNum += fre;
                        }
                    }
                }
            }
        }
        checkInRecordMap = sampledCheckInRecordMap;
        System.out.println("after sample\t" + randomRatio + ", sampled review stat:");
        System.out.println("#user\t" + sampledUserSet.size() + "/" + userSet.size() + "\t" + sampledUserSet.size() * 1.0 / userSet.size());
        System.out.println("#shop\t" + sampledShopSet.size() + "/" + shopSet.size() + "\t" + sampledShopSet.size() * 1.0 / shopSet.size());
        System.out.println("#dish\t" + sampledDishSet.size() + "/" + dishSet.size() + "\t" + sampledDishSet.size() * 1.0 / dishSet.size());
        System.out.println("#sampledTriple\t" + sampledTripleNum + "/" + tripleNum + "\t" + sampledTripleNum * 1.0 / tripleNum);
        System.out.println("#sampledReview\t" + sampledReviewNum + "/" + reviewCount + "\t" + sampledReviewNum * 1.0 / reviewCount);
    }

    public static Map<String, Set<String>> getUserItemSetMapofRecord() {
        int userShopPairNum = 0;
        Map<String, Set<String>> userItemSetMap = new HashMap<>();
        for (Map.Entry<String, Map<String, Map<String, Integer>>> userItemRecEntry : checkInRecordMap.entrySet()) {
            String tempUser = userItemRecEntry.getKey();
            Set<String> tempItemSet = new HashSet<>();
            Map<String, Map<String, Integer>> tempItemRecMap = userItemRecEntry.getValue();
            for (String tempItem : tempItemRecMap.keySet()) {
                userShopPairNum += 1;
                tempItemSet.add(tempItem);
            }
            if (tempItemSet.size() > 0)
                userItemSetMap.put(tempUser, tempItemSet);
        }
        System.out.println("抽样后review中user-shop Pair的数量 " + userShopPairNum);
        return userItemSetMap;
    }

    /**
     * 得到每个餐馆对应的菜品集合及每道菜在训练集中出现的总次数
     *
     * @return
     */
    public static Map<String, Map<String, Integer>> getShopDishSetMapofRecord() {
        int shopDishPairNum = 0;
        Map<String, Map<String, Integer>> shopDishSetMap = new HashMap<>();
        for (Map.Entry<String, Map<String, Map<String, Integer>>> userItemRecEntry : checkInRecordMap.entrySet()) {
            String tempUser = userItemRecEntry.getKey();
            for (Map.Entry<String, Map<String, Integer>> itemDishRecEntry : checkInRecordMap.get(tempUser).entrySet()) {
                String tempItem = itemDishRecEntry.getKey();
                if (shopInfoIdSet.contains(tempItem)) {
                    if (!shopDishSetMap.containsKey(tempItem))
                        shopDishSetMap.put(tempItem, new HashMap<>());
                    for (Map.Entry<String, Integer> DishRecEntry : checkInRecordMap.get(tempUser).get(tempItem).entrySet()) {
                        String dish = DishRecEntry.getKey();
                        if (!shopDishSetMap.get(tempItem).containsKey(dish)) {
                            shopDishPairNum++;
                            shopDishSetMap.get(tempItem).put(dish, DishRecEntry.getValue());
                        } else
                            shopDishSetMap.get(tempItem).put(dish, shopDishSetMap.get(tempItem).get(dish) + DishRecEntry.getValue());
                    }
                }
            }
        }
        System.out.println("抽样后review中shop-dish Pair的数量 " + shopDishPairNum);
        return shopDishSetMap;
    }

    /**
     * 记录每道菜被哪些用户在哪家店访问过
     *
     * @return
     */
    public static Map<String, Map<String, Set<String>>> getDishPostSetMap() {
        int dishUserPairNum =0;
        Map<String, Map<String, Set<String>>> dishPostSetMap = new HashMap<>();
        for (Map.Entry<String, Map<String, Map<String, Integer>>> userItemRecEntry : checkInRecordMap.entrySet()) {
            String tempUser = userItemRecEntry.getKey();
            for (Map.Entry<String, Map<String, Integer>> itemDishRecEntry : checkInRecordMap.get(tempUser).entrySet()) {
                String tempItem = itemDishRecEntry.getKey();
                for (Map.Entry<String, Integer> DishRecEntry : itemDishRecEntry.getValue().entrySet()) {
                    String dish = DishRecEntry.getKey();
                    if (!dishPostSetMap.containsKey(dish))
                        dishPostSetMap.put(dish, new HashMap());
                    if (!dishPostSetMap.get(dish).containsKey(tempUser)) {
                        dishUserPairNum ++;
                        dishPostSetMap.get(dish).put(tempUser, new HashSet<>());
                    }
                    dishPostSetMap.get(dish).get(tempUser).add(tempItem);
                }
            }
        }
        System.out.println("抽样后review中dish-user Pair的数量 " + dishUserPairNum);
        return dishPostSetMap;
    }

    /**
     * 统计原始的shopId个数，与原始shopInfo下的html文件夹进行对照：130705
     * original shop size 130705
     * 过滤后的 shop size 102301
     *
     * @param shopInfoPath
     */
    public static void getShopInfo(String shopInfoPath, String categoryFilterPath, boolean filterByCategory) {
        System.out.println("解析shopInfo.json开始");
        shopInfoIdSet = new HashSet<>();
        shopInfoMap = new HashMap<>();
        String read;
        FileInputStream file;
        BufferedReader bufferedReader;
        ShopInfo shop;

        categoryFilterList = new ArrayList<>();

        //记录原始的shopInfo文件中的shop的个数
        shopInfoIdSet = new HashSet<>();

        try {
            categoryFilterList = FileOperation.readLineArrayList(categoryFilterPath);

            file = new FileInputStream(shopInfoPath);
            bufferedReader = new BufferedReader(new InputStreamReader(file, "UTF-8"));
            while ((read = bufferedReader.readLine()) != null) {
                originShopNum++;
                shop = JSON.parseObject(read, ShopInfo.class);
                String shopId = shop.getShopId();
                String lng = shop.getLng();
                String lat = shop.getLat();
                String region = shop.getRegion();
                String tags = shop.getTags();
                String breadcrumb = shop.getBreadcrumb();

//                if (StringUtils.isNumeric(shopId) && lng != null && !lng.equals("") && lat != null && !lat.equals("") && isDouble(lng) && isDouble(lat)
//                        && region != null && !region.equals("") && tags != null && !tags.trim().equals("") && breadcrumb != null && !breadcrumb.trim().equals("")) {
                //有tag的shop只占了22%，去掉这个限制条件
                if (StringUtils.isNumeric(shopId) && lng != null && !lng.equals("") && lat != null && !lat.equals("") && isDouble(lng) && isDouble(lat)
                        && region != null && !region.equals("") && breadcrumb != null && !breadcrumb.trim().equals("")) {

                    String[] breadCrumb = breadcrumb.substring(2, breadcrumb.length() - 2).split("\",\"");
                    //其实数据中夹杂着一些不是上海餐厅的餐厅
                    if (breadCrumb[0].equals("上海餐厅")) {
                        String category;
                        if (breadCrumb.length >= 4)
                            category = breadCrumb[3];
                        else
                            category = breadCrumb[breadCrumb.length - 1];

                        if (filterByCategory) {
                            if (categoryFilterList.contains(category)) {
                                shopInfoIdSet.add(shopId);
                                shopInfoMap.put(shopId, shop);
                                shopCategoryMap.put(shopId, category);
                            }
                        } else {
                            shopInfoIdSet.add(shopId);
                            shopInfoMap.put(shopId, shop);
                            shopCategoryMap.put(shopId, category);
                        }
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

        System.out.println("解析shopInfo.json结束");
        System.out.println("original shop size " + originShopNum);
        System.out.println("filtered shop size " + shopInfoMap.size());
    }

    /**
     * user-shop-dish-frequency
     * 统计原始文件中的shop的个数，与原始reviews下html文件夹的个数进行对照：94251个shop
     * 统计<userid-shopid-dishid>三元组的个数，与reveiw.json原文件对比</>
     * 输入属性：
     * <reviewid, shopid,userid, taste, condition, service, favDishes, timestamp></>
     * 文件总行数	23015204
     * 与shopinfo取交集,经过一定条件过滤后 user size 1329292  item size 51039
     * dish size 106923 checkin size 6067294 triplenum 14237720 user-item pair num 5917195 user-dish pair num 13152247 shop-dish pair num 892291
     * 重复的三元组的个数	186930  很小一部分有重复现象
     *
     * @param reviewPath
     */
    public static void getCheckInRecord(String reviewPath) {
        System.out.println("checkinrecord解析review.json开始");
        String read;
        FileInputStream file;
        BufferedReader bufferedReader;
        Review review;
        checkInRecordMap = new HashMap<>();
        originUserSet = new HashSet<>();
        originShopSet = new HashSet<>();
        originDishSet = new HashSet<>();
        userSet = new HashSet<>();
        shopSet = new HashSet<>();
        dishSet = new HashSet<>();
        //记录重复的<user,shop,dish>三元组的个数
        int duplicateTriplePairNum = 0;
        Map<String, Set<String>> userDishPair = new HashMap<>();
        Map<String, Set<String>> shopDishPair = new HashMap<>();

        try {
            file = new FileInputStream(reviewPath);
            bufferedReader = new BufferedReader(new InputStreamReader(file, "UTF-8"));
            while ((read = bufferedReader.readLine()) != null) {
                originReviewCount++;
                review = JSON.parseObject(read, Review.class);
                String reviewId = review.getReviewId();
                String userId = review.getUserId();
                String shopId = review.getShopId();
                Set<String> favDishes = review.getFavDishes();
                originUserSet.add(userId);
                originShopSet.add(shopId);
                if (favDishes == null)
                    continue;

                if(favDishes.size() > 0){
                    for (String dish : favDishes) {
                        originDishSet.add(dish);
                    }
                }

                if (shopInfoIdSet.contains(shopId) && StringUtils.isNumeric(userId) && StringUtils.isNumeric(shopId) && favDishes.size() > 0) {
                    reviewCount++;
                    if (!checkInRecordMap.containsKey(userId))
                        checkInRecordMap.put(userId, new HashMap<>());

                    if (!checkInRecordMap.get(userId).containsKey(shopId))
                        checkInRecordMap.get(userId).put(shopId, new HashMap<>());

                    shopSet.add(shopId);
                    for (String dish : favDishes) {
                        dishSet.add(dish);
                        if (!checkInRecordMap.get(userId).get(shopId).containsKey(dish)) {

                            checkInRecordMap.get(userId).get(shopId).put(dish, 1);
                            tripleNum++;

                            if (!userDishPair.containsKey(userId)) {
                                userDishPair.put(userId, new HashSet<>());
                            }
                            userDishPair.get(userId).add(dish);

                            if (!shopDishPair.containsKey(shopId)) {
                                shopDishPair.put(shopId, new HashSet<>());
                            }
                            shopDishPair.get(shopId).add(dish);

                        } else {
                            duplicateTriplePairNum++;
                            checkInRecordMap.get(userId).get(shopId).put(dish, checkInRecordMap.get(userId).get(shopId).get(dish) + 1);
                        }
                    }
                }
            }
            bufferedReader.close();

            int userItemPairNum = 0;
            for (Map.Entry<String, Map<String, Map<String, Integer>>> userItemFreEntry : checkInRecordMap.entrySet()) {
                Map<String, Map<String, Integer>> tempItemFreMap = userItemFreEntry.getValue();
                userItemPairNum += tempItemFreMap.size();
            }
            int userDishPairNum = 0;
            for (Map.Entry<String, Set<String>> entry : userDishPair.entrySet()) {
                userDishPairNum += entry.getValue().size();
            }
            int shopDishPairNum = 0;
            for (Map.Entry<String, Set<String>> entry : shopDishPair.entrySet()) {
                shopDishPairNum += entry.getValue().size();
            }

            userSet = checkInRecordMap.keySet();

            System.out.println("checkinrecord解析review.json结束, statistics: ");
            System.out.println("#review\t" + originReviewCount);
            System.out.println("#user\t" + originUserSet.size());
            System.out.println("#shop\t" + originShopSet.size());
            System.out.println("#dish\t" + originDishSet.size());

            System.out.println("与shopinfo取交集,经过一定条件过滤后: ");
            System.out.println("#review\t" + reviewCount + "/" + originReviewCount);
            System.out.println("#user\t" + userSet.size() + "/" + originUserSet.size());
            System.out.println("#shop\t" + shopSet.size() + "/" + originShopSet.size());
            System.out.println("#dish\t" + dishSet.size() + "/" + originDishSet.size());
            System.out.println("#triple num\t" + tripleNum);
            System.out.println("#user-item pair\t" + userItemPairNum);
            System.out.println("#user-dish pair\t" + userDishPairNum);
            System.out.println("#shop-dish pair\t" + shopDishPairNum);
            System.out.println("重复的三元组的个数\t" + duplicateTriplePairNum);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param resultSet
     * @param desPath
     * @param userLeastCount
     * @param itemLeastCount
     * @param dishLeastCount
     */
    public static void outputResultWithMap(Map<String, Set<String>> userItemSetMap,
                                           Set<String>[] resultSet, String desPath, int userLeastCount, int itemLeastCount, int dishLeastCount, int dishReviewedLeastCount, boolean isMapping) {
        //做映射用的map
        Map<String, Integer> userMap = new ConcurrentHashMap<>();
        Map<String, Integer> itemMap = new ConcurrentHashMap<>();
        Map<String, Integer> dishMap = new ConcurrentHashMap<>();

        Set<String> userSet = resultSet[0];
        Set<String> itemSet = resultSet[1];
        Set<String> dishSet = resultSet[2];
        /**
         * 直接使用userset,itemset和shopdishes来进行映射,映射从0开始
         */
        int id = 0;

        for (String user : userSet) {
            userMap.put(user, id);
            id++;
        }

        id = 0;
        for (String item : itemSet) {
            itemMap.put(item, id);
            id++;
        }

        id = 0;
        for (String dish : dishSet) {
            dishMap.put(dish, id);
            id++;
        }

        StringBuilder shopDishStr = new StringBuilder();
        for (Map.Entry<String, Map<String, Integer>> shopDishEntry : globalshopDishSetMap.entrySet()) {
            String shop = shopDishEntry.getKey();
            if (isMapping) {
                shopDishStr.append(itemMap.get(shop) + " ");
                for (String dish : shopDishEntry.getValue().keySet()) {
                    shopDishStr.append(dishMap.get(dish) + " ");
                }
            } else {
                shopDishStr.append(shop + " ");
                for (String dish : shopDishEntry.getValue().keySet()) {
                    shopDishStr.append(dish + " ");
                }
            }
            shopDishStr.append("\n");
        }
        FileOperation.writeNotAppdend(desPath + "DianpingShopDishes" + isMapping + userLeastCount + itemLeastCount + dishLeastCount + dishReviewedLeastCount + ".txt", shopDishStr.toString());
        /**
         * 输出dishpostset
         */
        StringBuilder dishPostStr = new StringBuilder();
        for (Map.Entry<String, Map<String, Set<String>>> dishPostEntry : globalDishPostSetMap.entrySet()) {
            String dish = dishPostEntry.getKey();
            if (isMapping) {
                dishPostStr.append(dishMap.get(dish) + " ");
            } else {
                dishPostStr.append(dish + " ");
            }
            for (Map.Entry<String, Set<String>> PostEntry : dishPostEntry.getValue().entrySet()) {
                if (isMapping) {
                    dishPostStr.append(userMap.get(PostEntry.getKey()) + "<");

                    for (Iterator it = PostEntry.getValue().iterator(); it.hasNext(); ) {
                        dishPostStr.append(itemMap.get(it.next()) + " ");
                    }
                } else {
                    dishPostStr.append(PostEntry.getKey() + "<");

                    for (Iterator it = PostEntry.getValue().iterator(); it.hasNext(); ) {
                        dishPostStr.append(it.next() + " ");
                    }
                }

                dishPostStr.append(">,");
            }
            dishPostStr.append("\n");
        }
        FileOperation.writeNotAppdend(desPath + "DianpingDishPost" + isMapping + userLeastCount + itemLeastCount + dishLeastCount + dishReviewedLeastCount + ".txt", dishPostStr.toString());

        /**
         * 输出usermap,itemmap和dishmap
         */
        StringBuilder str = new StringBuilder();

        for (Map.Entry<String, Integer> userMapEntry : userMap.entrySet()) {
            str.append(userMapEntry.getKey() + " " + userMapEntry.getValue() + "\n");
        }
        FileOperation.writeNotAppdend(desPath + "userMap" + isMapping + userLeastCount + itemLeastCount + dishLeastCount + ".txt", str.toString());

        str = new StringBuilder();
        for (Map.Entry<String, Integer> itemMapEntry : itemMap.entrySet()) {
            str.append(itemMapEntry.getKey() + " " + itemMapEntry.getValue() + "\n");
        }
        FileOperation.writeNotAppdend(desPath + "itemMap" + isMapping + userLeastCount + itemLeastCount + dishLeastCount + ".txt", str.toString());

        str = new StringBuilder();
        for (Map.Entry<String, Integer> dishMapEntry : dishMap.entrySet()) {
            str.append(dishMapEntry.getKey() + " " + dishMapEntry.getValue() + "\n");
        }
        FileOperation.writeNotAppdend(desPath + "dishMap" + isMapping + userLeastCount + itemLeastCount + dishLeastCount + ".txt", str.toString());

        //用于验证输出数据是否满足四个约定的条件
        Map<String, Set<String>> userItemPair = new HashMap<>();
        Map<String, Set<String>> itemUserPair = new HashMap<>();
        Map<String, Set<String>> shopDishSetMap = new HashMap<>();
        Map<String, Map<String, Set<String>>> dishPostSetMap = new HashMap<>(); //dish-<user,<shop>>
        Map<String, Set<String>> userDishSetMap = new HashMap<>();
        int filteredTripleNum = 0;


        //输出时直接使用最终确定的userset和itemset,shopdishset来过滤checkInRecordMap，需要的话连带category的过滤
        StringBuilder userItemFreStr = new StringBuilder();
        for (Map.Entry<String, Map<String, Map<String, Integer>>> userItemFreEntry : checkInRecordMap.entrySet()) {
            String tempUser = userItemFreEntry.getKey();
            if (userSet.contains(tempUser)) {
                Map<String, Map<String, Integer>> tempItemFreMap = userItemFreEntry.getValue();
                for (String tempItem : tempItemFreMap.keySet()) {
                    if (itemSet.contains(tempItem)) {
                        Map<String, Integer> dishes = tempItemFreMap.get(tempItem);
                        for (Map.Entry<String, Integer> dishesEntry : dishes.entrySet()) {
                            String dish = dishesEntry.getKey();
//                            if(dishSet.contains(dish)){直接用dishSet与globalshopdish比较结果没有差别
                            if (globalshopDishSetMap.get(tempItem).keySet().contains(dish)) {
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

                                if (!userDishSetMap.containsKey(tempUser)) {
                                    userDishSetMap.put(tempUser, new HashSet<>());
                                }
                                userDishSetMap.get(tempUser).add(dish);


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
                                    filteredTripleNum++;
                                } else {
                                    userItemFreStr.append(tempUser);
                                    userItemFreStr.append(" ");
                                    userItemFreStr.append(tempItem);
                                    userItemFreStr.append(" ");
                                    userItemFreStr.append(dish);//key值是dish
                                    userItemFreStr.append(" ");
                                    userItemFreStr.append(frequency);//key值是dish
                                    userItemFreStr.append("\n");
                                    filteredTripleNum++;
                                }
                            }
                        }
                    }
                }
            }
        }
        FileOperation.writeNotAppdend(desPath + "DianpingCheckin" + isMapping + userLeastCount + itemLeastCount + dishLeastCount + dishReviewedLeastCount + ".txt", userItemFreStr.toString());

        //compute user-item pair num in checkInRecordMap after fitering

        int userItemPairNum = 0;
        Map<Integer, Double> visitedShopNumStatistics = new HashMap<>();//统计用户的Post数量及这个数量对应的用户数
        for (Map.Entry<String, Set<String>> userItemFreEntry : userItemPair.entrySet()) {
            int num = userItemFreEntry.getValue().size();
            userItemPairNum += num;
            if (!visitedShopNumStatistics.containsKey(num)) {
                visitedShopNumStatistics.put(num, 0.0d);
            }
            visitedShopNumStatistics.put(num, visitedShopNumStatistics.get(num) + 1.0);
        }
//        System.out.println("# user post statistics");
//        for (Map.Entry<Integer, Double> entry : visitedShopNumStatistics.entrySet()) {
////            visitedShopNumStatistics.put(entry.getKey(), entry.getValue()/userItemPairNum);
//            System.out.println(entry.getKey() + " " + entry.getValue() / userItemPair.size()); //
//        }

        int shopDishPairNum = 0;
        Set<String> outputFinalDishSet = new HashSet<>();
        for (Map.Entry<String, Set<String>> shopDishEntry : shopDishSetMap.entrySet()) {
            shopDishPairNum += shopDishEntry.getValue().size();
            for (String dish : shopDishEntry.getValue())
                outputFinalDishSet.add(dish);
        }

        int userDishPairNum = 0;
        for (Map.Entry<String, Set<String>> userDishEntry : userDishSetMap.entrySet()) {
            userDishPairNum += userDishEntry.getValue().size();
        }

        Iterator<Map.Entry<String, Set<String>>> iterator = shopDishSetMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Set<String>> tempKeyValueSetEntry = iterator.next();
            Set<String> tempValueSet = tempKeyValueSetEntry.getValue();
            if (tempValueSet.size() < dishLeastCount) {
                System.out.println("dishLeastCount不符合");
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
                System.out.println("userLeastCount不符合");
                System.exit(2);
            }
        }
//
        iterator = itemUserPair.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Set<String>> tempKeyValueSetEntry = iterator.next();
            Set<String> tempValueSet = tempKeyValueSetEntry.getValue();
            if (tempValueSet.size() < itemLeastCount) {
                System.out.println("itemLeastCount不符合");
                System.exit(3);
            }
        }

        Iterator<Map.Entry<String, Map<String, Set<String>>>> iterator2 = dishPostSetMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Map<String, Set<String>>> tempKeyValueSetEntry = iterator2.next();
            Map<String, Set<String>> tempValueSet = tempKeyValueSetEntry.getValue();//dish对应的user-shop对
            int size = 0;
            for (Map.Entry<String, Set<String>> userItemEntry : tempValueSet.entrySet()) {
                size += userItemEntry.getValue().size();
            }
            if (size < dishReviewedLeastCount) {
                System.out.println("dishReviewedLeastCount不符合");
                System.exit(4);
            }
        }

        /**
         *TODO 修改输出格式
         */
        System.out.println("过滤后checkInRecordMap features :");
        System.out.println( " usernum" +
                userItemPair.size());
        System.out.println(" shopnum " + shopDishSetMap.size() );
        System.out.println(" dishnum " + outputFinalDishSet.size());
        System.out.println(" userItemPairNum " + userItemPairNum);
        System.out.println( " userDishPairNum " + userDishPairNum);
        System.out.println(" shopDishPairNum " + shopDishPairNum);
        System.out.println(" filteredTipleNum " + filteredTripleNum);
        System.out.println("#average shop/user " + userItemPairNum*1.0/userItemPair.size());
        System.out.println("#average user/shop " + userItemPairNum*1.0/shopDishSetMap.size());
        System.out.println("#average dish/shop " + shopDishPairNum*1.0/shopDishSetMap.size());
        System.out.println("#average <user-shop>/dish " + filteredTripleNum*1.0/outputFinalDishSet.size());
        /**
         * 使用留一法来分训练集和测试集,把每个user对应的第一个shop分配给了测试集，
         * 当用户对应的只有一个shop时(不会有这种情况，已经保证每个用户去过的店大于10家了)
         * 将这个shop分给训练集,当有多个时，就把第一个分到测试集中
         */
        Set<String> trainUserSet = new HashSet<>();
        Set<String> testUserSet = new HashSet<>();

        Set<String> trainShopSet = new HashSet<>();
        Set<String> testShopSet = new HashSet<>();

        Set<String> trainDishSet = new HashSet<>();
        Set<String> testDishSet = new HashSet<>();

        boolean flag;

        int trainTripleNum = 0;
        int testTripleNum = 0;

        StringBuilder userItemDishTrainStr = new StringBuilder();
        StringBuilder userItemDishTestStr = new StringBuilder();
        /**
         * TODO 把这个训练集测试集的分割合并到上面的统计中
         */
        for (Map.Entry<String, Map<String, Map<String, Integer>>> userItemFreEntry : checkInRecordMap.entrySet()) {
            String tempUser = userItemFreEntry.getKey();
            if (userSet.contains(tempUser)) {
                Map<String, Map<String, Integer>> tempItemFreMap = userItemFreEntry.getValue();
//                if (userItemSetMap.get(tempUser).size() > 1) {
                flag = true; //对每个用户重新赋值
                for (String tempItem : tempItemFreMap.keySet()) {
                    if (itemSet.contains(tempItem)) {
                        if (flag) {
//                                testflag++;
                            Map<String, Integer> dishes = tempItemFreMap.get(tempItem);
                            for (Map.Entry<String, Integer> dishesEntry : dishes.entrySet()) {
                                String dish = dishesEntry.getKey();
                                if (globalshopDishSetMap.get(tempItem).keySet().contains(dish)) {
                                    testUserSet.add(tempUser);
                                    testShopSet.add(tempItem);
                                    testDishSet.add(dish);

                                    int frequency = dishesEntry.getValue();
                                    if (isMapping) {
                                        userItemDishTestStr.append(userMap.get(tempUser));
                                        userItemDishTestStr.append(" ");
                                        userItemDishTestStr.append(itemMap.get(tempItem));
                                        userItemDishTestStr.append(" ");
                                        userItemDishTestStr.append(dishMap.get(dish));
                                        userItemDishTestStr.append(" ");
                                        userItemDishTestStr.append(frequency);
                                    } else {
                                        userItemDishTestStr.append(tempUser);
                                        userItemDishTestStr.append(" ");
                                        userItemDishTestStr.append(tempItem);
                                        userItemDishTestStr.append(" ");
                                        userItemDishTestStr.append(dish);
                                        userItemDishTestStr.append(" ");
                                        userItemDishTestStr.append(frequency);
                                    }
                                    testTripleNum++;
                                    userItemDishTestStr.append("\n");
                                    flag = false; //有可能在原始数据集中第一个shop中的dish是被过滤掉的，所以以第一个被加入的dish的餐厅为准
                                }
                            }
                        } else {
                            Map<String, Integer> dishes = tempItemFreMap.get(tempItem);
                            for (Map.Entry<String, Integer> dishesEntry : dishes.entrySet()) {
                                String dish = dishesEntry.getKey();
//                                    if(dishSet.contains(dish)) {
                                if (globalshopDishSetMap.get(tempItem).keySet().contains(dish)) {
                                    trainUserSet.add(tempUser);
                                    trainShopSet.add(tempItem);
                                    trainDishSet.add(dish);
                                    int frequency = dishesEntry.getValue();
                                    if (isMapping) {
                                        userItemDishTrainStr.append(userMap.get(tempUser));
                                        userItemDishTrainStr.append(" ");
                                        userItemDishTrainStr.append(itemMap.get(tempItem));
                                        userItemDishTrainStr.append(" ");
                                        userItemDishTrainStr.append(dishMap.get(dish));
                                        userItemDishTrainStr.append(" ");
                                        userItemDishTrainStr.append(frequency);
                                    } else {
                                        userItemDishTrainStr.append(tempUser);
                                        userItemDishTrainStr.append(" ");
                                        userItemDishTrainStr.append(tempItem);
                                        userItemDishTrainStr.append(" ");
                                        userItemDishTrainStr.append(dish);
                                        userItemDishTrainStr.append(" ");
                                        userItemDishTrainStr.append(frequency);
                                    }
                                    trainTripleNum++;
                                    userItemDishTrainStr.append("\n");
                                }
                            }
                        }
                    }
                }
            }
        }

        System.out.println("Train tripleNum " + trainTripleNum + " #user " + trainUserSet.size() + " #shop " + trainShopSet.size() + " #dish " + trainDishSet.size());
        System.out.println("Test tripleNum " + testTripleNum + " #user " + testUserSet.size() + " #shop " + testShopSet.size() + " #dish " + testDishSet.size());

        //TODO 输出train集合和test集合中user,shop,dish,user-item对的统计信息，并给出train集合中shop对应的dish的最大值和最小值，以及dish对应的pair个数的最大值和最小值
        FileOperation.writeNotAppdend
                (desPath + "DianpingCheckin" + isMapping +
                        userLeastCount + itemLeastCount + dishLeastCount + dishReviewedLeastCount + "train.txt", userItemDishTrainStr.toString());
        FileOperation.writeNotAppdend
                (desPath + "DianpingCheckin" + isMapping +
                        userLeastCount + itemLeastCount + dishLeastCount + dishReviewedLeastCount + "test.txt", userItemDishTestStr.toString());


        final int[] filteredShopNum = {0};
        StringBuilder locationStr = new StringBuilder();
        StringBuilder tagStr = new StringBuilder();
        StringBuilder descStr = new StringBuilder();
        StringBuilder regionStr = new StringBuilder();
        shopInfoMap.entrySet().stream().forEach(tempEntry -> {
            ShopInfo shopInfo = tempEntry.getValue();
            if (itemSet.contains(shopInfo.getShopId())) {
                filteredShopNum[0]++;
                if (isMapping) {
                    locationStr.append(itemMap.get(shopInfo.getShopId()));
                } else {
                    locationStr.append(shopInfo.getShopId());
                }
                locationStr.append(" ");
                locationStr.append(shopInfo.getLat());
                locationStr.append(" ");
                locationStr.append(shopInfo.getLng());
                locationStr.append("\n");


                if (shopInfo.getTags() != null && !shopInfo.getTags().equals("")) {
                    String[] tags = shopInfo.getTags().substring(2, shopInfo.getTags().length() - 2).split("\",\"");
                    if (isMapping) {
                        tagStr.append(itemMap.get(shopInfo.getShopId()));
                    } else {
                        tagStr.append(shopInfo.getShopId());
                    }
                    for (String tag : tags) {
                        tagStr.append(" ## ");
                        tagStr.append(tag);
                    }
                    tagStr.append("\n");
                }
                String[] descs = shopInfo.getBreadcrumb().substring(2, shopInfo.getBreadcrumb().length() - 2).split("\",\"");
                if (isMapping) {
                    descStr.append(itemMap.get(shopInfo.getShopId())); //descStr是餐厅的类别
                } else {
                    descStr.append(shopInfo.getShopId());
                }
                for (String desc : descs) {
                    descStr.append(" ## ");
                    descStr.append(desc);
                }
                descStr.append("\n");

                if (isMapping) {
                    regionStr.append(itemMap.get(shopInfo.getShopId()));
                } else {
                    regionStr.append(shopInfo.getShopId());
                }
                regionStr.append(" ");
                regionStr.append(shopInfo.getRegion());
                regionStr.append("\n");

            }
        });

        System.out.println("location num after filtering: " + filteredShopNum[0]);

        FileOperation.writeNotAppdend(desPath + "DianpingLocation" + isMapping + userLeastCount + itemLeastCount + dishLeastCount + dishReviewedLeastCount + ".txt", locationStr.toString());
        FileOperation.writeNotAppdend(desPath + "DianpingTag" + isMapping + userLeastCount + itemLeastCount + dishLeastCount + dishReviewedLeastCount + ".txt", tagStr.toString());
        FileOperation.writeNotAppdend(desPath + "DianpingDescription" + isMapping + userLeastCount + itemLeastCount + dishLeastCount + dishReviewedLeastCount + ".txt", descStr.toString());
        FileOperation.writeNotAppdend(desPath + "DianpingRegion" + isMapping + userLeastCount + itemLeastCount + dishLeastCount + dishReviewedLeastCount + ".txt", regionStr.toString());

    }

    public static boolean isDouble(String str) {
        Pattern pattern = Pattern.compile("^[-\\+]?[.\\d]*$");
        return pattern.matcher(str).matches();
    }

}


