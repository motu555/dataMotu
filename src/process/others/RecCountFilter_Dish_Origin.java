package process.others;

import java.util.*;

/**
 * 该类用来过滤推荐系统数据，主要是两个过滤功能
 * (1) 根据 user-itemSet map 过滤user访问item数量少于userLeastCount的user， 和item被user访问次数少于temLeastCount的item；
 * (2) 根据 user-itemSet map和user-socialSet map过滤ser访问item数量少于userLeastCount的user，
 * item被user访问次数少于itemLeastCount的item，以及user的social关系少于socialLeastCount的user
 * （3）根据有四个过滤条件：1.用户访问的餐厅数超过userleastcount,2.餐厅被超过itemleastcount个用户访问过，
 * 3.每家餐厅有超过 dishLeastCount 道菜, 4.每道菜被超过dishReviewedLeastCount个<用户，餐厅>对访问过</>
 *
 * 处理菜品数据时调用shopCountFilter方法，实现4个过滤条件
 * TODO 原来代码的备份
 * @author Keqiang Wang 、Yuanyuan Jin
 */
public class  RecCountFilter_Dish_Origin{
    static Map<String, Map<String,Integer>> globalshopDishSetMap;
    static Map<String, Set<String>> globalUserItemSetMap;
    static Map<String, Map<String,Set<String>>> globalDishPostSetMap;

    /**
     * (1) 根据 user-itemSet map 过滤user访问item数量少于userLeastCount的user， 和item被user访问次数少于temLeastCount的item；
     *
     * @param userItemSetMap user-itemSet map (Map<String, Set<String>>),是map数据结果，每个user访问的item集合
     * @param userLeastCount 利用访问item个数过滤user，当访问item数量少于userLeastCount时过滤掉该用户
     * @param itemLeastCount 利用item被user访问次数过滤item， 当item被访问的user数量少于的itemLeastCount时过滤掉该item
     * @param iteration      迭代过滤循环次数，特别地当iteration=-1时，一直迭代过滤直至符合上述两个条件；
     * @return 过滤后的user set and item set
     */
    public static Set[] pairCountFilter(Map<String, Set<String>> userItemSetMap, int userLeastCount,
                                        int itemLeastCount, int iteration) {
        Map<String, Set<String>> itemUserSetMap = getItemUserSetMap(userItemSetMap);
        Set<String> filterUserSet;
        Set<String> filterItemSet;
        System.out.println("Original User Number: " + userItemSetMap.size() + ". Original Item Number: " +
                itemUserSetMap.size() + ". " + new Date());

        int iter = 1;
        do {

            // 过滤key
            filterUserSet = getFilterSet(userItemSetMap, userLeastCount);
            filterItemSet = getFilterSet(itemUserSetMap, itemLeastCount);

            //根据userSet和itemSet过滤ValueSet
            filterValue(userItemSetMap, filterItemSet);
            filterValue(itemUserSetMap, filterUserSet);
            ++iter;
            System.out.println("Iteration: " + iter + ". User Number: " + userItemSetMap.size() + ". Item Number: "
                    + itemUserSetMap.size() + ". " + new Date());

        } while ((filterUserSet.size() > 0 || filterItemSet.size() > 0) && (iteration == -1 || iter <= iteration));

        Set[] resultSet = new Set[2];
        resultSet[0] = userItemSetMap.keySet();
        resultSet[1] = itemUserSetMap.keySet();
        return resultSet;
    }


    /**
     * (2) 根据 user-itemSet map和user-socialSet map过滤ser访问item数量少于userLeastCount的user，
     * item被user访问次数少于itemLeastCount的item，以及user的social关系少于socialLeastCount的user
     *
     * @param userItemSetMap   user-itemSet map (Map<String, Set<String>>),是map数据结果，每个user访问的item集合
     * @param userSocialSetMap user-socialSet map (Map<String, Set<String>>),是map数据结果，每个user拥有的好友（或者粉丝）集合
     * @param userLeastCount   利用访问item个数过滤user，当访问item数量少于userLeastCount时过滤掉该用户
     * @param itemLeastCount   利用item被user访问次数过滤item， 当item被访问的user数量少于的itemLeastCount时过滤掉该item
     * @param socialLeastCount 利用好友数量过滤user，当好友数量少于socialLeastCount时过滤掉该用户
     * @param iteration        迭代过滤循环次数，特别地当iteration=-1时，一直迭代过滤直至符合上述三个条件；
     * @return user set and item set
     */
    public static Set[] socialCountFilter(Map<String, Set<String>> userItemSetMap, Map<String,
            Set<String>> userSocialSetMap, int userLeastCount, int itemLeastCount, int socialLeastCount, int iteration) {

        Map<String, Set<String>> itemUserSetMap = getItemUserSetMap(userItemSetMap);
        Set<String> filterUserSet;
        Set<String> filterItemSet;
        System.out.println("Original User Number: " + userItemSetMap.size() + ". Original Item Number: " +
                itemUserSetMap.size() + ". " + new Date());
        int iter = 0;
        do {
            // 过滤key 1.review本身需要满足两个条件，social本身需要满足一个条件，review和social还需要求交集
            filterUserSet = getFilterSet(userItemSetMap, userLeastCount);
            filterItemSet = getFilterSet(itemUserSetMap, itemLeastCount);

            filterUserSet.addAll(getFilterSet(userSocialSetMap, socialLeastCount));

            filterUserSet.addAll(filterUserSocial(userItemSetMap, userSocialSetMap));

            //根据userSet和itemSet过滤ValueSet
            filterValue(userItemSetMap, filterItemSet);
            filterValue(itemUserSetMap, filterUserSet);
            /*
            这步操作有必要么？
             */
            filterValue(userSocialSetMap, filterUserSet);

            ++iter;
            System.out.println("Iteration: " + iter + ". User Number: " + userItemSetMap.size() + ". Item Number: "
                    + itemUserSetMap.size() + ". " + new Date());

        } while ((filterUserSet.size() > 0 || filterItemSet.size() > 0) && (iteration == -1 || iter <= iteration));

        Set[] resultSet = new Set[2];
        resultSet[0] = userItemSetMap.keySet();
        resultSet[1] = itemUserSetMap.keySet();

        return resultSet;
    }


    /**
     * 有四个过滤条件：1.用户访问的餐厅数超过userleastcount,餐厅被超过itemleastcount个用户访问过，
     * 每家餐厅有超过 dishLeastCount 道菜, 每道菜被超过dishReviewedLeastCount个<用户，餐厅>对访问过</>
     * @param checkInRecordMap
     * @param userItemSetMap
     * @param shopDishSetMap
     * @param dishPostSetMap
     * @param userLeastCount
     * @param itemLeastCount
     * @param dishLeastCount
     * @param dishReviewedLeastCount
     * @param iteration
     * @return
     */
    public static Set[] shopCountFilter( Map<String, Map<String, Map<String, Integer>>> checkInRecordMap, Map<String, Set<String>> userItemSetMap,
                                         Map<String, Map<String,Integer >> shopDishSetMap,Map<String,Map<String,Set<String>>>dishPostSetMap, int userLeastCount, int itemLeastCount, int dishLeastCount, int dishReviewedLeastCount ,int iteration) {

        Map<String, Set<String>> itemUserSetMap = getItemUserSetMap(userItemSetMap);
        Set<String> filterUserSet;
        Set<String> filterItemSet;
        Set<String> filterDishSet;
        System.out.println("与shopinfo取shop交集后reveiw中  User Number: " + userItemSetMap.size() + ".   Item Number: " +
                itemUserSetMap.size() + ".   Dish Number: " + dishPostSetMap.size()+ new Date());
        int iter = 0;

        //计算user-item对的数量
        int userItemPairNum = 0;
        for (Map.Entry<String, Set<String>> userItemSetEntry : userItemSetMap.entrySet()) {
            userItemPairNum += userItemSetEntry.getValue().size();
        }

        Set<String> finalDishSet = new HashSet<>();
        for(Map.Entry<String, Map<String,Integer>> shopDishSetEntry : shopDishSetMap.entrySet()){
            Set<String> dishSet = shopDishSetEntry.getValue().keySet();
            for(String dish : dishSet){
                if(!finalDishSet.contains(dish))
                    finalDishSet.add(dish);
            }
        }

        Set<String> finalUserSet = new HashSet<>();
        Set<String> finalShopSet = new HashSet<>();
        for(Map.Entry<String,Map<String,Set<String>>>dishPostEntry : dishPostSetMap.entrySet()){
            Map<String,Set<String>> userShopMap = dishPostEntry.getValue();
            for(Map.Entry<String, Set<String>> userShopEntry : userShopMap.entrySet()){
                if(!finalUserSet.contains(userShopEntry.getKey())) {
                    finalUserSet.add(userShopEntry.getKey());
                }
                for (String shop : userShopEntry.getValue()) {
                    if(!finalShopSet.contains(shop)){
                        finalShopSet.add(shop);
                    }
                }
            }
        }

        System.out.println("Iteration: " + iter + " User Number: " + userItemSetMap.size() + " Item Number: "
                + itemUserSetMap.size() + " shopdish中的itemnumber " +
                shopDishSetMap.size() +" shopdish中的dishnumber " +
                finalDishSet.size()+" dishPost中的dishnumber "+dishPostSetMap.size() +" dishPost中的usernumber "+finalUserSet.size()+
                " dishPost中的shopnumber "+finalShopSet.size() +" user-item pair num "+ userItemPairNum + new Date());
        Map<String, Set<String>>[] filteruserItemPair = new HashMap [2];

        do {

            // 过滤key 1.review本身需要满足三个条件
            filterUserSet = getFilterSet(userItemSetMap, userLeastCount);
            filterItemSet = getFilterSet(itemUserSetMap, itemLeastCount);

            filterItemSet.addAll(getFilterShopSet(shopDishSetMap, dishLeastCount));


            filterDishSet = getFilterDishSet(dishPostSetMap, dishReviewedLeastCount);


            //根据userSet和itemSet过滤ValueSet
            filterValue(userItemSetMap, filterItemSet);

            filterKey(itemUserSetMap, filterItemSet);
            filterValue(itemUserSetMap, filterUserSet);

            System.out.println("本轮中的过滤集合 " + filterUserSet.size() + " " + filterItemSet.size() + " " + filterDishSet.size());


            /**
             *
             */
            filterShopDishValue(checkInRecordMap,shopDishSetMap, filterUserSet, filterItemSet,  filterDishSet);
            filteruserItemPair =  filterDishSet(dishPostSetMap,filterUserSet,filterItemSet);

            filteruserItemPair(userItemSetMap, filteruserItemPair[0]);
            filteruserItemPair(itemUserSetMap, filteruserItemPair[1]);


            //计算user-item对的数量
            userItemPairNum = 0;
            for (Map.Entry<String, Set<String>> userItemSetEntry : userItemSetMap.entrySet()) {
                userItemPairNum += userItemSetEntry.getValue().size();
            }

            finalDishSet = new HashSet<>();
            for(Map.Entry<String, Map<String,Integer>> shopDishSetEntry : shopDishSetMap.entrySet()){
                Set<String> dishSet = shopDishSetEntry.getValue().keySet() ;
                for(String dish : dishSet){
                    if(!finalDishSet.contains(dish))
                        finalDishSet.add(dish);
                }
            }


            finalUserSet = new HashSet<>();
            finalShopSet = new HashSet<>();

            Map<String, Set<String>> userItemPairInDishPost = new HashMap();
            for(Map.Entry<String,Map<String,Set<String>>>dishPostEntry : dishPostSetMap.entrySet()){
                Map<String,Set<String>> userShopMap = dishPostEntry.getValue();
                for(Map.Entry<String, Set<String>>userShopEntry : userShopMap.entrySet()){
                    if(!finalUserSet.contains(userShopEntry.getKey())) {
                        finalUserSet.add(userShopEntry.getKey());
                    }
                    if(!userItemPairInDishPost.keySet().contains(userShopEntry.getKey())) {
                        userItemPairInDishPost.put(userShopEntry.getKey(), new HashSet<>());
                    }
                    for (String shop : userShopEntry.getValue()) {
                        if(!finalShopSet.contains(shop)){
                            finalShopSet.add(shop);
                        }
                        userItemPairInDishPost.get(userShopEntry.getKey()).add(shop);
                    }
                }
            }

            int userItemPairNumInDishPost = 0;
            for(Map.Entry<String,Set<String>>userItemPairEntry : userItemPairInDishPost.entrySet()){
                userItemPairNumInDishPost += userItemPairEntry.getValue().size();
            }


            System.out.println("Iteration: " + iter + " User Number: " + userItemSetMap.size() + " Item Number: "
                    + itemUserSetMap.size() + " shopdish中的itemnumber " +
                    shopDishSetMap.size() +" shopdish中的dishnumber " +
                    finalDishSet.size()+" dishPost中的dishnumber "+dishPostSetMap.size() +" dishPost中的usernumber "+finalUserSet.size()+
                    " dishPost中的shopnumber "+finalShopSet.size()
                    + " user-item pair num "+ userItemPairNum + "userItemPairNumInDishPost " + userItemPairNumInDishPost + new Date());

            ++iter;

        } while ((filterUserSet.size() > 0 || filterItemSet.size() > 0|| filterDishSet.size()>0) && (iteration == -1 || iter <= iteration));
//         } while ((filterUserSet.size() > 0 || filterItemSet.size() > 0) && (iteration == -1 || iter <= iteration));

        Set[] resultSet = new Set[3];
        resultSet[0] = userItemSetMap.keySet();
        resultSet[1] = itemUserSetMap.keySet();
        resultSet[2] = dishPostSetMap.keySet();
        globalshopDishSetMap = shopDishSetMap;
        globalUserItemSetMap = userItemSetMap;
        globalDishPostSetMap = dishPostSetMap;
        return resultSet;
    }
    public static Map<String, Map<String,Integer>> get_shopDishSetMap(){
        return  globalshopDishSetMap;
    }

    public static Map<String, Set<String>> get_userItemSetMap(){
        return  globalUserItemSetMap;
    }

    public static Map<String, Map<String,Set<String>>> get_dishPostSetMap(){
        return  globalDishPostSetMap;
    }

    /**
     * 将user-itemSet map 形式转化成 user-itemSet map的形式
     *
     * @param userItemSetMap user-itemSet map 每个user访问的item集合
     * @return itemUserSetMap item-userSet map 每个item被访问的user集合
     */
    private static Map<String, Set<String>> getItemUserSetMap(Map<String, Set<String>> userItemSetMap) {
        Map<String, Set<String>> itemUserSetMap = new HashMap<>();
        for (Map.Entry<String, Set<String>> userItemSetEntry : userItemSetMap.entrySet()) {
            String tempUser = userItemSetEntry.getKey();
            Set<String> tempItemSet = userItemSetEntry.getValue();
            for (String tempItem : tempItemSet) {
                if (!itemUserSetMap.containsKey(tempItem)) {
                    itemUserSetMap.put(tempItem, new HashSet<>());
                }
                itemUserSetMap.get(tempItem).add(tempUser);
            }
        }
        return itemUserSetMap;
    }


    /**
     * filter the keyValueSetMap with the size of each value set is at least larger than leastCount
     *
     * @param keyValueSetMap key-ValueSet map Map<String, Set<String>> 数据结果，可以存储user-itemSet map 和 user-itemSet map
     * @param leastCount     每个key对应的ValueSet数量少于leastCount，则过滤该条Entry
     * @return 被过滤的keySet
     */
    private static Set<String> getFilterSet(Map<String, Set<String>> keyValueSetMap, int leastCount) {
        Set<String> filterSet = new HashSet<>();
        //使用迭代器可以在遍历时删除
        Iterator<Map.Entry<String, Set<String>>> iterator = keyValueSetMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Set<String>> tempKeyValueSetEntry = iterator.next();
            String tempKey = tempKeyValueSetEntry.getKey();
            Set<String> tempValueSet = tempKeyValueSetEntry.getValue();
            if (tempValueSet.size() < leastCount) {
                filterSet.add(tempKey);
                iterator.remove();
            }
        }
        return filterSet;
    }

    /**
     * 得到不满足过滤条件的dish集合
     * @param keyValueSetMap
     * @param leastCount
     * @return
     */
    private static Set<String> getFilterDishSet(Map<String, Map<String,Set<String>>> keyValueSetMap, int leastCount) {
        Set<String> filterSet = new HashSet<>();
        //使用迭代器可以在遍历时删除
        Iterator<Map.Entry<String, Map<String, Set<String>>>> iterator = keyValueSetMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Map<String, Set<String>>> tempKeyValueSetEntry = iterator.next();
            String tempKey = tempKeyValueSetEntry.getKey();//dish名称
            Map<String, Set<String>> tempValueSet = tempKeyValueSetEntry.getValue();//dish对应的user-shop对
            int size=0;
            for (Map.Entry<String, Set<String>>userItemEntry : tempValueSet .entrySet()) {
                size+=userItemEntry.getValue().size();
            }
            if(size<leastCount) {
                filterSet.add(tempKey);
                iterator.remove();
            }
        }
        return filterSet;
    }


    private static Set<String> getFilterShopSet(Map<String, Map<String,Integer>> keyValueSetMap, int leastCount) {
        Set<String> filterSet = new HashSet<>();
        //使用迭代器可以在遍历时删除
        Iterator<Map.Entry<String, Map<String,Integer>>> iterator = keyValueSetMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Map<String,Integer>> tempKeyValueSetEntry = iterator.next();
            String tempKey = tempKeyValueSetEntry.getKey();
            Map<String,Integer> tempValueSet = tempKeyValueSetEntry.getValue();
            if (tempValueSet.size() < leastCount) {
                filterSet.add(tempKey);
                iterator.remove();
            }
        }
        return filterSet;
    }


    /**
     * 利用集合filterValueSet过滤 key-ValueSet map中valueSet中存在的value
     *
     * @param keyValueSetMap key-ValueSet map Map<String, Set<String>> 数据结果，可以存储user-itemSet map 和 user-itemSet map
     * @param filterValueSet filterValueSet中的value在keyValueSetMap的valueSet中不应该出现
     * @return 过滤后的key-ValueSet map
     */
    private static Map<String, Set<String>> filterValue(Map<String, Set<String>> keyValueSetMap, Set<String> filterValueSet) {
        for (Map.Entry<String, Set<String>> tempKeyValueSetEntry : keyValueSetMap.entrySet()) {
            Set<String> tempValueSet = tempKeyValueSetEntry.getValue();
            tempValueSet.removeIf(filterValueSet::contains);
        }
        return keyValueSetMap;
    }


    private static Map<String, Map<String,Integer>> filterShopDishValue(Map<String, Map<String, Map<String, Integer>>> checkInRecordMap,
                                                                        Map<String,Map<String,Integer >> shopDishMap,  Set<String> filterUserSet, Set<String> filterItemSet ,Set<String> filterDishSet) {

        //从shopdishmap中直接把不符合条件的shop条目去除，并把shop对应的dish列表中直接清除不符合条件的dish
        Iterator<Map.Entry<String, Map<String,Integer >>> iterator = shopDishMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Map<String,Integer >> tempKeyValueSetEntry = iterator.next();
            if (filterItemSet.contains(tempKeyValueSetEntry.getKey())) {
                iterator.remove();
            }

            else {
                //这里需要用到新的遍历器，以便在遍历shopdishset内部的<dish,frequency>map时能实现遍历时的删除
                Iterator<Map.Entry< String,Integer >> dishFrequencyiterator = tempKeyValueSetEntry.getValue().entrySet().iterator();
                while (dishFrequencyiterator.hasNext()) {
                    Map.Entry<String,Integer>dishFrequencyEntry = dishFrequencyiterator.next();
                    if(filterDishSet.contains(dishFrequencyEntry.getKey())){
                        dishFrequencyiterator.remove();
                    }
                }
            }
        }
        //去除因为去除user造成的shop对应的dishes的减少，但不会因为某位用户的记录中有某道菜就会从shopdish中去除这道菜，只有计数减为0时才去除
        for (String user : filterUserSet) {
            for (Map.Entry<String, Map<String, Integer>> tempKeyValueSetEntry : checkInRecordMap.get(user).entrySet()) {
                String shopid = tempKeyValueSetEntry.getKey();
                if (shopDishMap.containsKey(shopid)) {
                    Set<String> dishes = tempKeyValueSetEntry.getValue().keySet();
                    for (String dish : dishes) {
                        if(shopDishMap.get(shopid).keySet().contains(dish)) {
                            if(shopDishMap.get(shopid).get(dish) - checkInRecordMap.get(user).get(shopid).get(dish) == 0) {
                                shopDishMap.get(shopid).remove(dish);
//                                System.out.println("remove");
                            }
                            else
                                shopDishMap.get(shopid).put(dish, shopDishMap.get(shopid).get(dish) - checkInRecordMap.get(user).get(shopid).get(dish));
//                            shopDishMap.get(shopid).put(dish, shopDishMap.get(shopid).get(dish) - checkInRecordMap.get(user).get(shopid).get(dish));
                        }
                    }
                }
            }
        }
        return shopDishMap;
    }

    /**
     * 过滤掉dish中对应的user-item对
     * @param dishPostSetMap
     * @param filterUserSet
     * @param filterItemSet
     * @return
     */
    private static Map<String, Set<String>>[] filterDishSet(Map<String,Map<String,Set<String>>>dishPostSetMap,Set<String>filterUserSet,Set<String>filterItemSet){
        Map<String, Set<String>>[] result = new HashMap[2];
        Iterator<Map.Entry<String,Map<String,Set<String>>>> iterator = dishPostSetMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String,Map<String,Set<String>>> tempKeyValueSetEntry = iterator.next();
            //这里需要用到新的遍历器，以便在遍历shopdishset内部的<dish,frequency>map时能实现遍历时的删除
            Iterator<Map.Entry< String,Set<String>>> userItemPairIterator = tempKeyValueSetEntry.getValue().entrySet().iterator();
            while (userItemPairIterator.hasNext()) {
                Map.Entry<String,Set<String>>userItemPairEntry=userItemPairIterator.next();
                Set<String>itemSet = new HashSet<>();
                if(filterUserSet.contains(userItemPairEntry.getKey())){
                    userItemPairIterator.remove();
                } else {
                    itemSet=userItemPairEntry.getValue();
                    itemSet.removeIf(filterItemSet::contains);
                    //删除键值itemset为空的整条记录
                    if(itemSet.isEmpty()){
                        userItemPairIterator.remove();
                    }
                }
            }
        }

        Map<String, Set<String>> userItemPairInDishPost = new HashMap();
        Map<String, Set<String>> itemUserPairInDishPost = new HashMap();
        for(Map.Entry<String,Map<String,Set<String>>>dishPostEntry : dishPostSetMap.entrySet()){
            Map<String,Set<String>> userShopMap = dishPostEntry.getValue();
            for(Map.Entry<String, Set<String>>userShopEntry : userShopMap.entrySet()){

                if(!userItemPairInDishPost.keySet().contains(userShopEntry.getKey())) {
                    userItemPairInDishPost.put(userShopEntry.getKey(), new HashSet<>());
                }
                for (String shop : userShopEntry.getValue()) {
                    userItemPairInDishPost.get(userShopEntry.getKey()).add(shop);
                }
            }
        }

        itemUserPairInDishPost = getItemUserSetMap(userItemPairInDishPost);
        result[0] = userItemPairInDishPost;
        result[1] = itemUserPairInDishPost;


        return result;
    }
    /**
     * 取得userItemSetMap和userSocialSetMap中key Set中有一方不存在的user Set
     *
     * @param userItemSetMap   user-itemSet map  每个user访问的item集合
     * @param userSocialSetMap user-socialSet Map 每个user拥有的好友（或者粉丝）集合
     * @return unContainKeySet(userItemSetMap和userSocialSetMap中key Set中有一方不存在的user Set)
     */
    private static Set<String> filterUserSocial(Map<String, Set<String>> userItemSetMap, Map<String, Set<String>> userSocialSetMap) {
        Set<String> userFilterSet = new HashSet<>();
        userFilterSet.addAll(filterUnContainKey(userItemSetMap, userSocialSetMap.keySet()));
        userFilterSet.addAll(filterUnContainKey(userSocialSetMap, userItemSetMap.keySet()));

        return userFilterSet;
    }

//    private static Set<String> filterShopDish(Map<String, Set<String>> itemUserSetMap, Map<String, Set<String>> shopDishSetMap) {
//        Set<String> itemFilterSet = new HashSet<>();
//        itemFilterSet.addAll(filterUnContainKey(itemUserSetMap, shopDishSetMap.keySet()));
//        itemFilterSet.addAll(filterUnContainKey(shopDishSetMap, itemUserSetMap.keySet()));
//
//        return itemFilterSet;
//    }

    /**
     * 得到keyValueSetMap中在keySet没出现的key值集合
     *
     * @param keyValueSetMap key-ValueSet map Map<String, Set<String>> 数据结果
     * @param keySet         keySet包含需要出现的key值
     * @return unContainKeySet
     */
    private static Set<String> filterUnContainKey(Map<String, Set<String>> keyValueSetMap, Set<String> keySet) {
        Set<String> unContainKeySet = new HashSet<>();
        Iterator<Map.Entry<String, Set<String>>> iterator = keyValueSetMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Set<String>> tempKeyValueSetEntry = iterator.next();
            if (!keySet.contains(tempKeyValueSetEntry.getKey())) {
                unContainKeySet.add(tempKeyValueSetEntry.getKey());
                iterator.remove();
            }
        }
        return unContainKeySet;
    }

    /**
     * 把keyvaluesetmap中出现在keyset中的key-value记录删除
     * @param keyValueSetMap
     * @param keySet
     * @return
     */
    private static Set<String> filterKey(Map<String, Set<String>> keyValueSetMap, Set<String> keySet) {
        Set<String> unContainKeySet = new HashSet<>();
        Iterator<Map.Entry<String, Set<String>>> iterator = keyValueSetMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Set<String>> tempKeyValueSetEntry = iterator.next();
            if (keySet.contains(tempKeyValueSetEntry.getKey())) {
                unContainKeySet.add(tempKeyValueSetEntry.getKey());
                iterator.remove();
            }
        }
        return unContainKeySet;
    }

    /**
     *
     * @param pairMap
     * @param filterPairMap  从dishpost中得到的<user,item>或<item,user>对,是要保留下来的</></>
     */
    private static void filteruserItemPair(Map<String, Set<String>>pairMap, Map<String, Set<String>>filterPairMap){
        Iterator<Map.Entry<String, Set<String>>> iterator = pairMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Set<String>> tempKeyValueSetEntry = iterator.next();
            if (!filterPairMap.keySet().contains(tempKeyValueSetEntry.getKey())) {
                iterator.remove();
            }
            else{
                Iterator< String > iteratorSet = tempKeyValueSetEntry.getValue().iterator();
                while (iteratorSet.hasNext()) {
                    String shop = iteratorSet.next();
                    if(!filterPairMap.get(tempKeyValueSetEntry.getKey()).contains(shop))
                        iteratorSet.remove();
                }
            }
        }

    }
}

