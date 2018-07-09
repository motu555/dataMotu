package process.others;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import data.RecDish;
import data.Review;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;
import util.CompareDish;
import util.FileOperation;
import util.TermSet;

import java.io.*;
import java.util.*;

/**
 * Created by wangkeqiang on 2016/4/7.
 * 用来填充用户评论中的favDishes字段的类
 */
public class DishesExtentsion_wkq {
    private Set<String> dishSet;
    private int dishNumber = 0;
    private int extentNumber = 0;
    private int processNumber = 100000;
    private int reviewNum = 0;
    private StringBuilder jsonStr;
    private String dataPath;
    private String cityCode;
    private String storePath;

    public DishesExtentsion_wkq(String dataPathParam, String cityCodeParam, String storePathParam) {
        this.dataPath = dataPathParam;
        this.cityCode = cityCodeParam;
        this.jsonStr = new StringBuilder("");
        this.dishSet = FileOperation.readLineSet(dataPathParam + "/" + cityCode + "/dishFilter2.txt");
        this.storePath = storePathParam;
        TermSet.loadTermLib(dataPathParam + "/" + cityCode + "/dishFilter2.txt");
    }


    /**
     * 将reveiw.json和review_filter.json中的favDishes字段利用dishFilter2.txt中的菜品列表进行过滤,
     * 并利用评论文本进行填充
     * 写入FavDishes.json
     */
    public void dishExtendAll() {
        this.dishExtendFilter();
        this.dishExtend();
        if (reviewNum % processNumber != 0) {
            FileOperation.writeAppdend(this.storePath + "/" + this.cityCode + "/FavDishes.json", this.jsonStr.toString());
        }
        System.out.println("Extend " + reviewNum + "th reviews " + " Dishes Number: " + dishNumber + " Extend Number: " + extentNumber + "\t" + new Date());
    }

    public void dishExtend() {
        String fileName = this.dataPath + "/" + cityCode + "/" + "reviews.json";
        String read = null;
        FileInputStream file = null;
        BufferedReader bufferedReader = null;
        Review rev;
        try {
            file = new FileInputStream(fileName);
            bufferedReader = new BufferedReader(new InputStreamReader(file));
            while ((read = bufferedReader.readLine()) != null) {

                rev = JSON.parseObject(read, Review.class);
                this.jsonStr.append(this.dishExtend(rev));
                this.jsonStr.append("\n");
                reviewNum++;
                if (reviewNum % processNumber == 0) {
                    System.out.println("Extend " + reviewNum + "th reviews " + " Dishes Number: " + dishNumber + " Extend Number: " + extentNumber + "\t" + new Date());
                    FileOperation.writeAppdend(this.storePath + "/" + this.cityCode + "/FavDishes.json", this.jsonStr.toString());
                    this.jsonStr = new StringBuilder("");
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
    }

    public void dishExtendFilter() {
        String fileName = this.dataPath + "/" + cityCode + "/" + "review_filter.json";
        String read = null;
        FileInputStream file = null;
        BufferedReader bufferedReader = null;
        Review rev;
        try {
            file = new FileInputStream(fileName);
            bufferedReader = new BufferedReader(new InputStreamReader(file));
            while ((read = bufferedReader.readLine()) != null) {
                rev = JSON.parseObject(read, Review.class);
                this.jsonStr.append(this.dishExtend(rev));
                this.jsonStr.append("\n");
                reviewNum++;
                if (reviewNum % processNumber == 0) {
                    System.out.println("Extend " + reviewNum + "th reviews " + " Dishes Number " + dishNumber + " Extend Number: " + extentNumber + "\t" + new Date());
                    FileOperation.writeAppdend(this.storePath + "/" + this.cityCode + "/FavDishes.json", this.jsonStr.toString());
                    this.jsonStr = new StringBuilder("");
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
    }

    /**
     *将用户评论中出现的菜名填充到用户的favDish字段中，注意这里可能有噪音，因为
     * 用户可能对所提到的菜品是负面评价，可能把某些负面评价也加进去了
     * @param review
     * @return
     */
    public String dishExtend(Review review) {
        Review re = new Review();
        re.setShopId(review.getShopId());
        re.setUserId(review.getUserId());
//      System.out.println(JSON.toJSONString(review,SerializerFeature.SortField));
        List<Term> termList = ToAnalysis.parse(review.getText());
        Set<String> dishOneReviewSet = new HashSet<>();
        if (review.getFavDishes() != null) {
            for (String dish : review.getFavDishes()) {
                if (dishSet.contains(dish))
                    dishOneReviewSet.add(dish);
            }
        }
        dishNumber += dishOneReviewSet.size();
        for (Term term : termList) {
            if (dishSet.contains(term.getName())) {
                dishOneReviewSet.add(term.getName());
            }
        }
        extentNumber += dishOneReviewSet.size();
        if (dishOneReviewSet.size() == 0) {
            dishOneReviewSet = null;
        }
        re.setFavDishes(dishOneReviewSet);
        re.setTaste(review.getTaste());
        re.setCondition(review.getCondition());
        re.setService(review.getService());
        String jsonString = JSON.toJSONString(re, SerializerFeature.SortField);
        return jsonString;
    }

    /**
     *按照菜名的长度进行排序？
     */
    public static void nameCount() {
        List<RecDish> dishesList = new ArrayList<>();
        Set<String> dishSet = FileOperation.readLineSet("F:\\datasets\\dianping\\1\\dishesFilterChar.txt");
        for (String dish : dishSet) {
            dishesList.add(new RecDish(dish, dish.length()));
        }
        Comparator<RecDish> RecDishCompare = new CompareDish();
        Collections.sort(dishesList, RecDishCompare);
        StringBuilder dishStr = new StringBuilder("");
        for (RecDish rd : dishesList) {
            dishStr.append(rd.getDish());
            dishStr.append("\t");
            dishStr.append(rd.getNumber());
            dishStr.append("\n");
        }
        FileOperation.writeNotAppdend("F:/datasets/dianping/1/dishCountSort.txt", dishStr.toString());
    }

    /**
     * 对dishSort.txt中的菜进行过滤，根据停用词和次数进行过滤
     * 得到dishSortFilter2.txt 带有菜名和频次
     * dishFilter2.txt  只有菜名
     */
    public static void dishesFilter() {
        List<String> dishSortList = FileOperation.readLineArrayList("F:/datasets/dianping/1/dishSort.txt");
        List<String> stopWords = FileOperation.readLineArrayList("stopLibrary.dic");
        StringBuilder dishStr = new StringBuilder("");
        StringBuilder dishStr2 = new StringBuilder("");
        for (String temp : dishSortList) {
            String[] strs = temp.split("\t");
            if (strs[0].length() > 1 && strs[0].length() < 11) {
                if (Integer.parseInt(strs[1]) > 2) {
                    boolean judge = true;
                    for (String stop : stopWords) {
                        if (strs[0].contains(stop)) {
                            judge = false;
                            break;
                        }
                    }
                    if (judge) {
                        dishStr.append(temp);
                        dishStr.append("\n");

                        dishStr2.append(strs[0]);
                        dishStr2.append("\n");
                    }
                }
            }
        }
        FileOperation.writeNotAppdend("F:/datasets/dianping/1/dishSortFilter2.txt", dishStr.toString());
        FileOperation.writeNotAppdend("F:/datasets/dianping/1/dishFilter2.txt", dishStr2.toString());
    }

    /**
     * 对上一步得到的dishSortFilter2.txt进行处理
     * 具体功能我不清楚
     */
    public static void segCount() {
        Map<String, Integer> segCountMap = new HashMap<>();
        Map<String, Integer> segCountSortMap = new HashMap<>();
        List<String> dishList = FileOperation.readLineArrayList("F:/datasets/dianping/1/dishSortFilter2.txt");
        for (String temp : dishList) {
            String[] strs = temp.split("\t");
            List<Term> segTerms = ToAnalysis.parse(strs[0]);
            for (Term term : segTerms) {
                if (segCountMap.containsKey(term.getName())) {
                    segCountMap.put(term.getName(), segCountMap.get(term.getName()) + 1);
                    segCountSortMap.put(term.getName(), segCountMap.get(term.getName()) + Integer.parseInt(strs[1]));
                } else {
                    segCountMap.put(term.getName(), 1);
                    segCountSortMap.put(term.getName(), Integer.parseInt(strs[1]));
                }
            }
        }

        List<RecDish> segCountList = new ArrayList<>();
        List<RecDish> segCountSortList = new ArrayList<>();
        for (String key : segCountMap.keySet()) {
            segCountList.add(new RecDish(key, segCountMap.get(key)));
            segCountSortList.add(new RecDish(key, segCountSortMap.get(key)));
        }

        Comparator<RecDish> RecDishCompare = new CompareDish();
        Collections.sort(segCountList, RecDishCompare);
        StringBuilder dishStr = new StringBuilder("");
        for (RecDish rd : segCountList) {
            dishStr.append(rd.getDish());
            dishStr.append("\t");
            dishStr.append(rd.getNumber());
            dishStr.append("\n");
        }
        FileOperation.writeNotAppdend("F:/datasets/dianping/1/seg2.txt", dishStr.toString());

        Collections.sort(segCountSortList, RecDishCompare);
        dishStr = new StringBuilder("");
        for (RecDish rd : segCountSortList) {
            dishStr.append(rd.getDish());
            dishStr.append("\t");
            dishStr.append(rd.getNumber());
            dishStr.append("\n");
        }
        FileOperation.writeNotAppdend("F:/datasets/dianping/1/segSort2.txt", dishStr.toString());
    }

    /**
     *利用dishSort2 manual.txt再次对FavDishes.json进行过滤
     * 保留存在与dishSort2_manual中的favDishes
     * 得到FavDishesManual.json
     */
    public static void jsonDishFilter() {
        List<String> dishSortList = FileOperation.readLineArrayList("F:/datasets/dianping/1/dishSort2 manual.txt");
        Set<String> dishSet = new HashSet<>();
        for (String temp : dishSortList) {
            dishSet.add(temp.split("\t")[0]);
        }

        String fileName = "F:/datasets/dianping/1/FavDishes.json";
        String encoding = "UTF-8";

        Map<String, Integer> dishMap = new HashMap<>();

        String read = null;
        FileInputStream file = null;
        BufferedReader bufferedReader = null;
        Review rev;
        int count = 0;
        StringBuilder jsonStr = new StringBuilder("");
        try {
            file = new FileInputStream(fileName);
            bufferedReader = new BufferedReader(new InputStreamReader(file, encoding));
            while ((read = bufferedReader.readLine()) != null) {

                rev = JSON.parseObject(read, Review.class);
                Set<String> oneDishSet = new HashSet<>();
                if (rev.getFavDishes() != null) {
                    for (String dish : rev.getFavDishes()) {
                        if (dishSet.contains(dish)) {
                            oneDishSet.add(dish);
                        }
                    }
                    rev.setFavDishes(oneDishSet);
                }
                jsonStr.append(JSON.toJSONString(rev, SerializerFeature.SortField));
                jsonStr.append("\n");
                count++;
                if (count % 1000000 == 0) {
                    System.out.println("Count up " + count + "th reviews " + new Date());
                    FileOperation.writeAppdend("F:/datasets/dianping/1/FavDishesManual.json", jsonStr.toString());
                    jsonStr = new StringBuilder("");
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
        if (count % 1000000 != 0) {
            System.out.println("Count up " + count + "th reviews " + new Date());
            FileOperation.writeAppdend("F:/datasets/dianping/1/FavDishesManual.json", jsonStr.toString());
        }
    }


    /**
     * 将FavDishesManual.json中评论的FavDishes不为空的reveiw挑出来写入FavDishesManualNo1.json
     */
    public static void jsonNoishFilter() {
        String fileName = "F:/datasets/dianping/1/FavDishesManual.json";
        String encoding = "UTF-8";

        Map<String, Integer> dishMap = new HashMap<>();

        String read = null;
        FileInputStream file = null;
        BufferedReader bufferedReader = null;
        Review rev;
        int count = 0;
        StringBuilder jsonStr = new StringBuilder("");
        try {
            file = new FileInputStream(fileName);
            bufferedReader = new BufferedReader(new InputStreamReader(file, encoding));
            while ((read = bufferedReader.readLine()) != null) {

                rev = JSON.parseObject(read, Review.class);
                if (rev.getFavDishes() != null  ) {
                    if(rev.getFavDishes().size()>0) {
                        jsonStr.append(JSON.toJSONString(rev, SerializerFeature.SortField));
                        jsonStr.append("\n");
                        count++;
                    }
                }

                if (count % 1000000 == 0) {
                    System.out.println("Count up " + count + "th reviews " + new Date());
                    FileOperation.writeAppdend("F:/datasets/dianping/1/FavDishesManualNo1.json", jsonStr.toString());
                    jsonStr = new StringBuilder("");
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
        if (count % 1000000 != 0) {
            System.out.println("Count up " + count + "th reviews " + new Date());
            FileOperation.writeAppdend("F:/datasets/dianping/1/FavDishesManualNo1.json", jsonStr.toString());
        }
    }

    public static void main(String[] args) {
        String dataPath = "D:/Data/OriginData/wkq_DianpingData/2";
        String storePath = "D:/Data/OriginData/wkq_DianpingData/2";
        FileOperation.makeDir(storePath);
        DishesExtentsion_wkq de = new DishesExtentsion_wkq(dataPath, "2", storePath);
        de.dishExtendAll();

        /**
         * check是否需要先做这几步，然后再进行disheExtendAll
         */
        //对dishSort.txt中的菜品根据停用词和其在评论的favDishes字段中出现的次数
        DishesExtentsion_wkq.dishesFilter();
        DishesExtentsion_wkq.segCount();
        DishesExtentsion_wkq.jsonNoishFilter();
    }

}
