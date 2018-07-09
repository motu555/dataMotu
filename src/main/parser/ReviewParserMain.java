package main.parser;

import parser.ReviewParser;
import util.FileOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * Created by wangkeqiang on 2016/4/3.
 * 用于将评论的html文件转为json格式的总入口
 */
public class ReviewParserMain {
    public static void main(String[] args) throws IOException, InterruptedException {
        /**
         * 这个地方要写绝对路径， 4个线程跑了2h
         */
        String dataPath = "D:/Data/OriginData/大众点评";
        String storePath = "D:/Data/ProcessedData/Dianping/大众点评评论数据集/点评数据_jyy预处理";

        FileOperation.makeDir(storePath);

        List<String> cityAllList = FileOperation.readLineArrayList("dataProcessConfig/cityList.txt");
        System.out.println("citylist中共有城市\t" + cityAllList.size());
        /**
         * cityStart 到 cityEnd限制了要读取的city列表
         *  /**
         *          * TODO notice!!! 当要处理多个城市的数据时，需要配置
         *          */

        int cityStart = 1;
        int cityEnd = 2;
        cityStart = cityStart > 0 ? cityStart : 1;
        cityEnd = cityEnd < cityAllList.size() ? cityEnd : cityAllList.size();

        List<String> cityList = new ArrayList<>();


        FileOperation.makeDir("cache");
        FileOperation.makeFile("cache/cityReivewParsed.txt");
        Set<String> cityReviewParseSet = FileOperation.readLineSet("cache/cityReivewParsed.txt");

        for (int i = cityStart - 1; i <= cityEnd - 1; i++) {
            if (!cityReviewParseSet.contains(cityAllList.get(i))) {
                cityList.add(cityAllList.get(i));
            }
        }

        for (String cityCode : cityList) {
            System.out.println("处理城市\t" + cityCode);
            FileOperation.makeFile("cache/" + cityCode + "reivewParsed.txt");
            Set<String> parsedSet = FileOperation.readLineSet("cache/" + cityCode + "reivewParsed.txt");

            List<String> idPathAllList = FileOperation.readAllFilePathCons(dataPath + "/" + cityCode + "/reviews/", "txt");
            List<String> idList = new ArrayList<>();

            int startLength = (dataPath + "/" + cityCode + "/reviews/").length();

            for (String idPath : idPathAllList) {
                String id = idPath.substring(startLength).replace(".txt", "");
                if (!parsedSet.contains(id)) {
                    idList.add(id);
                }
            }

            FileOperation.makeDir(storePath + "/" + cityCode);
            ReviewParser rp = new ReviewParser(dataPath, cityCode, storePath, idList);
            //非并行版本
//            rp.parseAllShopReview();

//            并行版本
            CountDownLatch countDownLatch = rp.parseAllShopReviewParallel();
            countDownLatch.await();
        }
    }
}
