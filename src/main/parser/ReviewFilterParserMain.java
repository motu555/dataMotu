package main.parser;

import org.htmlparser.util.ParserException;
import parser.ReviewFilterParser;
//import parser.ReviewParser;
import util.FileOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * Created by wangkeqiang on 2016/4/4.
 */
public class ReviewFilterParserMain {
    public static void main(String[] args) throws IOException, ParserException, InterruptedException {
        /**
         * 这个地方要写绝对路径
         */
        String dataPath = "D:/Data/OriginData/大众点评";
        String storePath = "D:/Data/ProcessedData/Dianping/大众点评评论数据集/点评数据_jyy预处理";
        FileOperation.makeDir(storePath);

        List<String> cityAllList = FileOperation.readLineArrayList("dataProcessConfig/cityList.txt");
        System.out.println("citylist中共有城市\t" + cityAllList.size());
        /**
         * TODO notice!!! 当要处理多个城市的数据时，需要配置
         */
        int cityStart = 1;
        int cityEnd = 1;
        cityStart = cityStart > 0 ? cityStart : 1;
        cityEnd = cityEnd < cityAllList.size() ? cityEnd : cityAllList.size();

        List<String> cityList = new ArrayList<>();


        FileOperation.makeDir("cache");
        FileOperation.makeFile("cache/cityReivewFilterParsed.txt");
        Set<String> cityReviewParseSet = FileOperation.readLineSet("cache/cityReivewFilterParsed.txt");

        for (int i = cityStart - 1; i <= cityEnd - 1; i++) {
            if (!cityReviewParseSet.contains(cityAllList.get(i))) {
                cityList.add(cityAllList.get(i));
            }
        }

        for (String cityCode : cityList) {
            System.out.println("处理城市\t" + cityCode);
            FileOperation.makeFile("cache/" + cityCode + "reivewFilterParsed.txt");
            Set<String> parsedSet = FileOperation.readLineSet("cache/" + cityCode + "reivewFilterParsed.txt");

            List<String> idPathAllList = FileOperation.readAllFilePathCons(dataPath + "/" + cityCode + "/review_filtered/", "txt");
            List<String> idList = new ArrayList<>();

            int startLength = (dataPath + "/" + cityCode + "/review_filtered/").length();

            for (String idPath : idPathAllList) {
                String id = idPath.substring(startLength).replace(".txt", "");
                if (!parsedSet.contains(id)) {
                    idList.add(id);
                }
            }

            FileOperation.makeDir(storePath + "/" + cityCode);
            ReviewFilterParser rfp = new ReviewFilterParser(dataPath, cityCode, storePath, idList);

            /**
             * 非多线程版本
             */
//            rfp.parseAllShopReview();
            /**
             * 多线程版本
             */
            CountDownLatch countDownLatch = rfp.parseAllShopReviewParallel();
            countDownLatch.await();
        }
    }
}
