package parser;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.sun.scenario.effect.impl.sw.sse.SSEBlend_SRC_OUTPeer;
import data.Review;
import org.htmlparser.util.NodeList;
import util.FileOperation;
import util.SimpleHtmlParser;

import java.io.*;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Pattern;


/**
 * Created by wangkeqiang on 2016/4/2.
 */
public class ReviewParser {
    final static int reviewIdStrLength = "dr-referid=\"".length();
    final static int userIdStrLength = "dr-referuserid=\"".length();

    final static String reviewIdStart = "dr-referid=\"";
    final static String reviewIdEnd = "\" dr-referuserid=\"";

    final static String userIdStart = "dr-referuserid=\"";
    final static String userIdEnd = "\">不当内容</a>";


    //the set of review id
//    private Set<String> reviewIdSet;
    private StringBuilder reviewJson;
    private String dataPath;
    private String storePath;
    private String cityCode;
    private int reviewNum = 0;
    //    final static int storeNum = 10000;
    final static int shopNumProcess = 100;
    private List<String> shopList;
    private String reviewPath;
    private StringBuilder shopIdStr;


    public ReviewParser(String dataPathParam, String cityCodeParam, String storePathParam, List<String> shopListParam) {
//        this.reviewIdSet = new HashSet<>();
        this.reviewJson = new StringBuilder("");
        this.dataPath = dataPathParam;
        this.cityCode = cityCodeParam;
        this.storePath = storePathParam;
        this.shopList = shopListParam;
        this.reviewPath = this.dataPath + "/" + cityCode + "/reviews/";
    }

    public ReviewParser() {

    }

    /**
     * parse all shop reviews of a city cityCode
     */
    public void parseAllShopReview() throws IOException {
        System.out.println("Need to parse " + shopList.size() + " Shop reviews");
        int shopNum = 0;
        shopIdStr = new StringBuilder("");
        for (String shopId : shopList) {
            shopNum++;
//            System.out.println(shopId);
            String content = FileOperation.readAutoDianping(this.reviewPath + shopId + ".txt");
            if (content.length() > 30) {
                if (!content.contains("不当内容")) {
                    content = FileOperation.read(this.reviewPath + shopId + ".txt", "UTF-8");
                }
                if (!content.contains("不当内容")) {
                    content = FileOperation.read(this.reviewPath + shopId + ".txt", "GB2312");
                }
                if (!content.contains("不当内容")) {
                    System.out.println(shopId + " " + content.length() + "\n" + content);
                    System.exit(0);
                }
                this.parseAllReview(content, shopId);
            }
            shopIdStr.append(shopId);
            shopIdStr.append("\n");
            if (shopNum % shopNumProcess == 0) {
                System.out.println("Parse " + shopNum + " shops and parse " + reviewNum + " reviews! " + new Date());
                FileOperation.writeAppdend(this.storePath + "/" + this.cityCode + "/reviews.json", this.reviewJson.toString());
                FileOperation.writeAppdend("cache/" + cityCode + "reivewParsed.txt", shopIdStr.toString());
                this.reviewJson = new StringBuilder("");
                this.shopIdStr = new StringBuilder("");
            }
        }
        if (shopNum % shopNumProcess != 0) {
            FileOperation.writeAppdend(this.storePath + "/" + this.cityCode + "/reviews.json", this.reviewJson.toString());
            FileOperation.writeAppdend("cache/" + cityCode + "reivewParsed.txt", shopIdStr.toString());
        }

        System.out.println("Parse " + shopList.size() + " shops and parse " + reviewNum + " reviews! " + new Date());
        System.out.println("Parse Over!");
    }

    /**
     * 多线程版本，手动将其分为4个多线程，加锁同步后写入同一个文件
     *
     * @return
     * @throws IOException
     */
    public CountDownLatch parseAllShopReviewParallel() throws IOException {
        System.out.println("Need to parse " + shopList.size() + " Shop reviews");
        /**
         * 构建多线程的index列表
         * 开4个线程，每个线程中的任务串行
         */
        int shopNum = shopList.size();
        //线程总数
        int threadNum = 4;
        //每个线程内串行任务的个数
        int threadSize = shopNum / threadNum;
        //串行任务划分到4个线程中
        int[] threadStep = new int[threadNum + 1];
        threadStep[0] = 0;
        for (int i = 1; i < threadNum; i++) {
            threadStep[i] = threadSize * i;
        }
        threadStep[threadNum] = shopNum;

        //初始化countdown数目为线程数
        CountDownLatch countDownAllLatch = new CountDownLatch(threadNum);

        for (int i = 0; i < threadStep.length - 1; i++) {
            int start = threadStep[i];
            int end = threadStep[i + 1];
            int interval = end - start;
            int stepIndex = i;
            System.out.println("start\t" + start + "\tend\t" + end + "\tinterval\t" + interval);
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    int processedShopNum = 0;
                    int reviewNum = 0;
                    StringBuilder reviewJson = new StringBuilder();
                    StringBuilder shopIdStr = new StringBuilder();
                    for (int j = start; j < end; j++) {
                        String shopId = shopList.get(j);
                        processedShopNum++;
                        String content = FileOperation.readAutoDianping(reviewPath + shopId + ".txt");
                        if (content.length() > 30) {
                            if (!content.contains("不当内容")) {
                                content = FileOperation.read(reviewPath + shopId + ".txt", "UTF-8");
                            }
                            if (!content.contains("不当内容")) {
                                content = FileOperation.read(reviewPath + shopId + ".txt", "GB2312");
                            }
                            if (!content.contains("不当内容")) {
                                System.out.println(shopId + " " + content.length() + "\n" + content);
                                System.exit(0);
                            }
                            reviewNum += parseAllReview(content, shopId, reviewJson);
                        }
                        shopIdStr.append(shopId);
                        shopIdStr.append("\n");
                        if (processedShopNum % shopNumProcess == 0) {
                            System.out.println("Parse " + processedShopNum + " shops and parse " + reviewNum + " reviews! " + new Date());
                            synchronized (ReviewParser.class) {
                                FileOperation.writeAppdend(storePath + "/" + cityCode + "/reviews.json", reviewJson.toString());
                                FileOperation.writeAppdend("cache/" + cityCode + "reivewParsed.txt", shopIdStr.toString());
                            }
                            reviewJson = new StringBuilder("");
                            shopIdStr = new StringBuilder("");
                        }
                    }
                    if (processedShopNum % shopNumProcess != 0) {
                        synchronized (ReviewParser.class) {
                            FileOperation.writeAppdend(storePath + "/" + cityCode + "/reviews.json", reviewJson.toString());
                            FileOperation.writeAppdend("cache/" + cityCode + "reivewParsed.txt", shopIdStr.toString());
                        }
                    }

                    System.out.println("thread step:\t" + stepIndex + " Parse " + interval + " shops and parse " + reviewNum + " reviews! " + new Date());
                    System.out.println("Parse Over!");
                    countDownAllLatch.countDown();
                }
            });
            thread.start();
        }
        return countDownAllLatch;
    }


    /**
     * parse one shop's reviews
     *
     * @param content
     * @param shopId
     */
    public void parseAllReview(String content, String shopId) {
        NodeList commentList = SimpleHtmlParser.parserHtml(content, "div", "class", "content");
        int size = commentList.size();
//        System.out.println(size);
        for (int i = 0; i < size; i++) {
            String reviewContent = commentList.elementAt(i).toHtml();
            String reviewInfo = this.parseOneReview(reviewContent, shopId);
            if (reviewInfo != null) {
                this.reviewJson.append(reviewInfo);
                this.reviewJson.append("\n");
                reviewNum++;
            }
        }
    }

    public int parseAllReview(String content, String shopId, StringBuilder reviewJson) {
        NodeList commentList = SimpleHtmlParser.parserHtml(content, "div", "class", "content");
        int size = commentList.size();
//        System.out.println(size);
        int reviewNum = 0;
        for (int i = 0; i < size; i++) {
            String reviewContent = commentList.elementAt(i).toHtml();
            String reviewInfo = this.parseOneReview(reviewContent, shopId);
            if (reviewInfo != null) {
                reviewJson.append(reviewInfo);
                reviewJson.append("\n");
                reviewNum++;
            }
        }
        return reviewNum;
    }

    /**
     * parse the review information from the html content
     *
     * @param content
     */
    public String parseOneReview(String content, String shopId) {
        String[] ids = this.getIds(content);
        if (ids[0] == null) {
            System.out.println(content);
            System.out.println("ids[0] is null\t" + shopId);
            return null;
        }
        Set<String> reviewIdSet = new HashSet<>();

        if (!reviewIdSet.contains(ids[0])) {
            Review review = new Review();
            review.setReviewId(ids[0]);
            review.setShopId(shopId);
            review.setUserId(ids[1]);
            review.setPrice(this.getPrice(content));

            review.setOverAllRating(this.getOverallRating(content));
            int[] ratings = this.getRatings(content);
            review.setTaste(ratings[0]);
            review.setCondition(ratings[1]);
            review.setService(ratings[2]);

            Set<String>[] tags = this.getTags(content);
            //喜欢的菜
            review.setFavDishes(tags[0]);
            //给餐厅打的tag
            review.setCharacteristic(tags[1]);

            review.setTime(this.getTime(content));
            review.setText(this.getText(content));
            review.setCommentType(this.getCommentType(content));
            review.setThumbs(this.getThumbs(content));

            String jsonString = JSON.toJSONString(review, SerializerFeature.SortField);
            reviewIdSet.add(ids[0]);
            return jsonString;
        } else {
            return null;
        }
    }

    /**
     * <span class="comm-per">
     *
     * @param content
     * @return average price
     */
    public int getPrice(String content) {
        NodeList priceNodes = SimpleHtmlParser.parserHtml(content, "span", "class", "comm-per");
        if (priceNodes.size() > 0) {
            return Integer.parseInt(Pattern.compile("[^0-9]").matcher(priceNodes.elementAt(0).toPlainTextString()).replaceAll(""));
        }
        return -1;
    }

    /**
     * <a rel="nofollow" href="javascript:"  class="J_pop-reviewReport" dr-referid="244425977" dr-referuserid="796181914">
     *
     * @param content
     * @return review id and user id
     */
    public String[] getIds(String content) {
        String[] ids = new String[2];
        NodeList idNodes = SimpleHtmlParser.parserHtml(content, "a", "class", "J_pop-reviewReport");
        if (idNodes.size() > 0) {
            String idsStr = idNodes.elementAt(0).toHtml();

//            System.out.println(idsStr);
            ids[0] = idsStr.substring(idsStr.indexOf(reviewIdStart) + reviewIdStrLength, idsStr.indexOf(reviewIdEnd));
            ids[1] = idsStr.substring(idsStr.indexOf(userIdStart) + userIdStrLength, idsStr.indexOf(userIdEnd));
        } else {
            ids[0] = null;
            ids[1] = null;
        }
//        System.out.println(idsStr);
        return ids;
    }

    /**
     * <div class="comment-txt">
     * p class="comment-type">团购点评</p>
     * <div class="J_brief-cont">
     * 不错…………………………………
     * </div>
     * </div>
     *
     * @param content
     * @return review text of the user for the shop
     */
    public String getText(String content) {
        NodeList reviewNodes = SimpleHtmlParser.parserHtml(content, "div", "class", "J_brief-cont");
        if (reviewNodes.size() > 0) {
            return reviewNodes.elementAt(0).toPlainTextString().trim().replaceAll(" ", "").replace("&nbsp", "");
        } else {
            return null;
        }
    }

    /**
     * p class="comment-type">团购点评</p>
     *
     * @param content
     * @return
     */
    public String getCommentType(String content) {
        NodeList reviewNodes = SimpleHtmlParser.parserHtml(content, "p", "class", "comment-type");
        if (reviewNodes.size() > 0) {
            return reviewNodes.elementAt(0).toPlainTextString().trim().replaceAll(" ", "").replace("&nbsp", "");
        } else {
            return null;
        }
    }

    /**
     * <div class="comment-recommend">
     *
     * @param content
     * @return 喜欢的菜 and 餐厅特色（or 推荐菜 and 标签）
     * <p>
     * dish: <a class="col-exp" href="http://www.dianping.com/shop/2278378/dish-%E6%B0%B4%E7%85%AE%E9%B2%B6%E9%B1%BC" target="_blank" >
     * characteristic: <a class="col-exp"  title="" href="javascript:">朋友聚餐</a>
     */
    public Set<String>[] getTags(String content) {
        Set<String>[] tags = new HashSet[2];
        tags[0] = null;
        tags[1] = null;

        NodeList tagNodes = SimpleHtmlParser.parserHtml(content, "div", "class", "comment-recommend");
        int size = tagNodes.size();
        for (int i = 0; i < size; i++) {
            String subContent = tagNodes.elementAt(i).toHtml();
            NodeList nodes = SimpleHtmlParser.parserHtml(subContent, "a", "class", "col-exp");
            int tagSize = nodes.size();
            if (tagSize > 0) {
                if (subContent.contains("推荐菜：") || subContent.contains("喜欢的菜：")) {
                    tags[0] = new HashSet<>();
                    for (int d = 0; d < tagSize; d++) {
                        tags[0].add(nodes.elementAt(d).toPlainTextString().trim().replaceAll(" ", ""));
                    }
                } else {
                    tags[1] = new HashSet<>();
                    for (int d = 0; d < tagSize; d++) {
                        tags[1].add(nodes.elementAt(d).toPlainTextString().trim().replaceAll(" ", ""));
                    }
                }
            }
        }
        return tags;
    }

    /**
     * get the review time <span class="time">07-11-25</span>
     *
     * @param content
     * @return time yy-mm-dd
     */
    public String getTime(String content) {
        NodeList timeNodes = SimpleHtmlParser.parserHtml(content, "span", "class", "time");
        String time = null;
        if (timeNodes.size() > 0) {
            time = timeNodes.elementAt(0).toPlainTextString().trim();
            if (time.split("-").length < 3) {
                time = "16-" + time;
            }
        }
        return time;
    }

    /**
     * <div class="comment-rst">
     * <em class="sep">|</em>
     * <span class="rst">口味3<em class="col-exp">(很好)</em></span>
     * <span class="rst">环境2<em class="col-exp">(好)</em></span>
     * <span class="rst">服务3<em class="col-exp">(很好)</em></span>
     * </div>
     *
     * @param content
     * @return the ratings of 口味，环境和服务（0-4）
     */
    public int[] getRatings(String content) {
        int[] ratings = new int[3];
        ratings[0] = -1;
        ratings[1] = -1;
        ratings[2] = -1;
        NodeList ratingsNodes = SimpleHtmlParser.parserHtml(content, "span", "class", "rst");
        if (ratingsNodes.size() > 0) {
            for (int i = 0; i < ratingsNodes.size(); i++) {
                String subContent = ratingsNodes.elementAt(i).toPlainTextString().trim();
                if (subContent.contains("口味")) {
                    ratings[0] = Integer.parseInt(subContent.substring(2, 3));
                } else if (subContent.contains("环境")) {
                    ratings[1] = Integer.parseInt(subContent.substring(2, 3));
                } else if (subContent.contains("服务")) {
                    ratings[2] = Integer.parseInt(subContent.substring(2, 3));
                }
            }
        }
        return ratings;
    }
    /**
     *  <div class="user-info">
     *             <span title="非常好" class="item-rank-rst irr-star50"></span>
     *             <span class="comm-per">人均 ￥70</span>
     *             <div class="comment-rst">
     *                 <em class="sep">|</em>
     *                 <span class="rst">口味4<em class="col-exp">(非常好)</em></span>
     *                 <span class="rst">环境4<em class="col-exp">(非常好)</em></span>
     *                 <span class="rst">服务4<em class="col-exp">(非常好)</em></span>
     *             </div>
     *
     *         </div>
     *  得到用户对餐厅的总评， 将文字映射为0-4的分数
     *
     */
    public int getOverallRating(String content) {
       int overAllRating = -1;
        NodeList ratingsNodes = SimpleHtmlParser.parserHtml(content, "div", "class", "user-info");
        if (ratingsNodes.size() > 0) {
            for (int i = 0; i < ratingsNodes.size(); i++) {
                String subContent = ratingsNodes.elementAt(i).toHtml();
                NodeList nodesList = getOverallRatingNode(subContent);
                if(nodesList.size() > 0){
                overAllRating = mapOverAllRating(nodesList.elementAt(0).toHtml().split(" ")[1].substring(7).replace("\"",""));
                }
            }
        }
        return overAllRating;
    }

    public static NodeList getOverallRatingNode(String subContent){
        NodeList nodesList = null;
        for(int i = 1;i <= 5; i++){
            String attrValue = "item-rank-rst irr-star" + i + "0";
            nodesList = SimpleHtmlParser.parserHtml(subContent, "span", "class", attrValue);
            if(nodesList.size() > 0){
                break;
            }
        }
        return nodesList;
    }
    /**
     * <span class="heart-name">赞</span>
     * <span class="heart-num">(2)</span>
     *
     * @param content
     * @return the number of Thumbs up
     */
    public int getThumbs(String content) {
        int thumbs = -1;
        NodeList thumbsNodes = SimpleHtmlParser.parserHtml(content, "span", "class", "heart-num");
        if (thumbsNodes.size() > 0) {
            thumbs = Integer.parseInt(Pattern.compile("[^0-9]").matcher(thumbsNodes.elementAt(0).toPlainTextString()).replaceAll(""));
        }
        return thumbs;
    }

    public static int mapOverAllRating(String text){
        int rating = 0;
        switch(text){
            case "非常好":
                rating = 4;
                break;
            case "很好":
                rating = 3;
                break;
            case "好":
                rating = 2;
                break;
            case "差":
                rating = 1;
                break;
            case "很差":
                rating = 0;
                break;
        }
//        System.out.println(text + "\t" + rating);
        return rating;
    }


    public static void main(String[] args) throws IOException {
        String content = FileOperation.readAutoDianping("GenerateFeatures.html");
        ReviewParser rp = new ReviewParser();
        rp.parseOneReview(content, "1");
    }
}
