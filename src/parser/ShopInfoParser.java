package parser;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import data.RecDish;
import data.Shop;
import data.Star;
import org.htmlparser.util.NodeList;
import org.htmlparser.util.ParserException;
import util.FileOperation;
import util.SimpleHtmlParser;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Created by wangkeqiang on 2016/4/3.
 */
public class ShopInfoParser {
    private StringBuilder shopJson;
    private String dataPath;
    private String storePath;
    private String cityCode;
    private int reviewNum = 0;
    //    final static int storeNum = 10000;
    final static int shopNumProcess = 100;
    private List<String> shopList;
    private String shopInfoPath;
    private StringBuilder shopIdStr;

    public ShopInfoParser(String dataPathParam, String cityCodeParam, String storePathParam, List<String> shopListParam) {
//        this.reviewIdSet = new HashSet<>();
        this.shopJson = new StringBuilder("");
        this.dataPath = dataPathParam;
        this.cityCode = cityCodeParam;
        this.storePath = storePathParam;
        this.shopList = shopListParam;
        this.shopInfoPath = this.dataPath + "/" + cityCode + "/shopInfo/";
    }

    public void parseAllShop() throws ParserException {
        System.out.println("Need to parse " + shopList.size() + " Shop info");
        int shopNum = 0;
        shopIdStr = new StringBuilder("");

        for (String shopId : shopList) {
            shopNum++;
//            System.out.println(shopId);
            String content = FileOperation.readAutoDianping(this.shopInfoPath + shopId + ".txt");
            if (content.length() > 100) {
                if (!content.contains("添加分店")) {
                    content = FileOperation.read(this.shopInfoPath + shopId + ".txt", "UTF-8");
                }
                if (!content.contains("添加分店")) {
                    content = FileOperation.read(this.shopInfoPath + shopId + ".txt", "GB2312");
                }
                if (!content.contains("添加分店")) {
                    System.out.println(shopId + " " + content.length() + "\n" + content);
                    System.exit(0);
                }
                this.shopJson.append(this.parseOneShop(content, shopId));
                this.shopJson.append("\n");
            }
            shopIdStr.append(shopId);
            shopIdStr.append("\n");
            if (shopNum % shopNumProcess == 0) {
                System.out.println("Parse " + shopNum + " shops and parse " + new Date());
                FileOperation.writeAppdend(this.storePath + "/" + this.cityCode + "/shopInfo/shopInfo.json", this.shopJson.toString());
                FileOperation.writeAppdend("cache/" + cityCode + "shopInfoParsed.txt", shopIdStr.toString());
                this.shopJson = new StringBuilder("");
                this.shopIdStr = new StringBuilder("");
            }
        }
        if (shopNum % shopNumProcess != 0) {
            FileOperation.writeAppdend(this.storePath + "/" + this.cityCode + "/shopInfo/shopInfo.json", this.shopJson.toString());
            FileOperation.writeAppdend("cache/" + cityCode + "shopInfoParsed.txt", shopIdStr.toString());

        }
        System.out.println("Parse " + shopList.size() + " shops " + new Date());
        System.out.println("Parse Over!");
    }

    /**
     * 多线程执行，加锁后写入同一个文件
     * 每个线程内的数据结构是分开不共享的
     *
     * @return
     * @throws ParserException
     */
    public CountDownLatch parseAllShopParallel() throws ParserException {
        System.out.println("Need to parse " + shopList.size() + " Shop info");
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
                    StringBuilder shopJson = new StringBuilder();
                    StringBuilder shopIdStr = new StringBuilder();
                    for (int j = start; j < end; j++) {
                        String shopId = shopList.get(j);
                        processedShopNum++;
                        String content = FileOperation.readAutoDianping(shopInfoPath + shopId + ".txt");
                        if (content.length() > 100) {
                            if (!content.contains("添加分店")) {
                                content = FileOperation.read(shopInfoPath + shopId + ".txt", "UTF-8");
                            }
                            if (!content.contains("添加分店")) {
                                content = FileOperation.read(shopInfoPath + shopId + ".txt", "GB2312");
                            }
                            //shop 39261139 不包含 “添加分店”
                            if (!content.contains("添加分店")) {
                                System.out.println(shopId + " " + content.length() + "\n" + content);
                                System.exit(0);
                            }
                            try {
                                shopJson.append(parseOneShop(content, shopId));
                            } catch (ParserException e) {
                                e.printStackTrace();
                            }
                            shopJson.append("\n");
                        }
                        shopIdStr.append(shopId);
                        shopIdStr.append("\n");
                        if (processedShopNum % shopNumProcess == 0) {
                            System.out.println("Parse " + processedShopNum + " shops and parse " + new Date());
                            synchronized (ShopInfoParser.class) {
                                FileOperation.writeAppdend(storePath + "/" + cityCode + "/shopInfo/shopInfo.json", shopJson.toString());
                                FileOperation.writeAppdend("cache/" + cityCode + "shopInfoParsed.txt", shopIdStr.toString());
                            }
                            shopJson = new StringBuilder("");
                            shopIdStr = new StringBuilder("");
                        }
                    }
                    if (processedShopNum % shopNumProcess != 0) {
                        synchronized (ShopInfoParser.class) {
                            FileOperation.writeAppdend(storePath + "/" + cityCode + "/shopInfo/shopInfo.json", shopJson.toString());
                            FileOperation.writeAppdend("cache/" + cityCode + "shopInfoParsed.txt", shopIdStr.toString());
                        }
                    }
                    System.out.println("thread step:\t" + stepIndex + " Parse " + interval + " shops!" + new Date());
                    System.out.println("Parse Over!");
                    countDownAllLatch.countDown();
                }
            });
            thread.start();
        }
        return countDownAllLatch;
    }

    /**
     * parse the shop information from the html content
     *
     * @param content
     */
    public String parseOneShop(String content, String shopId) throws ParserException {
        System.out.println("parse shop\t" + shopId);
        Shop shop = new Shop();
        shop.setShopId(shopId);
        shop.setShopName(this.getShopName(content));
        String[] addresses = this.getAddress(content);
        shop.setAddress(addresses[1]);
        shop.setRegion(addresses[0]);
        shop.setCityCode(this.cityCode);
        shop.setPhoneNumbers(this.getPhoneNumbers(content));
        shop.setTags(this.getTags(content));
        shop.setTime(this.getTime(content));
        shop.setIntro(this.getIntro(content));
        shop.setBreadcrumb(this.getBreadcrumb(content));
        double brief[] = this.getBrief(content);
        shop.setPrice(brief[0]);
        shop.setTaste(brief[1]);
        shop.setCondition(brief[2]);
        shop.setService(brief[3]);
        shop.setStar(this.getStar(content)); /**
         * 加上推荐菜这个属性的读取
         */
        shop.setRecDishes(this.getRecDishes(content, shopId));
        String[] lnglat = this.getLngLat(content);
        shop.setLng(lnglat[0]);
        shop.setLat(lnglat[1]);
        String jsonString = JSON.toJSONString(shop, SerializerFeature.SortField);
        return jsonString;

    }

    /**
     * <div class="breadcrumb">
     * <a href="http://www.dianping.com/shanghai/food" itemprop="url">上海餐厅</a>&gt;
     * <a href="http://www.dianping.com/search/category/1/10/r5" itemprop="url">浦东新区</a>&gt;
     * <a href="http://www.dianping.com/search/category/1/10/r801" itemprop="url">陆家嘴</a>&gt;
     * <a href="http://www.dianping.com/search/category/1/10/g117r801" itemprop="url">面包甜点</a>&gt;
     * <a href="http://www.dianping.com/search/category/1/10/g241r801" itemprop="url">冰淇淋</a>&gt;
     * <span>哈根达斯(正大星美影院店)</span>
     * </div>
     *
     * @param content
     * @return the shop's name
     */
    public String getShopName(String content) {
        NodeList shopNameNodes = SimpleHtmlParser.parserHtml(content, "div", "class", "breadcrumb");
        String shopName = null;
        if (shopNameNodes.size() > 0) {
            String subContent = shopNameNodes.elementAt(0).toHtml();
            shopNameNodes = SimpleHtmlParser.parserHtml(subContent, "span");
            if (shopNameNodes.size() > 0) {
                shopName = shopNameNodes.elementAt(0).toPlainTextString().trim();
            }
        }
        return shopName;
    }

    /**
     * <div class="expand-info address" itemprop="street-address">
     * <span class="info-name">地址：</span>
     * <a href="/search/category/1/10/r5" rel="nofollow" target="_blank">
     * <span itemprop="locality region">浦东新区</span>
     * </a>
     * <span class="item" itemprop="street-address" title="陆家嘴西路168号正大广场1楼01-31(近东方明珠)">
     * 陆家嘴西路168号正大广场1楼01-31(近东方明珠)
     * </span>
     * </div>
     *
     * @param content
     * @return locality region and street-address
     */
    public String[] getAddress(String content) {
        String[] address = new String[2];
        address[0] = null;
        address[1] = null;
        NodeList addressNodes = SimpleHtmlParser.parserHtml(content, "div", "class", "expand-info address");
        if (addressNodes.size() > 0) {
            content = addressNodes.elementAt(0).toHtml();
            NodeList regionNode = SimpleHtmlParser.parserHtml(content, "span", "itemprop", "locality region");
            if (regionNode.size() > 0) {
                address[0] = regionNode.elementAt(0).toPlainTextString().trim();
            }

            NodeList streetNode = SimpleHtmlParser.parserHtml(content, "span", "itemprop", "street-address");
            if (streetNode.size() > 0) {
                address[1] = streetNode.elementAt(0).toPlainTextString().trim();
            }
        }
        return address;
    }

    /**
     * <p class="expand-info tel">
     * <span class="info-name">电话：</span>
     * <span class="item" itemprop="tel">021-50490025</span>
     * <span class="item" itemprop="tel">021-50472193</span>
     * </p>
     *
     * @param content
     * @return the phone number of shop
     */
    public List<String> getPhoneNumbers(String content) {
        List<String> phoneList = null;
        NodeList phoneNodes = SimpleHtmlParser.parserHtml(content, "span", "itemprop", "tel");
        if (phoneNodes.size() > 0) {
            phoneList = new ArrayList<>();
            for (int i = 0; i < phoneNodes.size(); i++) {
                phoneList.add(phoneNodes.elementAt(i).toPlainTextString().trim());
            }
        }
        return phoneList;
    }


    /**
     * <p class="info info-indent">
     * <span class="info-name">分类标签：</span>
     * <span class="item">
     * <a href="/search/keyword/1/10_%E9%9A%8F%E4%BE%BF%E5%90%83%E5%90%83" rel="tag" target="_blank">随便吃吃</a>(9)
     * </span>
     * <span class="item">
     * <a href="/search/keyword/1/10_%E6%83%85%E4%BE%A3%E7%BA%A6%E4%BC%9A" rel="tag" target="_blank">情侣约会</a>(4)
     * </span>
     * <span class="item">
     * <a href="/search/keyword/1/10_%E4%BC%91%E9%97%B2%E5%B0%8F%E6%86%A9" rel="tag" target="_blank">休闲小憩</a>(3)
     * </span>
     * <span class="item">
     * <a href="/search/keyword/1/10_%E5%8F%AF%E4%BB%A5%E5%88%B7%E5%8D%A1" rel="tag" target="_blank">可以刷卡</a>(3)
     * </span>
     * <span class="item">
     * <a href="/search/keyword/1/10_%E6%97%A0%E7%BA%BF%E4%B8%8A%E7%BD%91" rel="tag" target="_blank">无线上网</a>(3)
     * </span>
     * </p>
     *
     * @param content
     * @return
     */
    public List<String> getTags(String content) {
        List<String> tags = null;
        NodeList tagNodes = SimpleHtmlParser.parserHtml(content, "a", "rel", "tag");
        if (tagNodes.size() > 0) {
            tags = new ArrayList<>();
            for (int i = 0; i < tagNodes.size(); i++) {
                tags.add(tagNodes.elementAt(i).toPlainTextString().trim());
            }
        }
        return tags;
    }

    /**
     * <p class="info info-indent">
     * <span class="info-name">营业时间：</span>
     * <span class="item">早10：00 - 晚10：00</span>
     * <a class="item-gray J-edit-time">修改</a>
     * </p>
     *
     * @param content
     * @return
     */
    public String getTime(String content) {
        String time = null;
        NodeList nodes = SimpleHtmlParser.parserHtml(content, "p", "class", "info info-indent");
        for (int i = 0; i < nodes.size(); i++) {
            String subContent = nodes.elementAt(i).toHtml();
            if (subContent.contains("营业时间：")) {
                NodeList timeNodes = SimpleHtmlParser.parserHtml(subContent, "span", "class", "item");
                if (timeNodes.size() > 0) {
                    time = timeNodes.elementAt(0).toPlainTextString().trim();
                }
            }
        }
        return time;
    }

    /**
     * <p class="info info-indent">
     * <span class="info-name">餐厅简介：</span>
     * 冰淇淋里的“奢侈品”。曾经一度“因为她的广告和口碑而心动”，现在光顾，多半是为了“视觉享受”，和某种“小资的优越感”。“
     * 没理由”的喜欢抹茶系列，其中又数单球“最为经典”，只是“不能一下子吃太多”，否则会感觉“超甜腻”。夏天不妨选雪芭，“酸酸的”，
     * “不腻口”。尽管分部众多，滨江店仍是外景最好的所在，非常适合“小情侣们去甜蜜一下”。
     * </p>
     *
     * @param content
     * @return
     */
    public String getIntro(String content) {
        String intro = null;
        NodeList nodes = SimpleHtmlParser.parserHtml(content, "p", "class", "info info-indent");
        for (int i = 0; i < nodes.size(); i++) {
            String subContent = nodes.elementAt(i).toPlainTextString();
            if (subContent.contains("餐厅简介：")) {
                intro = subContent.replace("餐厅简介：", "").trim();
            }
        }
        return intro;
    }

    /**
     * <div class="breadcrumb">
     * <a href="http://www.dianping.com/shanghai/food" itemprop="url">上海餐厅</a>&gt;
     * <a href="http://www.dianping.com/search/category/1/10/r5" itemprop="url">浦东新区</a>&gt;
     * <a href="http://www.dianping.com/search/category/1/10/r801" itemprop="url">陆家嘴</a>&gt;
     * <a href="http://www.dianping.com/search/category/1/10/g117r801" itemprop="url">面包甜点</a>&gt;
     * <a href="http://www.dianping.com/search/category/1/10/g241r801" itemprop="url">冰淇淋</a>&gt;
     * <span>哈根达斯(正大星美影院店)</span>
     * </div>
     *
     * @param content
     * @return
     */
    public List<String> getBreadcrumb(String content) {
        List<String> breadList = null;
        NodeList breadNodes = SimpleHtmlParser.parserHtml(content, "a", "itemprop", "url");
        if (breadNodes.size() > 0) {
            breadList = new ArrayList<>();
            for (int i = 0; i < breadNodes.size(); i++) {
                breadList.add(breadNodes.elementAt(i).toPlainTextString().trim());
            }
        }
        return breadList;
    }

    /**
     * <div class="brief-info">
     * <span title="准五星商户" class="mid-rank-stars mid-str45"></span>
     * <span class="item">6006条评论</span>
     * <div class="star-from-desc J-star-from-desc Hide">星级来自业内综合评估<i class="icon"></i></div>
     * <span class="item">人均：67元</span>|
     * <span class="item">口味：8.4</span>|
     * <span class="item">环境：8.3</span>|
     * <span class="item">服务：8.3</span>
     * <a class="icon score-btn J-score"></a>
     * </div>
     *
     * @param content
     * @return
     */
    public double[] getBrief(String content) {
        double[] brief = new double[4];
        brief[0] = -1.0d;
        brief[1] = -1.0d;
        brief[2] = -1.0d;
        brief[3] = -1.0d;

        NodeList breifNodes = SimpleHtmlParser.parserHtml(content, "div", "class", "brief-info");
        if (breifNodes.size() > 0) {
            breifNodes = SimpleHtmlParser.parserHtml(breifNodes.elementAt(0).toHtml(), "span", "class", "item");
            for (int i = 0; i < breifNodes.size(); i++) {
                String subContent = breifNodes.elementAt(i).toPlainTextString().trim();
                if (subContent.contains("人均：") && !subContent.contains("-")) {
                    brief[0] = Double.parseDouble(subContent.replace("人均：", "").replace("元", "").trim());
                } else if (subContent.contains("口味：") && !subContent.contains("-")) {
                    brief[1] = Double.parseDouble(subContent.replace("口味：", "").trim());
                } else if (subContent.contains("环境：") && !subContent.contains("-")) {
                    brief[2] = Double.parseDouble(subContent.replace("环境：", "").trim());
                } else if (subContent.contains("服务：") && !subContent.contains("-")) {
                    brief[3] = Double.parseDouble(subContent.replace("服务：", "").trim());
                }
            }
        }
        return brief;
    }

    /**
     * <p class="recommend-name">
     * <a class="item" href="/shop/3637864/dish-冰淇淋火锅" target="_blank" title="冰淇淋火锅">冰淇淋火锅<em class="count">(10)</em></a>
     * <a class="item" href="/shop/3637864/dish-冰淇淋" target="_blank" title="冰淇淋">冰淇淋<em class="count">(8)</em></a>
     * <a class="item" href="/shop/3637864/dish-朗姆酒" target="_blank" title="朗姆酒">朗姆酒<em class="count">(4)</em></a>
     * <a class="item" href="/shop/3637864/dish-可可" target="_blank" title="可可">可可<em class="count">(2)</em></a>
     * <a class="item" href="/shop/3637864/dish-水果茶" target="_blank" title="水果茶">水果茶<em class="count">(2)</em></a>
     * <a class="item" href="/shop/3637864/dish-爱琴海之舟" target="_blank" title="爱琴海之舟">爱琴海之舟<em class="count">(1)</em></a>
     * </p>
     *1.首先定位到 <div id="shop-tabs" class="mod">模块
     * 2.将<script type="text/panel" class="J-panels">和 </script>字符串替换掉
     * 3.提取<p class="recommend-name">模块
     * 4.提取<a class="item"  就是各个菜品的部分
     * 5.解析出菜名和菜被推荐的次数（这个被推荐次数是店内的，根据这个能得到店内的热门菜）
     * @param content
     * @return
     */
    public List<RecDish> getRecDishes(String content, String shopId) {
        List<RecDish> recDishes = null;
        NodeList recNodes = SimpleHtmlParser.parserHtml(content, "div", "class", "mod");
        if (recNodes.size() > 0) {
            String subContent = recNodes.elementAt(0).toHtml();
            //剔除掉一些字符串
            subContent = subContent.replace("<script type=\"text/panel\" class=\"J-panels\">", "").replace("</script>", "");
//            System.out.println(subContent);
            recNodes = SimpleHtmlParser.parserHtml(subContent, "p", "class", "recommend-name");
//            System.out.println("recnode size:" + recNodes.size());
            if (recNodes.size() > 0) {
                String dishContent = recNodes.elementAt(0).toHtml();
                recNodes = SimpleHtmlParser.parserHtml(dishContent, "a", "class", "item");
//                System.out.println("dish node size:" + recNodes.size());
                recDishes = new ArrayList<>();
                for (int i = 0; i < recNodes.size(); i++) {
                    RecDish recDish = new RecDish();
                    String dishInfo = recNodes.elementAt(i).toPlainTextString();
                    String[] splits = dishInfo.split("\\s+");
                    if(splits.length < 3) {
                        System.out.println(shopId + " dishInfo:"  + dishInfo);
                    }
                    if(splits.length >=3) {
                        String dish = splits[1];
                        String number = splits[2];
                         System.out.println(shopId +"dishname :" + dish);
                        System.out.println(shopId +"number:" + number);
                        recDish.setDishes(dish.replace("\\s+", ""));
                        try {
                            recDish.setNumber(Integer.parseInt(number.replace("(", "").replace(")", "").trim()));
                        }catch(NumberFormatException e)
                        {
                            //捕获异常并继续执行
                            System.out.println("NumberFormatException");
                        }
                        recDishes.add(recDish);
                    }
                }
            }
        }
        if(recDishes!= null) {
            if (recDishes.size() == 0) {
                recDishes = null;
            }
        }
        return recDishes;
    }

    /**
     * <ul class="stars">
     * <li><span class="mid-rank-stars mid-str50"></span><span class="progress-bar"><span style="width:89%"></span></span>82</li>
     * <li><span class="mid-rank-stars mid-str40"></span><span class="progress-bar"><span style="width:9%"></span></span>9</li>
     * <li><span class="mid-rank-stars mid-str30"></span><span class="progress-bar"><span style="width:0%"></span></span>0</li>
     * <li><span class="mid-rank-stars mid-str20"></span><span class="progress-bar"><span style="width:0%"></span></span>0</li>
     * <li><span class="mid-rank-stars mid-str10"></span><span class="progress-bar"><span style="width:1%"></span></span>1</li>
     * </ul>
     * <p>
     * <div class="brief-info">
     * <span title="准五星商户" class="mid-rank-stars mid-str45"></span>
     * <span class="item">6006条评论</span>
     * <div class="star-from-desc J-star-from-desc Hide">星级来自业内综合评估<i class="icon"></i></div>
     * <span class="item">人均：67元</span>|
     * <span class="item">口味：8.4</span>|
     * <span class="item">环境：8.3</span>|
     * <span class="item">服务：8.3</span>
     * <a class="icon score-btn J-score"></a>
     * </div>
     *
     * @param content
     * @return
     */
    public Star getStar(String content) throws ParserException {
        Star star = null;
        NodeList starNodes = SimpleHtmlParser.parserHtml(content, "ul", "class", "stars");
        if (starNodes.size() > 0) {
            starNodes = SimpleHtmlParser.parserHtml(starNodes.elementAt(0).toHtml(), "li");
            if (starNodes.size() > 0) {
                star = new Star();
                star.setStar5(Integer.parseInt(starNodes.elementAt(0).toPlainTextString().trim()));
                star.setStar4(Integer.parseInt(starNodes.elementAt(1).toPlainTextString().trim()));
                star.setStar3(Integer.parseInt(starNodes.elementAt(2).toPlainTextString().trim()));
                star.setStar2(Integer.parseInt(starNodes.elementAt(3).toPlainTextString().trim()));
                star.setStar1(Integer.parseInt(starNodes.elementAt(4).toPlainTextString().trim()));
            }
        }

        NodeList gradeNodes = SimpleHtmlParser.parserHtml(content, "div", "class", "brief-info");
        if (gradeNodes.size() > 0) {
            if (star == null) {
                star = new Star();
            }
            NodeList titleNodes = SimpleHtmlParser.parseNodeAttribtute(gradeNodes.elementAt(0).toHtml(), "title");
            if (titleNodes.size() > 0) {
                String subContent = titleNodes.elementAt(0).getText();
                star.setGrade(subContent.substring(12, subContent.indexOf("\" class=\"")).trim());
            }
        }
        return star;
    }

    /**
     * 得到精确的经纬度位置
     *
     * @param fileText
     * @return
     */
    public String[] getLngLat(String fileText) {
        String[] lngLat = new String[2];
        lngLat[0] = null;
        lngLat[1] = null;
        if (fileText.contains("({lng:") && fileText.contains(",lat:") && fileText.contains("});    </script>")) {
            lngLat = fileText.substring(fileText.indexOf("({lng:") + 6, fileText.indexOf("});    " +
                    "</script>")).replace(",lat:", "\t").trim().split("\t");
        }
        return lngLat;
    }
}
