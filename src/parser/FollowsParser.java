package parser;

import org.htmlparser.util.NodeList;
import util.FileOperation;
import util.SimpleHtmlParser;

import java.util.Date;
import java.util.List;

/**
 * Created by wangkeqiang on 2016/11/26.
 */
public class FollowsParser {
    final static String userIdStartStr = "\" user-id=\"";
    final static String userIdEndStr = "\" class=\"J_card\"";
    final static int userIdLength = userIdStartStr.length();

    List<String> filePathList;
    String storeFilePath;
    StringBuilder allUserFollowsStr;

    public FollowsParser() {

    }

    public FollowsParser(String storeFilePath, List<String> filePathList) {
        this.storeFilePath = storeFilePath;
        this.filePathList = filePathList;
        this.allUserFollowsStr = new StringBuilder("");
    }

    public void getAllFollowsInfo() {
        int count = 0;
        for (String filePath : filePathList) {
            String userId = filePath.substring(filePath.lastIndexOf("\\") + 1).replace(".txt", "");
            allUserFollowsStr.append(userId);
            allUserFollowsStr.append(":");
            allUserFollowsStr.append(this.getFollowIds(FileOperation.read(filePath)));
            allUserFollowsStr.append("\n");
            if (count++ % 1000 == 0) {
                FileOperation.writeAppdend(storeFilePath, allUserFollowsStr.toString());
                allUserFollowsStr = new StringBuilder("");
                System.out.println("Parser "+count+" follow relationships! "+ new Date());
            }
        }
        if(count % 1000!=0){
            FileOperation.writeAppdend(storeFilePath, allUserFollowsStr.toString());
        }
    }

    /**
     * <div class="tit">
     * <h6><a title="温柔三刀_1496" onclick="pageTracker._trackPageview('dp_following_user');" user-id="615400300" class="J_card" href="/member/615400300">温柔三刀_1496</a></h6>
     * </div>
     *
     * @param content
     * @return follow ids
     */
    public StringBuilder getFollowIds(String content) {
        StringBuilder followsIdsStr = new StringBuilder("");
        NodeList idNodes = SimpleHtmlParser.parserHtml(content, "div", "class", "tit");
        String id;
        if (idNodes.size() > 0) {
            for (int nodesIndex = 0; nodesIndex < idNodes.size(); ++nodesIndex) {
                String idsStr = idNodes.elementAt(nodesIndex).toHtml();
                id = idsStr.substring(idsStr.indexOf(userIdStartStr) + userIdLength, idsStr.indexOf(userIdEndStr));
                followsIdsStr.append(" ");
                followsIdsStr.append(id);
            }
        }
        return followsIdsStr;
    }

    public static void main(String[] args) {
        String content = FileOperation.read("F:\\git\\wkq\\workspace_git\\DianPing\\data\\follows\\6078369.txt");
        FollowsParser followsParser = new FollowsParser();
        System.out.println(followsParser.getFollowIds(content).toString().trim());
    }
}
