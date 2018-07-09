package main.parser;

import parser.FollowsParser;
import util.FileOperation;

import java.io.IOException;
import java.util.List;

/**
 * Created by motu on 2018/7/8.
 */
public class followtest {
    public static void main(String[] args) throws IOException {

        String path = "D:\\cbd\\！！毕业论文\\给贺小木的数据处理代码+数据\\userparser_test\\";
        List<String> filePaths = FileOperation.readAllFilePathCons(path, "txt");
        FollowsParser followsParser = new FollowsParser("D:\\cbd\\！！毕业论文\\给贺小木的数据处理代码+数据\\userparser_test\\followsInfo10C.txt", filePaths);
        followsParser.getAllFollowsInfo();
    }

}
