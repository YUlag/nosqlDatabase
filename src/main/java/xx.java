import service.NormalStore;
import utils.RandomAccessFileUtil;

import java.io.File;

public class xx {
    public static void main(String[] args)
    {
        String filePath = "data\\\\data_1.txt";
        File file = new File(filePath);
        System.out.println(file.length());
        RandomAccessFileUtil.readByIndex(filePath, 6000, 28); //54748
    }
}
