import service.NormalStore;
import utils.RandomAccessFileUtil;

public class xx {
    public static void main(String[] args)
    {
        String filePath = "data/data_1.table";
        RandomAccessFileUtil.writeLogStart(filePath, 8);
        RandomAccessFileUtil.writeLogEnd(filePath, 8);
    }
}
