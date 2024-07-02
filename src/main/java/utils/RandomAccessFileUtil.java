/*
 *@Type RandomAccessFileUtil.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 02:58
 * @version
 */
package utils;

import service.NormalStore;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;

public class RandomAccessFileUtil {
    private static final String RW_MODE = "rw";
    private static final String logFilePath = "data" + File.separator + File.separator + "data_1" + ".table";

    public static int write(String filePath, byte[] value) throws IOException {
        RandomAccessFile file = null;
        int endPos = 0;
        try {
            file = new RandomAccessFile(filePath, RW_MODE);
            long writeStart = -1L;
            if (!filePath.equals(logFilePath)) {
                writeStart = file.length();
            } else {
                writeStart = getLogEnd(filePath) + Integer.BYTES;
            }
            file.seek(writeStart);
            file.write(value);
            endPos = (int) file.getFilePointer();
            file.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return endPos;
    }

    public static int writeInt(String filePath, int value) {
        RandomAccessFile file = null;
        long writeStart = -1L;
        try {
            file = new RandomAccessFile(filePath, RW_MODE);
            if (!filePath.equals(logFilePath)) {
                writeStart = file.length();
            } else {
                writeStart = getLogEnd(filePath);
            }
            file.seek(writeStart);
            file.writeInt(value);
            file.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return (int) writeStart;
    }

    public static byte[] readByIndex(String filePath, int index, int len) {
        RandomAccessFile file = null;
        byte[] res = new byte[len];
        try {
            file = new RandomAccessFile(filePath, RW_MODE);
            file.seek((long) index);
            file.read(res, 0, len);
            return res;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                file.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public static void writeLogStart(String filePath, int value) {
        RandomAccessFile file = null;
        try {
            file = new RandomAccessFile(filePath, RW_MODE);
            file.writeInt(value);
            file.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void writeLogEnd(String filePath, int value) {
        RandomAccessFile file = null;
        try {
            file = new RandomAccessFile(filePath, RW_MODE);
            file.seek(Integer.BYTES);
            file.writeInt(value);
            file.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static int getLogStart(String filePath) {
        RandomAccessFile file = null;
        int start = 0;
        try {
            file = new RandomAccessFile(filePath, RW_MODE);
            start = file.readInt();
            file.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return start;
    }

    public static int getLogEnd(String filePath) {
        RandomAccessFile file = null;
        int end = 0;
        try {
            file = new RandomAccessFile(filePath, RW_MODE);
            file.seek(Integer.BYTES);
            end = file.readInt();
            file.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return end;
    }

    public static void truncate(String filePath, int len) {
        RandomAccessFile file = null;
        try {
            file = new RandomAccessFile(filePath, RW_MODE);
            file.setLength(len);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
