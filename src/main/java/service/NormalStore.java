/*
 *@Type NormalStore.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 02:07
 * @version
 */
package service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import common.BplusTree;
import model.command.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.CommandUtil;
import utils.LoggerUtil;
import utils.RandomAccessFileUtil;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Deque;
import java.util.ArrayDeque;

public class NormalStore implements Store {

    public static final String TABLE = ".table";
    public static final String DATA = ".data";
    public static final String RW_MODE = "rw";
    public static final String NAME = "data_1";
    private final Logger LOGGER = LoggerFactory.getLogger(NormalStore.class);
    private final String logFormat = "[NormalStore][{}]: {}";
    private static final ExecutorService compressionExecutor = Executors.newSingleThreadExecutor();


    /**
     * 读写表
     */
    private TreeMap<String, Command> readWriteTable;

    /**
     * 只读表
     */
    private TreeMap<String, Command> readOnlyTable;

    /**
     * B+树索引，存的是数据长度和偏移量
     */
    private BplusTree index;

    /**
     * 数据目录
     */
    private final String dataDir;

    /**
     * 读写锁，支持多线程，并发安全写入
     */
    private final ReadWriteLock indexLock;

    /**
     * 暂存数据的日志句柄
     */
    private RandomAccessFile writerReader;

    /**
     * 持久化阈值
     */
    private final int storeThreshold;

    /**
     * 数据文件最大大小
     */
    private static final int MAX_DATA_FILE_SIZE = 60 * 1024; // 100kB

    /**
     * 日志文件最大大小
     */
    private static final int MAX_LOG_FILE_SIZE = 5 * 1024; // 5kB

    /**
     * 在压缩过程中阻塞get操作
     */
    private final Object getLock = new Object();

    private final Object reIndexLock = new Object();

    /**
     * data目录下所有.data后缀的文件队列
     */
    private Deque<String> dataFiles;

    public NormalStore(String dataDir, Integer storeThreshold) throws IOException, ClassNotFoundException {
        this.dataDir = dataDir;
        this.indexLock = new ReentrantReadWriteLock();
        this.readWriteTable = new TreeMap<String, Command>();
        this.index = new BplusTree();
        this.storeThreshold = storeThreshold;
        this.dataFiles = this.getDataFiles(dataDir);

        File file = new File(dataDir);
        if (!file.exists()) {
            LoggerUtil.info(LOGGER, logFormat, "NormalStore", "dataDir isn't exist,creating...");
            file.mkdirs();
        }
        this.replayLog(); //恢复日志
        this.checkConsistency(); //检查一致性
        //this.reloadIndex(); // 恢复索引 TODO: 索引持久化后还需要恢复吗
        this.startMonitoring(); // 开启定时压缩
    }

    public String getWalFilePath() {
        return this.dataDir + File.separator + NAME + TABLE;
    }

    public String getDiskFilePath() {
        return this.dataDir + File.separator + NAME + DATA;
    }

    public String getOldFilePath() {
        String peek = dataFiles.peek();
        Pattern pattern = Pattern.compile("data_(\\d+)\\.data");
        Matcher matcher = pattern.matcher(peek);
        int number = 0;
        if (matcher.find()) {
            number = Integer.parseInt(matcher.group(1)) + 1;
        }
        return this.dataDir + File.separator + "data_" + number + DATA;
    }


    public void reloadIndex() {
        synchronized (reIndexLock){
            try {
                index.clear();

                Deque<String> dataFiles_temp = new ArrayDeque<>(this.dataFiles);
                while (!dataFiles_temp.isEmpty()) {
                    String pollFile = dataFiles_temp.poll();
                    try (RandomAccessFile file = new RandomAccessFile(pollFile, RW_MODE)) {
                        long len = file.length();
                        long start = 0;
                        file.seek(start);
                        while (start < len) {
                            int cmdLen = file.readInt();
                            byte[] bytes = new byte[cmdLen];
                            file.read(bytes);
                            JSONObject value = JSON.parseObject(new String(bytes, StandardCharsets.UTF_8)); //反序列
                            Command command = CommandUtil.jsonToCommand(value);
                            start += Integer.BYTES;
                            if (command != null) {
                                CommandPos cmdPos = new CommandPos((int) start, cmdLen, pollFile);
                                index.insertOrUpdate(command.getKey(), cmdPos);
                            }
                            start += cmdLen;
                        }
                        file.seek(file.length());
//                    LoggerUtil.debug(LOGGER, logFormat, "reload index: " + index.toString()); TODO:
                    }//随机读写文件
                    index.save(index);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void set(String key, String value) {
        try {
            SetCommand command = new SetCommand(key, value);
            byte[] commandBytes = JSONObject.toJSONBytes(command);
            // 加锁
            indexLock.writeLock().lock();
            // 先写内存表，内存表达到一定阀值再写进磁盘
            readWriteTable.put(key, command);
            if (readWriteTable.size() >= storeThreshold) {
                flushMemTableToDisk();
            }
            // 写table（wal）文件
            int startPoint = RandomAccessFileUtil.writeInt(this.getWalFilePath(), commandBytes.length);
            int endPoint = RandomAccessFileUtil.write(this.getWalFilePath(), commandBytes);
            if (endPoint > MAX_LOG_FILE_SIZE) {
                RandomAccessFileUtil.writeLogEnd(this.getWalFilePath(), Integer.BYTES * 2);
                RandomAccessFileUtil.truncate(this.getWalFilePath(), endPoint);
            } else {
                RandomAccessFileUtil.writeLogEnd(this.getWalFilePath(), endPoint);
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    @Override
    public String get(String key) {
        synchronized (getLock) {
            try {
                // 先从只读表中获取信息
                if (readOnlyTable.containsKey(key)) {
                    Command cmd = readOnlyTable.get(key);
                    if (cmd instanceof SetCommand) {
                        return ((SetCommand) cmd).getValue();
                    }
                    if (cmd instanceof RmCommand) {
                        return null;
                    }
                }

                // 再尝试从读写表中获取信息
                if (readWriteTable.containsKey(key)) {
                    Command cmd = readWriteTable.get(key);
                    if (cmd instanceof SetCommand) {
                        return ((SetCommand) cmd).getValue();
                    }
                    if (cmd instanceof RmCommand) {
                        return null;
                    }
                }

                // 从索引中获取信息
                CommandPos cmdPos = null;
                synchronized (getLock) { // 等待压缩后的索引重置
                    cmdPos = (CommandPos) index.get(key);
                }
                if (cmdPos == null) {
                    return null;
                }

                byte[] commandBytes = RandomAccessFileUtil.readByIndex(cmdPos.getFilePath(), cmdPos.getPos(), cmdPos.getLen());

                JSONObject value = JSONObject.parseObject(new String(commandBytes));
                Command cmd = CommandUtil.jsonToCommand(value);
                if (cmd instanceof SetCommand) {
                    return ((SetCommand) cmd).getValue();
                }
                if (cmd instanceof RmCommand) {
                    return null;
                }
            } catch (Throwable t) {
                throw new RuntimeException(t);
            }
        }
        return null;
    }

    @Override
    public void rm(String key) {
        try {
            RmCommand command = new RmCommand(key);
            byte[] commandBytes = JSONObject.toJSONBytes(command);
            // 先写内存表，内存表达到一定阀值再写进磁盘
            readWriteTable.put(key, command);
            if (readWriteTable.size() >= storeThreshold) {
                flushMemTableToDisk();
            }
            // 写table（wal）文件
            int startPoint = RandomAccessFileUtil.writeInt(this.getWalFilePath(), commandBytes.length);
            int endPoint = RandomAccessFileUtil.write(this.getWalFilePath(), commandBytes);
            if (endPoint > MAX_LOG_FILE_SIZE) {
                RandomAccessFileUtil.writeLogEnd(this.getWalFilePath(), Integer.BYTES * 2);
                RandomAccessFileUtil.truncate(this.getWalFilePath(), endPoint);
            } else {
                RandomAccessFileUtil.writeLogEnd(this.getWalFilePath(), endPoint);
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            indexLock.writeLock().lock();
            // 将内存表中的数据写回磁盘
            flushMemTableToDisk();
        } finally {
            indexLock.writeLock().unlock();
            if (writerReader != null) {
                writerReader.close();
            }
        }
    }

    private void flushMemTableToDisk() {
        transferToReadOnly();
        try {
            // 打开一个磁盘文件用于写入
            try (RandomAccessFile file = new RandomAccessFile(this.getDiskFilePath(), RW_MODE)) {
                for (Map.Entry<String, Command> entry : readOnlyTable.entrySet()) {
                    String key = entry.getKey();
                    Command command = entry.getValue();
                    byte[] commandBytes = JSONObject.toJSONBytes(command);

                    int startPoint = RandomAccessFileUtil.writeInt(this.getDiskFilePath(), commandBytes.length);
                    RandomAccessFileUtil.write(this.getDiskFilePath(), commandBytes);
                    CommandPos cmdPos = new CommandPos(startPoint, commandBytes.length, this.getDiskFilePath());

                    synchronized (reIndexLock){
                        index.insertOrUpdate(key, cmdPos);
                    }
                }
                index.save(index);
                if (file.length() > MAX_DATA_FILE_SIZE) {
                    file.close();
                    rotateDataFile();
                }
            }
            // 清空WAL文件(伪清空)
            clearWAL();
        } catch (IOException e) {
            throw new RuntimeException("Failed to flush memTable to disk", e);
        }
    }

    private void replayLog() { //redolog
        try {
            int logStart = RandomAccessFileUtil.getLogStart(this.getWalFilePath());
            int logEnd = RandomAccessFileUtil.getLogEnd(this.getWalFilePath());
            boolean isJump = logEnd < logStart;

            RandomAccessFile logFile = new RandomAccessFile(this.getWalFilePath(), "r");
            logFile.seek(logStart);
            int len = (int) new File(this.getWalFilePath()).length();
            while (logFile.getFilePointer() != logEnd) {
                int length = logFile.readInt();
                byte[] commandBytes = new byte[length];
                logFile.readFully(commandBytes);

                Command command = JSONObject.parseObject(commandBytes, Command.class);
                if (command instanceof SetCommand) {
                    readWriteTable.put(command.getKey(), command);
                } else if (command instanceof RmCommand) {
                    readWriteTable.remove(command.getKey());
                }

                if (isJump && logFile.getFilePointer() == len) {
                    logFile.seek(Integer.BYTES * 2);
                    isJump = false;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void checkConsistency() {
        // 遍历内存表，检查每个键的索引是否一致
        for (Map.Entry<String, Command> entry : readWriteTable.entrySet()) {
            String key = entry.getKey();
            Command command = entry.getValue();
            CommandPos cmdPos = (CommandPos) index.get(key);

            // 如果索引不存在或不一致，将内存表中的数据写回磁盘
            if (cmdPos == null || !isCommandInDisk(command, cmdPos)) {
                flushMemTableToDisk();
                return;
            }
        }
    }

    private boolean isCommandInDisk(Command command, CommandPos cmdPos) {
        // 检查磁盘上是否存在相应的命令，并且内容一致
        try {
            RandomAccessFile file = new RandomAccessFile(cmdPos.getFilePath(), "r");
            file.seek(cmdPos.getPos());
            int length = file.readInt();
            byte[] commandBytes = new byte[length];
            file.readFully(commandBytes);

            Command diskCommand = JSONObject.parseObject(commandBytes, Command.class);
            return command.equals(diskCommand);

        } catch (IOException e) {
            return false;
        }
    }

    private void clearWAL() {
//        try {
//            // 打开 WAL 文件以便写入模式
//            RandomAccessFile raf = new RandomAccessFile(new File(this.getWalFilePath()), RW_MODE);
//            // 将文件长度截断为 0，即清空文件内容
//            raf.setLength(0);
//            // 关闭文件
//            raf.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//            throw new RuntimeException("Clear WAL error：" + e.getMessage());
//        }
        int logEnd = RandomAccessFileUtil.getLogEnd(this.getWalFilePath());
        RandomAccessFileUtil.writeLogStart(this.getWalFilePath(), logEnd);
    }

    private void rotateDataFile() throws IOException {
        compressionExecutor.submit(() -> {
            synchronized (getLock) {
                try {
                    LoggerUtil.debug(LOGGER, logFormat, "The disk reaches a threshold, Compressing...");

                    String oldFilePath = this.getOldFilePath();
                    File oldFile = new File(oldFilePath);
                    Files.move(Paths.get(this.getDiskFilePath()), oldFile.toPath());
                    dataFiles.addFirst(oldFilePath);
                    File dataFile = new File(this.getDiskFilePath().replace("\\", "\\\\"));
                    dataFile.createNewFile();

                    compressFile(oldFile);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public void compressFile(File fileToCompress) throws IOException {
        try {
            TreeMap<String, Command> latestEntries;
            try (RandomAccessFile file = new RandomAccessFile(fileToCompress, RW_MODE)) { // 压缩操作
                latestEntries = new TreeMap<>();
                long len = file.length();
                long start = 0;
                file.seek(start);
                while (start < len) {
                    int cmdLen = file.readInt();
                    byte[] bytes = new byte[cmdLen];
                    file.read(bytes);
                    JSONObject value = JSON.parseObject(new String(bytes, StandardCharsets.UTF_8)); //反序列
                    Command command = CommandUtil.jsonToCommand(value);
                    start += Integer.BYTES;
                    if (command instanceof SetCommand) {
                        latestEntries.put(command.getKey(), command);
                    } else if (command instanceof RmCommand) {
                        latestEntries.remove(command.getKey());
                    }
                    start += cmdLen;
                }
                file.seek(file.length());
            }
            String tempFilePath = dataDir + File.separator + "data" + DATA + ".temp";
            File tempFile = new File(tempFilePath);
            for (Map.Entry<String, Command> entry : latestEntries.entrySet()) {
                try (RandomAccessFile file2 = new RandomAccessFile(tempFile, RW_MODE)) {
                    Command command = entry.getValue();
                    byte[] commandBytes = JSONObject.toJSONBytes(command);

                    RandomAccessFileUtil.writeInt(tempFilePath, commandBytes.length);
                    RandomAccessFileUtil.write(tempFilePath, commandBytes);
                }
            }
            // 压缩操作完成->tempFile

            Files.move(tempFile.toPath(), fileToCompress.toPath(), StandardCopyOption.REPLACE_EXISTING); // 将tempFile重命名oldFile

//            Thread.sleep(20000); // 模拟压缩时间

//            indexLock.writeLock().lock();
//            appendFiles(this.getOldFilePath(), this.getDiskFilePath(), this.getDiskFilePath());
//            fileToCompress.delete();
//            indexLock.writeLock().unlock();
            reloadIndex();

            LoggerUtil.debug(LOGGER, logFormat, "Compression is complete");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void appendFiles(String newfile, String oldfile) throws IOException { // 创建时间新旧
        String tempFile1 = "temp.data";
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile1))) {
            // 读取并写入更旧文件的数据
            try (BufferedReader reader = new BufferedReader(new FileReader(oldfile))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    writer.write(line);
                }
            }
            // 读取并写入更新文件的数据
            try (BufferedReader reader = new BufferedReader(new FileReader(newfile))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    writer.write(line);
                }
            }
        }
        // 将临时文件重命名为目标文件
        File oldFile = new File(oldfile);
        File newFile = new File(newfile);
        File tempFile = new File(tempFile1);
        newFile.delete(); // 删除旧的data.data文件
        oldFile.delete();
        tempFile.renameTo(oldFile);
    }

    private Deque<String> getDataFiles(String directoryPath) {
        Deque<String> dataFilesList = new ArrayDeque<>();
        Path dirPath = Paths.get(directoryPath);

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dirPath, "*.data")) {
            for (Path entry : stream) {
                if (Files.isRegularFile(entry)) {
                    // 直接使用Path对象的toString方法，然后替换反斜杠
                    String filePath = entry.toString().replace("\\", "\\\\");
                    dataFilesList.addFirst(filePath); // 使用 addFirst 将文件路径字符串添加到队列前端
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return dataFilesList;
    }

    // 转移数据到只读表
    private void transferToReadOnly() {
        try {
            indexLock.writeLock().lock();
            TreeMap<String, Command> temp = readWriteTable;
            readWriteTable = new TreeMap<String, Command>();
            readOnlyTable = temp;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    public void monitorAndMerge() {
        synchronized (getLock) {
            try {
                // 持续监测队列，直到程序结束
                while (dataFiles.size() >= 3) {
                    // 执行合并操作
                    appendFiles(dataFiles.pollFirst(),dataFiles.peekFirst());
                    this.compressFile(new File(dataFiles.peekFirst()));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void startMonitoring() {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(this::monitorAndMerge, 0, 10, TimeUnit.SECONDS);
    }
}
