package utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

public class PropertyReaderUtil {
    private static volatile PropertyReaderUtil instance;
    private Properties properties = new Properties();

    private PropertyReaderUtil() {
        this.loadConfig();
    }

    public static PropertyReaderUtil getInstance() {
        if (instance == null) {
            synchronized (PropertyReaderUtil.class) {
                if (instance == null) {
                    instance = new PropertyReaderUtil();
                }
            }
        }
        return instance;
    }

    public String getProperty(String name) {
        return properties.getProperty(name);
    }

    private void loadConfig() {
        InputStream inputStream = null;
        try {
            inputStream = PropertyReaderUtil.class.getClassLoader().getResourceAsStream("dataBase.properties");
            if (inputStream != null) {
                properties.load(new InputStreamReader(inputStream, "UTF-8"));
            } else {
                System.out.println("Property file 'dataBase.properties' not found in the classpath");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}