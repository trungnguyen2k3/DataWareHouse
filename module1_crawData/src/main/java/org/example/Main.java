package org.example;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Main {
    public static String fileName;
    public static String sourceFile;
    public static String destFile;
    public static String stagingHost;
    public static String stagingDatabase;
    public static String dwHost;
    public static String createdBy;

    // Thêm các thuộc tính cho username, password, và port
    public static String dwSourceUsername;
    public static String dwSourcePassword;
    public static String stagingSourceUsername;
    public static String stagingSourcePassword;

    public static String stagingSourcePort;
    public static String dwSourcePort;

    public static void main(String[] args) {
        // Đường dẫn file config
        String configFilePath = "D:\\warehouse_final\\module1_crawData\\src\\main\\java\\config.properties";

        // Đọc file config và thiết lập giá trị cho các thuộc tính
        Properties properties = readConfig(configFilePath);
        setProperties(properties);

        // In thông tin ra console
//        printConfigInfo();
    }

    // Phương thức đọc file config
    public static Properties readConfig(String filePath) {
        Properties properties = new Properties();
        try (InputStream input = new FileInputStream(filePath)) {
            // Load thông tin từ file config
            properties.load(input);
        } catch (IOException e) {
            System.err.println("Lỗi khi đọc file config: " + e.getMessage());
        }
        return properties;
    }

    // Phương thức thiết lập các thuộc tính từ file config
    public static void setProperties(Properties properties) {
        fileName = properties.getProperty("file_name");
        sourceFile = properties.getProperty("source_file");
        destFile = properties.getProperty("dest_file");
        stagingHost = properties.getProperty("STAGING_source_host");
        stagingDatabase = properties.getProperty("STAGING_database_name");
        dwHost = properties.getProperty("DW_source_host");
        createdBy = properties.getProperty("create_by");
        // Thiết lập các username và password
        dwSourceUsername = properties.getProperty("DW_source_username");
        dwSourcePassword = properties.getProperty("DW_source_password");
        stagingSourceUsername = properties.getProperty("STAGING_source_username");
        stagingSourcePassword = properties.getProperty("STAGING_source_password");

        // Thiết lập các port cho STAGING và DW
        stagingSourcePort = properties.getProperty("STAGING_source_port");
        dwSourcePort = properties.getProperty("DW_source_port");
    }

    public static void printConfigInfo() {
        System.out.println("File Name: " + fileName);
        System.out.println("Source File: " + sourceFile);
        System.out.println("Destination File: " + destFile);
        System.out.println("STAGING Host: " + stagingHost);
        System.out.println("STAGING Database: " + stagingDatabase);
        System.out.println("DW Host: " + dwHost);
        System.out.println("Created By: " + createdBy);

        // In thông tin về username và password
        System.out.println("DW Source Username: " + dwSourceUsername);
        System.out.println("DW Source Password: " + dwSourcePassword);
        System.out.println("STAGING Source Username: " + stagingSourceUsername);
        System.out.println("STAGING Source Password: " + stagingSourcePassword);

        // In thông tin về port
        System.out.println("STAGING Source Port: " + stagingSourcePort);
        System.out.println("DW Source Port: " + dwSourcePort);
    }
}