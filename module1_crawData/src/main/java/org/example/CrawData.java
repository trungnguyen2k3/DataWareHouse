package org.example;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class CrawData {
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
    public static String dwDatabaseName;

    public static void main(String[] args) {
        String configFilePath = "D:\\warehouse_final\\module1_crawData\\src\\main\\java\\config.properties";
// 1.lấy dữ liệu từ config.properties
        Properties properties = readConfig(configFilePath);
        setProperties(properties);
        String location = destFile + "\\" + fileName + "_" + LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")) + ".csv";
        //2. Thiết lập kết nối connection tới cơ sở dữ liệu
        Connection connection = connect();
        //3.Kiểm tra status
        boolean status = checkStatus();
        System.out.println("Trạng thái: " + status);
        if (checkStatus() == false) {
            //4. Chèn dữ liệu vào config
            insertConfig();
            //5.Tiền hành craw dữ liệu
            boolean is_craw = crawData();
            // 6. Kiểm tra craw thành công chưa
            if (is_craw) {
                // 6.1. Ghi log với status = SUCCESS và cập nhật status của config =  SUCCESS
                insertLog(location, 1, "SUCCESS");
                setStatusConfig("SUCCESS");

            } else {
                //6.2. Ghi log với status= ERROR
                insertLog(location, 1, "ERROR");
            }
        }else{
            //3.1. Ghi log với status = ERROR
            insertLog(location, 1, "ERROR");
        }
    }

    //1. Đọc file config
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

    // 1.1.Thiết lập các thuộc tính từ file config
    public static void setProperties(Properties properties) {
        fileName = properties.getProperty("file_name");
        sourceFile = properties.getProperty("source_file");
        destFile = properties.getProperty("dest_file");
        stagingHost = properties.getProperty("STAGING_source_host");
        stagingDatabase = properties.getProperty("STAGING_database_name");
        dwHost = properties.getProperty("DW_source_host");
        createdBy = properties.getProperty("create_by");
        dwDatabaseName = properties.getProperty("DW_database_name");
        // Thiết lập các username và password
        dwSourceUsername = properties.getProperty("DW_source_username");
        dwSourcePassword = properties.getProperty("DW_source_password");
        stagingSourceUsername = properties.getProperty("STAGING_source_username");
        stagingSourcePassword = properties.getProperty("STAGING_source_password");

        // Thiết lập các port cho STAGING và DW
        stagingSourcePort = properties.getProperty("STAGING_source_port");
        dwSourcePort = properties.getProperty("DW_source_port");
    }

    //2. Kết nối tới database
    public static Connection connect() {
        String url_database = "jdbc:mysql://" + stagingHost + ":" + stagingSourcePort + "/" + stagingDatabase;
        Connection connection = null;
        try {
            // Tải driver của MySQL (từ phiên bản JDBC 4.0 trở lên, không cần gọi Class.forName())
            connection = DriverManager.getConnection(url_database, stagingSourceUsername, stagingSourcePassword);
            System.out.println("Kết nối tới MySQL thành công!");
        } catch (SQLException e) {
            System.err.println("Lỗi khi kết nối tới MySQL: " + e.getMessage());
        }
        return connection;
    }
//3. Kiểm tra status
    public static boolean checkStatus() {
        try (Connection connection = connect()) {
            if (connection != null) {
                // Câu lệnh gọi thủ tục lưu trữ
                String procedureCall = "{call checkStatusProcessingOrSuccess()}";

                // Thực thi thủ tục
                try (CallableStatement statement = connection.prepareCall(procedureCall)) {
                    // Thực thi thủ tục và nhận kết quả
                    ResultSet resultSet = statement.executeQuery();

                    // Kiểm tra kết quả trả về từ thủ tục
                    if (resultSet.next()) {
                        // Lấy giá trị boolean trả về từ thủ tục
                        return resultSet.getBoolean("result");
                    }
                } catch (SQLException e) {
                    System.err.println("Lỗi khi gọi thủ tục: " + e.getMessage());
                }
            }
        } catch (SQLException e) {
            System.err.println("Lỗi kết nối tới cơ sở dữ liệu: " + e.getMessage());
        }

        // Nếu không có kết quả, trả về false
        return false;
    }

    //4. Chèn dữ liệu vào table config
// 4. Chèn dữ liệu vào bảng config
    public static void insertConfig() {
        // Kết nối tới cơ sở dữ liệu MySQL
        try (Connection connection = connect()) {
            if (connection != null) {
                // Câu lệnh gọi thủ tục insertConfig
                String sql = "{CALL insertConfig(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)}"; // 16 tham số

                try (CallableStatement statement = connection.prepareCall(sql)) {
                    // Đặt giá trị cho các tham số đầu vào của thủ tục
                    statement.setString(1, fileName);
                    statement.setString(2, sourceFile);
                    statement.setString(3, destFile);
                    statement.setString(4, stagingSourceUsername);
                    statement.setString(5, stagingSourcePassword);
                    statement.setString(6, stagingHost);
                    statement.setString(7, stagingSourcePort);
                    statement.setString(8, stagingDatabase);
                    statement.setString(9, dwSourceUsername);
                    statement.setString(10, dwDatabaseName);
                    statement.setString(11, dwHost);
                    statement.setString(12, dwSourcePort);
                    statement.setString(13, dwHost);
                    statement.setString(14, "PROCESSING");  // status
                    statement.setString(15, createdBy);
                    statement.setDate(16, java.sql.Date.valueOf(LocalDate.now()));  // create_date

                    // Thực thi thủ tục
                    statement.executeUpdate();
                    System.out.println("Config đã được chèn thành công!");
                } catch (SQLException e) {
                    System.err.println("Lỗi khi gọi thủ tục: " + e.getMessage());
                }
            }
        } catch (SQLException e) {
            System.err.println("Lỗi kết nối tới cơ sở dữ liệu: " + e.getMessage());
        }
    }
//5. Thực hiện craw dữ liệu
    static boolean crawData() {
        String currentDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        String csvFilePath = destFile + "\\" + fileName + "_" + currentDate + ".csv";
        fileName = "\\cinestar_moviesdata_" + currentDate + ".csv";
        int currentPage = 1;
        boolean hasNextPage = true;
        try {
            // Tiến hành tải dữ liệu với phân trang
            while (hasNextPage) {
                // Fetch JSON data from API
                JSONObject jsonData = fetchJsonFromApi(sourceFile + "?page=" + currentPage);

                // Extract movie list
                JSONArray moviesArray = jsonData
                        .getJSONObject("pageProps")
                        .getJSONObject("res")
                        .getJSONArray("listMovie");

                // Write data to CSV immediately after fetching
                writeDataToCsv(moviesArray, csvFilePath);

                // Kiểm tra nếu có trang tiếp theo
                hasNextPage = jsonData.getJSONObject("pageProps").getJSONObject("res").optBoolean("hasNextPage", false);
                System.out.println("Page " + currentPage + " processed...");
                currentPage++;
            }
            System.out.println("Data successfully saved to " + csvFilePath);
            return true;  // Return true if data crawled successfully

        } catch (IOException e) {
            System.err.println("Error occurred: " + e.getMessage());
            return false;  // Return false if an error occurred
        }
    }
//5.1 lưu file dữ liệu vào csv
    private static void writeDataToCsv(JSONArray moviesArray, String csvFilePath) throws IOException {
        File csvFile = new File(csvFilePath);
        // Ensure the parent directory exists
        if (!csvFile.getParentFile().exists()) {
            csvFile.getParentFile().mkdirs();
        }

        // Check if file exists or create a new one
        boolean fileAlreadyExists = csvFile.exists();

        // Open FileWriter and CSVPrinter in append mode with UTF-8 encoding
        try (FileOutputStream fos = new FileOutputStream(csvFile, true);
             OutputStreamWriter writer = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
             CSVPrinter csvPrinter = new CSVPrinter(writer,
                     fileAlreadyExists ? CSVFormat.DEFAULT : CSVFormat.DEFAULT.withHeader(
                             "Vietnamese Name", "Director", "Actors",
                             "Country (VN)", "Format (VN)", "Genre (VN)", "Release Date", "End Date",
                             "Description (VN)", "Image URL", "Duration", "Age Limit (VN)"
                     ))) {
            // Loop through the movie array and write each record
            for (int i = 0; i < moviesArray.length(); i++) {
                JSONObject movie = moviesArray.getJSONObject(i);

                csvPrinter.printRecord(
                        movie.optString("name_vn"),
                        movie.optString("director"),
                        movie.optString("actor"),
                        movie.optString("country_name_vn"),
                        movie.optString("formats_name_vn"),
                        movie.optString("type_name_vn"),
                        movie.optString("release_date"),
                        movie.optString("end_date"),
                        movie.optString("brief_vn"),
                        movie.optString("image"),
                        movie.optString("time"),
                        movie.optString("limitage_vn")
                );
            }
        }
    }
// 5.1. Lấy dữ liệu từ api
    private static JSONObject fetchJsonFromApi(String apiUrl) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) new URL(apiUrl).openConnection();
        connection.setRequestMethod("GET");
        // Kiểm tra mã trạng thái HTTP
        if (connection.getResponseCode() != 200) {
            throw new IOException("Failed to get response from API: " + connection.getResponseCode());
        }

        try (InputStream inputStream = connection.getInputStream();
             InputStreamReader reader = new InputStreamReader(inputStream)) {
            StringBuilder data = new StringBuilder();
            int read;
            char[] buffer = new char[1024];
            while ((read = reader.read(buffer)) != -1) {
                data.append(buffer, 0, read);
            }
            return new JSONObject(data.toString());
        }
    }

    // 6.2 Cập nhật  status trong config
    public static void setStatusConfig(String status) {
        String sql = "{CALL setStatusConfig(?)}"; // Câu lệnh gọi thủ tục
        try (Connection connection = connect();
             CallableStatement callableStatement = connection.prepareCall(sql)) {

            // Thiết lập tham số đầu vào cho thủ tục
            callableStatement.setString(1, status);

            // Thực thi thủ tục
            callableStatement.execute();
            System.out.println("Đã cập nhật trạng thái thành công: " + status);

        } catch (SQLException e) {
            if (e.getSQLState().equals("45000")) {
                // Xử lý lỗi tùy chỉnh từ SIGNAL
                System.err.println("Lỗi từ thủ tục: " + e.getMessage());
            } else {
                System.err.println("Lỗi khi gọi thủ tục: " + e.getMessage());
            }
        }
    }

    public static int getFileSize(String filePath) {
        File file = new File(filePath);

        if (!file.exists() || !file.isFile()) {
            throw new IllegalArgumentException("Đường dẫn không hợp lệ hoặc tệp không tồn tại: " + filePath);
        }

        return (int) file.length(); // Kích cỡ file tính bằng byte
    }
//
    public static void insertLog(String location, int count, String status) {
        String sql = "{CALL insertLog(?, ?, ?)}"; // Câu lệnh gọi stored procedure

        try (Connection connection = connect();
             CallableStatement callableStatement = connection.prepareCall(sql)) {

            // Truyền tham số cho stored procedure
            callableStatement.setString(1, location); // p_location
            callableStatement.setInt(2, count);      // p_count
            callableStatement.setString(3, status);  // p_status

            // Thực thi stored procedure
            callableStatement.execute();
            System.out.println("Log đã được chèn thành công!");

        } catch (SQLException e) {
            if ("45000".equals(e.getSQLState())) {
                // Xử lý lỗi SIGNAL từ stored procedure
                System.err.println("Lỗi từ thủ tục: " + e.getMessage());
            } else {
                System.err.println("Lỗi khi chèn log: " + e.getMessage());
            }
        }
    }
}
