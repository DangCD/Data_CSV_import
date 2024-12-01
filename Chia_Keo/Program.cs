using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Text;
using System.Timers;

class Program
{
    private static Timer _timer;
    private static string _connectionString = "Server=localhost;Database=Data_Test;Trusted_Connection=True;";
    private static string _tableName = "DTB";

    static void Main(string[] args)
    {
        Console.WriteLine("Nhập danh sách tên các tệp cần xử lý (cách nhau bởi dấu phẩy): ");
        string[] fileNames = Console.ReadLine().Split(',');

        // Thiết lập Timer để kiểm tra các tệp sau mỗi khoảng thời gian (ví dụ: 5 giây)
        _timer = new Timer(5000); // 5000ms = 5 giây
        _timer.Elapsed += (sender, e) => ProcessFiles(fileNames);
        _timer.Start();
        Console.ReadKey();
    }

    private static void ProcessFiles(string[] fileNames)
    {
        foreach (var fileName in fileNames)
        {
            string csvFilePath = $@"C:\Users\Admin\Desktop\Data_Test\Data_month_{fileName.Trim()}.csv";
            int lastProcessedLine = GetLastProcessedLine(csvFilePath);
            Console.WriteLine($"Đang xử lý tệp: {csvFilePath}. Dòng cuối cùng đã xử lý: {lastProcessedLine}");

            List<string[]> newData = ReadCsvFile(csvFilePath, lastProcessedLine);

            // Xử lý dữ liệu mới
            using (var connection = new SqlConnection(_connectionString))
            {
                connection.Open();
                foreach (var record in newData)
                {
                    string noModel = record[0];
                    string rawDateTime = record[1];
                    string judge = record[2];

                    DateTime dateTime;
                    string datePart = rawDateTime.Substring(0, 8);
                    string timePart = rawDateTime.Substring(9);
                    string formattedDateTime = $"{datePart} {timePart}";
                    dateTime = DateTime.ParseExact(formattedDateTime, "yyyyMMdd HHmm", CultureInfo.InvariantCulture);

                    string selectQuery = $@"
                            SELECT TOP 1 No_Model, Date_time, judge
                            FROM [{_tableName}]
                            WHERE No_Model = @NoModel
                            ORDER BY Date_time DESC";

                    SqlCommand selectCmd = new SqlCommand(selectQuery, connection);
                    selectCmd.Parameters.AddWithValue("@NoModel", noModel);

                    SqlDataReader readerDb = selectCmd.ExecuteReader();

                    bool exists = false;
                    DateTime existingDateTime = DateTime.MinValue;
                    while (readerDb.Read())
                    {
                        exists = true;
                        existingDateTime = readerDb.GetDateTime(1);
                    }
                    readerDb.Close();

                    if (!exists)
                    {
                        string insertQuery = $@"
                                INSERT INTO [{_tableName}] (No_Model, Date_time, judge)
                                VALUES (@NoModel, @DateTime, @Judge)";

                        SqlCommand insertCmd = new SqlCommand(insertQuery, connection);
                        insertCmd.Parameters.AddWithValue("@NoModel", noModel);
                        insertCmd.Parameters.AddWithValue("@DateTime", dateTime);
                        insertCmd.Parameters.AddWithValue("@Judge", judge);
                        insertCmd.ExecuteNonQuery();
                    }
                    else if (dateTime > existingDateTime)
                    {
                        string updateQuery = $@"
                                UPDATE [{_tableName}]
                                SET Date_time = @DateTime, judge = @Judge
                                WHERE No_Model = @NoModel";

                        SqlCommand updateCmd = new SqlCommand(updateQuery, connection);
                        updateCmd.Parameters.AddWithValue("@NoModel", noModel);
                        updateCmd.Parameters.AddWithValue("@DateTime", dateTime);
                        updateCmd.Parameters.AddWithValue("@Judge", judge);
                        updateCmd.ExecuteNonQuery();
                    }
                }

                // Lưu dòng cuối đã xử lý vào tệp
                SaveLastProcessedLine(csvFilePath, lastProcessedLine + newData.Count);
            }
        }

        Console.WriteLine("Đồng bộ dữ liệu hoàn tất!");
    }

    // Đọc dữ liệu từ tệp CSV từ dòng tiếp theo
    private static List<string[]> ReadCsvFile(string csvFilePath, int startLine)
    {
        List<string[]> data = new List<string[]>();

        using (var reader = new StreamReader(csvFilePath))
        {
            // Đọc các dòng trước đó đã được xử lý
            for (int i = 0; i <= startLine && !reader.EndOfStream; i++)
            {
                reader.ReadLine(); // Bỏ qua những dòng đã xử lý
            }

            // Đọc các dòng chưa xử lý
            while (!reader.EndOfStream)
            {
                string line = reader.ReadLine();
                if (!string.IsNullOrWhiteSpace(line))
                {
                    string[] values = line.Split(',');
                    data.Add(values);
                }
            }
        }

        return data;
    }

    // Lưu vị trí dòng cuối đã xử lý vào tệp
    private static void SaveLastProcessedLine(string csvFilePath, int lineNumber)
    {
        string positionFilePath = GetPositionFilePath(csvFilePath);
        File.WriteAllText(positionFilePath, lineNumber.ToString());
    }

    // Lấy vị trí dòng cuối đã xử lý từ tệp
    private static int GetLastProcessedLine(string csvFilePath)
    {
        string positionFilePath = GetPositionFilePath(csvFilePath);
        if (File.Exists(positionFilePath))
        {
            return int.Parse(File.ReadAllText(positionFilePath));
        }
        return 0; // Nếu chưa có dòng nào được xử lý, bắt đầu từ dòng 0
    }

    // Tạo tên tệp lưu thông tin dòng cuối đã xử lý
    private static string GetPositionFilePath(string csvFilePath)
    {
        string fileNameWithoutExtension = Path.GetFileNameWithoutExtension(csvFilePath);
        return $@"C:\Users\Admin\Desktop\Data_Test\{fileNameWithoutExtension}_LastProcessed.txt";
    }
}

