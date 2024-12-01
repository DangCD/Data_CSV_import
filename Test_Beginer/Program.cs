using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Timers;

namespace Test_Beginer
{
    internal class Program
    {
        
        //private static Timer timer;
        private static string connectionString = "Server=localhost;Database=Data_Test;Trusted_Connection=True;";
        private static string tableName = "DTB";
        static void Main(string[] args)
        {
            string month = Console.ReadLine();
            string csvFilePath = string.Format(@"C:\Users\Admin\Desktop\Data_Test\Data_month_{0}.csv", month);
            int line_number = Get_Last_Position(csvFilePath);
            List <string[]> list_Data = Read_file_CSV(csvFilePath, line_number);
                using(var connection = new SqlConnection(connectionString))
                {
                connection.Open();
                foreach (var data in list_Data)
                    {
                    string noModel = data[0];
                    string rawDateTime = data[1];
                    string judge = data[2];
                    DateTime date_time;
                    string datepart = rawDateTime.Substring(0, 8);
                    string timepart = rawDateTime.Substring(9);
                    string time = $"{datepart} {timepart}";
                    date_time = DateTime.ParseExact(time, "yyyyMMdd HHmm", CultureInfo.InvariantCulture);

                    string selectQuery = $@"
                            SELECT TOP 1 No_Model, Date_time, judge
                            FROM [{tableName}]
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
                                INSERT INTO [{tableName}] (No_Model, Date_time, judge)
                                VALUES (@NoModel, @DateTime, @Judge)";

                        SqlCommand insertCmd = new SqlCommand(insertQuery, connection);
                        insertCmd.Parameters.AddWithValue("@NoModel", noModel);
                        insertCmd.Parameters.AddWithValue("@DateTime", date_time);
                        insertCmd.Parameters.AddWithValue("@Judge", judge);
                        insertCmd.ExecuteNonQuery();
                    }
                    else if (date_time > existingDateTime)
                    {
                        string updateQuery = $@"
                                UPDATE [{tableName}]
                                SET Date_time = @DateTime, judge = @Judge
                                WHERE No_Model = @NoModel";

                        SqlCommand updateCmd = new SqlCommand(updateQuery, connection);
                        updateCmd.Parameters.AddWithValue("@NoModel", noModel);
                        updateCmd.Parameters.AddWithValue("@DateTime", date_time);
                        updateCmd.Parameters.AddWithValue("@Judge", judge);
                        updateCmd.ExecuteNonQuery();
                    }

                }
                Set_Last_Position(csvFilePath, line_number + list_Data.Count);
            }
            Console.WriteLine("Done");
            Console.ReadKey();

            //timer = new Timer(5000);
            ////timer.Elapsed += (sender, e) => Insert_Data;
            //timer.Start();
           
        }
        //hàm tạo path của file lưu vị trí
        private static string File_Path_Position(string file_path)
        {
            string File_Name_of_Position = Path.GetFileNameWithoutExtension(file_path);
            return $@"C:\Users\Admin\Desktop\Data_Test\Position_{File_Name_of_Position}.txt";
        }
        //hàm lấy vị trí
        private static int Get_Last_Position(string file_path)
        {
            string file_path_position = File_Path_Position(file_path);
            if (File.Exists(file_path_position))
            {
                return int.Parse(File.ReadAllText(file_path_position));
            }
            else return 0;

        }
        //hàm set vị trí
        private static void Set_Last_Position(string file_path, int Line_number)
        {
            string file_path_position = File_Path_Position(file_path);
            File.WriteAllText(file_path_position, Line_number.ToString());
        }
        //hàm đọc
        private static List<string[]> Read_file_CSV(string file_path, int start_Line)
        {
            List<string[]> List_data = new List<string[]>();
            using (var reader = new StreamReader(file_path))
            {
                for (int i = 0; i <= start_Line && !reader.EndOfStream; i++)
                {
                    Console.ReadLine();
                }
                while (!reader.EndOfStream)
                {
                    string line = reader.ReadLine();
                    string[] value = line.Split(',');
                    List_data.Add(value);
                }

            }   
            return List_data;
        }
      
    }
}
