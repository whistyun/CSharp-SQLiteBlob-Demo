using System;
using System.IO;
using System.Data;
using System.Data.SQLite;
using System.Collections.Generic;
using System.Linq;

namespace vbefx4
{
    class SQLiteBlobDemo : IDisposable
    {
        private const int BUFFER_SIZE = 4;
        private SQLiteConnection con;

        public SQLiteBlobDemo(SQLiteConnectionStringBuilder sqlConnectionSb)
        {
            con = new SQLiteConnection(sqlConnectionSb.ToString());
            con.Open();
        }

        ~SQLiteBlobDemo()
        {
            Dispose();
        }

        public void Dispose()
        {
            if (con != null)
            {
                con.Close();
                con = null;
            }
        }

        public void CreateTable()
        {
            using (var cmd = new SQLiteCommand(con))
            {
                cmd.CommandText = @"
						create table binary_storage( 
							id integer PRIMARY KEY  ,
							bin blob
						)
					";

                cmd.ExecuteNonQuery();
            }
        }

        public int UploadByByteArray(Stream stream)
        {
            var length = (int)stream.Length;
            var binary = new byte[length];
            stream.Seek(0, SeekOrigin.Begin);
            stream.Read(binary, 0, length);

            using (var cmd = new SQLiteCommand(con))
            {
                cmd.CommandText = @"
					insert into binary_storage (
						bin
					) values (
						?
					)
				";
                cmd.Parameters.Add(new SQLiteParameter(DbType.Binary) { Value = binary });
                cmd.ExecuteNonQuery();
            }

            // integer PRIMARY KEYとした、カラムはrowidの別名になるため、
            // last_insert_rowidを取得すれば採番値がわかる
            // see: https://sqlite.org/c3ref/last_insert_rowid.html
            return GetLastRowId(con);
        }

        public byte[] DownloadByByteArray(int id)
        {
            using (var cmd = new SQLiteCommand("select bin from binary_storage where id = ?", con))
            {
                cmd.Parameters.Add(new SQLiteParameter(DbType.Int32) { Value = id });

                using (var reader = cmd.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        return (byte[])reader[0];
                    }
                    else
                    {
                        throw new KeyNotFoundException("id=" + id);
                    }
                }
            }
        }

        public int UploadByBlob(Stream stream)
        {
            var length = (int)stream.Length;

            using (var cmd = new SQLiteCommand(con))
            {
                // 事前に空のBLOBでインサート(必要なサイズはこの時点で決める必要がある)
                // see: https://sqlite.org/c3ref/blob_write.html
                cmd.CommandText = @"
					insert into binary_storage (
						bin
					) values (
						zeroblob(?)
					)
				";
                cmd.Parameters.Add(new SQLiteParameter(DbType.Int32) { Value = length });
                cmd.ExecuteNonQuery();
            }

            // integer PRIMARY KEYとした、カラムはrowidの別名になるため、
            // last_insert_rowidを取得すれば採番値がわかる
            // see: https://sqlite.org/c3ref/last_insert_rowid.html
            int id = GetLastRowId(con);

            using (var cmd = new SQLiteCommand(con))
            {
                // SQLiteBlobを使用するために、上でインサートした行を取得する。
                // SQLiteBlobは内部で、sqlite3_blob_openを使用しているが、
                // それに渡すパラメタ(スキーマ名、テーブル名、rowid、BLOBとして扱うカラムのカラム名)が
                // 取得できるようにする必要がある。そのため、
                //  1.SELECTの駆動表はBLOBを管理しているテーブル
                //  2.SELECT発行時に「System.Data.CommandBehavior.KeyInfo」を指定する
                //  3.取得項目は駆動表にゆかりがあるカラム。そしてblobのカラム名とエイリアスが一致するようにする
                // を行っている。
                //
                // 3.はblobのカラムを直接指定してしまうと、blobの内容をメモリに一度乗っけてしまう
                // (DbDataReader.Item[Int32]等で、値を取得できるようにするため？)ので、BLOBの項目ではない
                // 別のカラムに別名をつけることで対応している(ただし、この方法が正しいやり方か不明。ドキュメントが見つからない)
                //
                // https://sqlite.org/c3ref/blob.html
                // https://sqlite.org/c3ref/blob_open.html
                cmd.CommandText = @" select rowid as bin from binary_storage where id = ?";
                cmd.Parameters.Add(new SQLiteParameter(DbType.Int32) { Value = id });
                using (var reader = cmd.ExecuteReader(System.Data.CommandBehavior.KeyInfo))
                {
                    var b = reader.Read();

                    using (var blob = reader.GetBlob(0, false))
                    {
                        byte[] buffer = new byte[BUFFER_SIZE];

                        int blobOffset = 0;

                        int read;
                        while ((read = stream.Read(buffer, 0, buffer.Length)) > 0)
                        {
                            blob.Write(buffer, read, blobOffset);
                            blobOffset += read;
                        }
                    }
                }

                return id;
            }
        }

        public byte[] DownloadByBlob(int id)
        {
            string sql = @"
				select
					rowid       as bin,
					length(bin) as bin_length
				from
					binary_storage where id = ?
			";

            using (var cmd = new SQLiteCommand(sql, con))
            {
                cmd.Parameters.Add(new SQLiteParameter(DbType.Int32) { Value = id });
                using (var reader = cmd.ExecuteReader(System.Data.CommandBehavior.KeyInfo))
                {
                    if (reader.Read())
                    {
                        var blobSize = reader.GetInt32(1);

                        using (var blob = reader.GetBlob(0, true))
                        {
                            byte[] buffer = new byte[BUFFER_SIZE];

                            var blobOffset = 0;
                            var blobRemain = blobSize;
                            using (var stream = new MemoryStream())
                            {
                                while (blobRemain > 0)
                                {
                                    var read = Math.Min(blobRemain, buffer.Length);
                                    blob.Read(buffer, read, blobOffset);

                                    stream.Write(buffer, 0, read);
                                    blobRemain -= read;
                                    blobOffset += read;
                                }
                                return stream.ToArray();
                            }

                        }
                    }
                    else
                    {
                        throw new KeyNotFoundException("id=" + id);
                    }
                }
            }
        }

        private static int GetLastRowId(SQLiteConnection cnt)
        {
            using (var command = new SQLiteCommand("select last_insert_rowid()", cnt))
            {
                using (var reader = command.ExecuteReader())
                {
                    reader.Read();
                    return reader.GetInt32(0);
                }
            }
        }

    }

    class Program
    {
        static void Main(string[] args)
        {
            var sqlConnectionSb = new SQLiteConnectionStringBuilder { DataSource = ":memory:" };

            using (var sql = new SQLiteBlobDemo(sqlConnectionSb))
            {
                sql.CreateTable();

                byte[] test = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 };

                int arrayId = sql.UploadByByteArray(new MemoryStream(test));
                int blobId = sql.UploadByBlob(new MemoryStream(test));

                byte[] res1 = sql.DownloadByByteArray(arrayId);
                byte[] res2 = sql.DownloadByByteArray(blobId);
                byte[] res3 = sql.DownloadByBlob(arrayId);
                byte[] res4 = sql.DownloadByBlob(blobId);

                Console.WriteLine("test: " + String.Join(", ", test.Select(bin => bin.ToString("X2"))));
                Console.WriteLine("res1 is match: " + Enumerable.SequenceEqual(test, res1));
                Console.WriteLine("res2 is match: " + Enumerable.SequenceEqual(test, res2));
                Console.WriteLine("res3 is match: " + Enumerable.SequenceEqual(test, res3));
                Console.WriteLine("res4 is match: " + Enumerable.SequenceEqual(test, res4));
            }

            Console.WriteLine("press any key...");
            Console.ReadKey();
        }
    }
}
