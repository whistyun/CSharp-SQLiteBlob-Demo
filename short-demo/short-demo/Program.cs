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
							id  integer PRIMARY KEY, -- another alias for the rowid
							bin blob
						)
					";

                cmd.ExecuteNonQuery();
            }
        }

        #region byte-array

        public long InsertDataByteArray(Stream inStream)
        {
            // stream to byte-array
            var length = (int)inStream.Length;
            var binary = new byte[length];
            inStream.Seek(0, SeekOrigin.Begin);
            inStream.Read(binary, 0, length);

            using (var cmd = new SQLiteCommand(con))
            {
                cmd.CommandText = @"
					insert into binary_storage (
						bin
					) values (
						@binary
					)
				";
                cmd.Parameters.Add("@binary", DbType.Binary).Value = binary;
                cmd.ExecuteNonQuery();

                cmd.Reset();
                cmd.CommandText = "select last_insert_rowid()";
                return (long)cmd.ExecuteScalar();
            }
        }

        public byte[] GetDataByteArray(long id)
        {
            using (var cmd = new SQLiteCommand("select bin from binary_storage where id = ?", con))
            {
                cmd.Parameters.Add(new SQLiteParameter(DbType.Int64) { Value = id });

                using (var reader = cmd.ExecuteReader())
                {
                    if (!reader.Read())
                    {
                        throw new KeyNotFoundException("id=" + id);
                    }

                    return (byte[])reader[0];
                }
            }
        }

        #endregion


        #region SQLiteBlob version 1.0.108.0

        public long InsertDataBlob108(Stream inStream)
        {
            var length = (int)inStream.Length;

            using (var cmd = new SQLiteCommand(con))
            {
                // 事前に空のBLOBでインサート(必要なサイズはこの時点で決める)
                // see: https://sqlite.org/c3ref/blob_write.html
                cmd.CommandText = @"
                    insert into binary_storage (
                        bin
                    ) values (
                        zeroblob(@bin_len)
                    )
                ";
                cmd.Parameters.Add("@bin_len", DbType.Int32).Value = length;
                cmd.ExecuteNonQuery();

                cmd.Reset();
                // 主キー(= rowid)取得。
                // SQLiteでは、integer PRIMARY KEYとしたカラムはrowidの別名
                // see: https://sqlite.org/c3ref/last_insert_rowid.html
                cmd.CommandText = "select last_insert_rowid()";
                var id = (long)cmd.ExecuteScalar();

                cmd.Reset();
                // SQLiteBlobを使用するために、上でインサートした行を取得する。
                // ここでは、rowidにblob型カラムの名称をつけることで、
                // blobデータを取得せずに(blobの内容をメモリに展開せずに)
                // System.Data.SQLiteにblob型カラムの名称を渡すことができる。

                // see: https://sqlite.org/c3ref/blob.html
                //      https://sqlite.org/c3ref/blob_open.html
                cmd.CommandText = @"
                    select rowid as bin
                    from   binary_storage
                    where  id = @id";
                cmd.Parameters.Add("@id", DbType.Int64).Value = id;
                using (var reader = cmd.ExecuteReader(CommandBehavior.KeyInfo))
                {
                    reader.Read();

                    // 本来、GetBlobにはblob型カラムの名称が解るカラムを
                    // 指定する必要が、Select文にてBlob型カラムの名称を
                    // 別名としてつけることで誤魔化す。
                    using (var blob = reader.GetBlob(0, false))
                    {
                        byte[] buffer = new byte[BUFFER_SIZE];

                        int blobOffset = 0;

                        int read;
                        while ((read = inStream.Read(buffer, 0, buffer.Length)) > 0)
                        {
                            blob.Write(buffer, read, blobOffset);
                            blobOffset += read;
                        }
                    }
                }

                return id;
            }
        }

        public void SelectDataBlob108(long id, Stream outStream)
        {
            using (var cmd = new SQLiteCommand(con))
            {
                // blobのサイズが解らないとどこまで読み込めばよいかわからない
                cmd.CommandText = @"
                    select
                        rowid       as bin,
                        length(bin) as bin_length
                    from
                        binary_storage where id = @id
                ";
                cmd.Parameters.Add("@id", DbType.Int64).Value = id;
                using (var reader = cmd.ExecuteReader(CommandBehavior.KeyInfo))
                {
                    if (!reader.Read())
                    {
                        throw new KeyNotFoundException("id=" + id);
                    }

                    var blobSize = reader.GetInt32(1);

                    using (var blob = reader.GetBlob(0, true))
                    {
                        byte[] buffer = new byte[BUFFER_SIZE];

                        var blobOffset = 0;
                        var blobRemain = blobSize;

                        while (blobRemain > 0)
                        {
                            var read = Math.Min(blobRemain, buffer.Length);
                            blob.Read(buffer, read, blobOffset);

                            outStream.Write(buffer, 0, read);
                            blobRemain -= read;
                            blobOffset += read;
                        }
                    }
                }
            }
        }

        public byte[] GetDataBlob108(long id)
        {
            using (var stream = new MemoryStream())
            {
                SelectDataBlob108(id, stream);

                return stream.ToArray();
            }
        }

        #endregion


        #region SQLiteBlob version 1.0.109.0 and later

        public long InsertDataBlob109(Stream inStream)
        {
            var length = (int)inStream.Length;

            using (var cmd = new SQLiteCommand(con))
            {
                // 事前に空のBLOBでインサート(必要なサイズはこの時点で決める)
                // see: https://sqlite.org/c3ref/blob_write.html
                cmd.CommandText = @"
                    insert into binary_storage (
                        bin
                    ) values (
                        zeroblob(@bin_len)
                    )
                ";
                cmd.Parameters.Add("@bin_len", DbType.Int32).Value = length;
                cmd.ExecuteNonQuery();

                cmd.Reset();
                // 主キー(= rowid)取得。
                // SQLiteでは、integer PRIMARY KEYとしたカラムはrowidの別名
                // see: https://sqlite.org/c3ref/last_insert_rowid.html
                cmd.CommandText = "select last_insert_rowid()";
                var id = (long)cmd.ExecuteScalar();

                // Incremental I/O
                // see: https://sqlite.org/c3ref/blob_open.html
                using (var blob = SQLiteBlob.Create(
                    con,              // connection
                    con.Database,     // database name
                    "binary_storage", // table name
                    "bin",            // column
                    id,               // rowid
                    false             // readonly
                    ))
                {
                    byte[] buffer = new byte[BUFFER_SIZE];

                    int blobOffset = 0;

                    int read;
                    while ((read = inStream.Read(buffer, 0, buffer.Length)) > 0)
                    {
                        blob.Write(buffer, read, blobOffset);
                        blobOffset += read;
                    }
                }

                return id;
            }
        }

        public void SelectDataBlob109(long id, Stream outStream)
        {
            using (var cmd = new SQLiteCommand(con))
            {
                // blobのサイズが解らないとどこまで読み込めばよいかわからない
                cmd.CommandText = @"
                    select
                        length(bin) as bin_length
                    from
                        binary_storage where id = @id
                ";
                cmd.Parameters.Add("@id", DbType.Int64).Value = id;
                using (var reader = cmd.ExecuteReader(CommandBehavior.KeyInfo))
                {
                    if (!reader.Read())
                    {
                        throw new KeyNotFoundException("id=" + id);
                    }

                    var blobSize = reader.GetInt32(0);

                    using (var blob = SQLiteBlob.Create(
                        con,              // connection
                        con.Database,     // database name
                        "binary_storage", // table name
                        "bin",            // column
                        id,               // rowid
                        true              // readonly
                        ))
                    {
                        byte[] buffer = new byte[BUFFER_SIZE];

                        var blobOffset = 0;
                        var blobRemain = blobSize;

                        while (blobRemain > 0)
                        {
                            var read = Math.Min(blobRemain, buffer.Length);
                            blob.Read(buffer, read, blobOffset);

                            outStream.Write(buffer, 0, read);
                            blobRemain -= read;
                            blobOffset += read;
                        }
                    }
                }
            }
        }

        public byte[] GetDataBlob109(long id)
        {
            using (var stream = new MemoryStream())
            {
                SelectDataBlob109(id, stream);

                return stream.ToArray();
            }
        }

        #endregion

    }

    class Program
    {
        static void Main(string[] args)
        {
            var sqlConnectionSb = new SQLiteConnectionStringBuilder { DataSource = ":memory:" };

            using (var sql = new SQLiteBlobDemo(sqlConnectionSb))
            {
                sql.CreateTable();

                byte[] test = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23 };
                Console.WriteLine("test: " + String.Join(", ", test.Select(bin => bin.ToString("X2"))));

                long arrayId = sql.InsertDataByteArray(new MemoryStream(test));
                long blobId108 = sql.InsertDataBlob108(new MemoryStream(test));
                long blobId109 = sql.InsertDataBlob109(new MemoryStream(test));

                foreach (var registeredId in new[] { arrayId, blobId108, blobId109 })
                {
                    byte[] selectByteArray = sql.GetDataByteArray(registeredId);
                    byte[] selectBlobId108 = sql.GetDataBlob108(registeredId);
                    byte[] selectBlobId109 = sql.GetDataBlob109(registeredId);

                    Console.WriteLine("insert[" + registeredId + "]: " + String.Join(", ", selectByteArray.Select(bin => bin.ToString("X2"))));
                    Console.WriteLine("selectByteArray is match: " + Enumerable.SequenceEqual(test, selectByteArray));
                    Console.WriteLine("selectBlobId108 is match: " + Enumerable.SequenceEqual(test, selectBlobId108));
                    Console.WriteLine("selectBlobId109 is match: " + Enumerable.SequenceEqual(test, selectBlobId109));
                }
            }

            Console.WriteLine("press any key...");
            Console.ReadKey();
        }
    }
}
