# CSharp-SQLiteBlob-Demo

System.Data.SQLiteにてSQLiteBlobを使用する方法をまとめたサンプルソースです。

SQLiteではバイナリデータをBLOB型として扱うことができます。DotNetFrameworkからはバイナリ全体をbyte配列として扱うことができますが、SQLiteBlobを使用することで、バイナリの一部分を少しずつ取り扱うことができます。

ここでは、SQLiteBlobの取得方法の説明を行います。byte配列として扱うので十分であれば、
[StackOverflowに簡潔な例があるので](https://stackoverflow.com/questions/625029/how-do-i-store-and-retrieve-a-blob-from-sqlite)、そちらを参照してください。

## BLOB型を持つ表の例

サンプルでは、BLOBのカラムとタプルを識別するためのidのみの単純な表を使用します。

```sql
create table binary_storage( 
    id  integer PRIMARY KEY, -- another alias for the rowid
    bin blob
)
```

列を追加することはできますが、SQLiteDataReader.GetBlobを使用してSQLiteBlobを取得する場合、System.Data.SQLiteの実装上の問題により以下のルールを守る必要があります。

1. CommandBehavior.KeyInfoの指定が必要
2. SQLiteDataReaderを得るためのSelect文では、Blob型のカラムに別名をつけてはならない。もしくは元のカラム名を保持する
3. SQLiteDataReaderを得るためのSelect文では、Blob型のカラムが定義されているテーブルは結合すべきでない？(駆動表にしていれば問題ない？)

上記の問題を鑑みると、バイナリデータの管理は専用のシンプルなテーブルを用意したほうが無難です。


## SQLiteBlobの取得方法(version 1.0.108.0)

### Insert
```cs
long InsertDataBlob108(SQLiteConnection con, Stream inStream)
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
```


### Select

```cs
void SelectDataBlob108(SQLiteConnection con, long id, Stream outStream)
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
```

## SQLiteBlobの取得方法(version 1.0.109.0以降)

1.0.109.0にて、SQLiteBlobに新しいメソッド(Create)が追加されました。
このメソッドは、[Incremental I/O](https://sqlite.org/c3ref/blob_open.html)のラッパーとして使用できます。

ここでは、Insert文のみ例として挙げます(Select文は108とほとんど同じばかりか、むしろコード量が増えるため)。

### Insert
```cs
long InsertDataBlob109(SQLiteConnection con, Stream inStream)
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
```