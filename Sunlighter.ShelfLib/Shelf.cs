using Sunlighter.OptionLib;
using Sunlighter.TypeTraitsLib;
using System.Collections.Immutable;
using System.Data;
using System.Data.SQLite;

namespace Sunlighter.ShelfLib
{
    [Flags]
    public enum AddReplaceMode
    {
        Add = 1,
        Replace = 2,
        AddOrReplace = 3
    }

    public interface IShelfTransaction : IDisposable
    {
        void Commit();
    }

    public interface IShelf<K, V> : IDisposable
    {
        bool ContainsKey(K key);
        Option<V> TryGetValue(K key);
        bool SetValue(AddReplaceMode mode, K key, V value);
        bool DeleteValue(K key);
        int Count { get; }
        ImmutableList<K> GetKeys(int skip, int? take);

        IShelfTransaction BeginTransaction();
    }

    [Flags]
    public enum CreateOpenMode
    {
        Create = 1,
        Open = 2,
        CreateOrOpen = 3
    }

    public sealed class Shelf<K, V> : IShelf<K, V>, IDisposable
    {
        private readonly SQLiteConnection conn;
        private SQLiteTransaction? transaction;

        private readonly ITypeTraits<K> keyTypeTraits;
        private readonly ITypeTraits<V> valueTypeTraits;

        private readonly SelectKeysByHashQuery selectKeysByHashQuery;
        private readonly SelectValueByIdQuery selectValueByIdQuery;
        private readonly SelectKeyCountQuery selectKeyCountQuery;
        private readonly SelectKeyListQuery selectKeyListQuery;

        private readonly InsertQuery insertQuery;
        private readonly UpdateQuery updateQuery;
        private readonly DeleteQuery deleteQuery;

        private Shelf
        (
            SQLiteConnection conn,
            ITypeTraits<K> keyTypeTraits,
            ITypeTraits<V> valueTypeTraits
        )
        {
            this.conn = conn;
            this.transaction = null;

            this.keyTypeTraits = keyTypeTraits;
            this.valueTypeTraits = valueTypeTraits;

            selectKeysByHashQuery = new SelectKeysByHashQuery(conn);
            selectValueByIdQuery = new SelectValueByIdQuery(conn);
            selectKeyCountQuery = new SelectKeyCountQuery(conn);
            selectKeyListQuery = new SelectKeyListQuery(conn);

            insertQuery = new InsertQuery(conn);
            updateQuery = new UpdateQuery(conn);
            deleteQuery = new DeleteQuery(conn);
        }

        private void SetTransactions(SQLiteTransaction? transaction)
        {
            this.transaction = transaction;

            selectKeysByHashQuery.Transaction = transaction;
            selectValueByIdQuery.Transaction = transaction;
            selectKeyCountQuery.Transaction = transaction;
            selectKeyListQuery.Transaction = transaction;

            insertQuery.Transaction = transaction;
            updateQuery.Transaction = transaction;
            deleteQuery.Transaction = transaction;
        }

        public static Shelf<K, V> Create
        (
            string pathName,
            ITypeTraits<K> keyTypeTraits,
            ITypeTraits<V> valueTypeTraits,
            CreateOpenMode mode
        )
        {
            if (File.Exists(pathName))
            {
                if (mode.AllowsOpen())
                {
                    SQLiteConnectionStringBuilder scsb = new SQLiteConnectionStringBuilder();
                    scsb.DataSource = pathName;
                    scsb.FailIfMissing = true;

                    SQLiteConnection conn = new SQLiteConnection(scsb.ToString());
                    conn.Open();

                    return new Shelf<K, V>(conn, keyTypeTraits, valueTypeTraits);
                }
                else
                {
                    throw new InvalidOperationException($"File \"{pathName}\" already exists");
                }
            }
            else
            {
                if (mode.AllowsCreate())
                {
                    SQLiteConnectionStringBuilder scsb = new SQLiteConnectionStringBuilder();
                    scsb.DataSource = pathName;
                    scsb.FailIfMissing = false;

                    SQLiteConnection conn = new SQLiteConnection(scsb.ToString());
                    conn.Open();

                    conn.ExecuteNonQuery
                    (
                        """
                        CREATE TABLE "SunlighterShelf"
                        (
                            "id" INTEGER PRIMARY KEY AUTOINCREMENT,
                            "keyhash" BLOB NOT NULL,
                            "key" BLOB NOT NULL,
                            "value" BLOB NOT NULL
                        )
                        """
                    );

                    conn.ExecuteNonQuery
                    (
                        """
                        CREATE INDEX "SunlighterShelfKeyHash" ON "SunlighterShelf" ( "keyhash" )
                        """
                    );

                    return new Shelf<K, V>(conn, keyTypeTraits, valueTypeTraits);
                }
                else
                {
                    throw new InvalidOperationException($"File \"{pathName}\" does not exist");
                }
            }
        }

        private class SelectKeysByHashQuery : IDisposable
        {
            private readonly SQLiteCommand cmd;
            private readonly SQLiteParameter pHash;

            public SelectKeysByHashQuery(SQLiteConnection conn)
            {
                cmd = new SQLiteCommand
                (
                    """
                    SELECT "id", "key"
                    FROM "SunlighterShelf"
                    WHERE "keyhash" = @pHash
                    """
                );

                pHash = cmd.Parameters.Add("pHash", DbType.Binary);
                pHash.IsNullable = false;
            }

            public SQLiteTransaction? Transaction { get { return cmd.Transaction; } set { cmd.Transaction = value; } }

            public ImmutableList<(long, byte[])> Execute(byte[] hash)
            {
                pHash.Value = hash;
                ImmutableList<(long, byte[])> results = ImmutableList<(long, byte[])>.Empty;
                using (SQLiteDataReader sdr = cmd.ExecuteReader())
                {
                    while (sdr.Read())
                    {
                        long id = sdr.GetInt64(0);
                        byte[] key = sdr.GetBytes(1);
                        results = results.Add((id, key));
                    }
                }
                return results;
            }

            public void Dispose()
            {
                cmd.Dispose();
            }
        }

        private Option<long> GetIdForKey(K key)
        {
            byte[] hash = keyTypeTraits.GetSHA256Hash(key);
            foreach (var (id, keyBytes) in selectKeysByHashQuery.Execute(hash))
            {
                K theKey = keyTypeTraits.DeserializeFromBytes(keyBytes);
                if (keyTypeTraits.Compare(key, theKey) == 0)
                {
                    return Option<long>.Some(id);
                }
            }
            return Option<long>.None;
        }

        public bool ContainsKey(K key)
        {
            return GetIdForKey(key).HasValue;
        }

        private class SelectValueByIdQuery : IDisposable
        {
            private readonly SQLiteCommand cmd;
            private readonly SQLiteParameter pId;

            public SelectValueByIdQuery(SQLiteConnection conn)
            {
                cmd = new SQLiteCommand
                (
                    """
                    SELECT "value"
                    FROM "SunlighterShelf"
                    WHERE "id" = @pId
                    """
                );
                pId = cmd.Parameters.Add("pId", DbType.Int64);
                pId.IsNullable = false;
            }

            public SQLiteTransaction? Transaction { get { return cmd.Transaction; } set { cmd.Transaction = value; } }

            public byte[] Execute(long id)
            {
                pId.Value = id;
                using (SQLiteDataReader sdr = cmd.ExecuteReader())
                {
                    if (sdr.Read())
                    {
                        return sdr.GetBytes(0);
                    }
                    else
                    {
                        throw new InvalidOperationException($"No value found for id {id}");
                    }
                }
            }

            public void Dispose()
            {
                cmd.Dispose();
            }
        }

        private T WithTransactionIfPossible<T>(Func<T> func)
        {
            if (transaction is null)
            {
                using IShelfTransaction tx = BeginTransaction();
                T result = func();
                tx.Commit();
                return result;
            }
            else
            {
                return func();
            }
        }

        private Option<V> TryGetValue_Internal(K key)
        {

            Option<long> id = GetIdForKey(key);
            if (id.HasValue)
            {
                byte[] valueBytes = selectValueByIdQuery.Execute(id.Value);
                V value = valueTypeTraits.DeserializeFromBytes(valueBytes);
                return Option<V>.Some(value);
            }
            else
            {
                return Option<V>.None;
            }
        }

        public Option<V> TryGetValue(K key)
        {
            return WithTransactionIfPossible(() => TryGetValue_Internal(key));
        }

        private bool SetValue_Internal(AddReplaceMode mode, K key, V value)
        {
            Option<long> idForKey = GetIdForKey(key);

            if (idForKey.HasValue)
            {
                if (mode.AllowsReplace())
                {
                    byte[] valueBytes = valueTypeTraits.SerializeToBytes(value);
                    return updateQuery.Execute(idForKey.Value, valueBytes);
                }
                else return false;
            }
            else
            {
                if (mode.AllowsAdd())
                {
                    byte[] hash = keyTypeTraits.GetSHA256Hash(key);
                    byte[] keyBytes = keyTypeTraits.SerializeToBytes(key);
                    byte[] valueBytes = valueTypeTraits.SerializeToBytes(value);
                    return insertQuery.Execute(hash, keyBytes, valueBytes);
                }
                else return false;
            }
        }

        public bool SetValue(AddReplaceMode mode, K key, V value)
        {
            return WithTransactionIfPossible(() => SetValue_Internal(mode, key, value));
        }

        private bool DeleteValue_Internal(K key)
        {
            Option<long> idForKey = GetIdForKey(key);
            if (idForKey.HasValue)
            {
                return deleteQuery.Execute(idForKey.Value);
            }
            else
            {
                return false;
            }
        }

        public bool DeleteValue(K key)
        {
            return WithTransactionIfPossible(() => DeleteValue_Internal(key));
        }

        private class SelectKeyCountQuery : IDisposable
        {
            private readonly SQLiteCommand cmd;

            public SelectKeyCountQuery
            (
                SQLiteConnection conn
            )
            {
                string cmdText =
                    """
                    SELECT COUNT("id") FROM "SunlighterShelf"
                    """;

                cmd = new SQLiteCommand(cmdText, conn);
            }

            public SQLiteTransaction? Transaction { get { return cmd.Transaction; } set { cmd.Transaction = value; } }

            public int Execute()
            {
                return Convert.ToInt32(cmd.ExecuteScalar());
            }

            public void Dispose()
            {
                cmd.Dispose();
            }
        }

        public int Count
        {
            get
            {
                return selectKeyCountQuery.Execute();
            }
        }

        private class SelectKeyListQuery : IDisposable
        {
            private readonly SQLiteCommand cmd;
            private readonly SQLiteParameter pLimit;
            private readonly SQLiteParameter pOffset;

            public SelectKeyListQuery
            (
                SQLiteConnection conn
            )
            {
                cmd = new SQLiteCommand
                (
                    """
                    SELECT "key" FROM "SunlighterShelf" ORDER BY "keyhash", "id" LIMIT @pLimit OFFSET @pOffset
                    """,
                    conn
                );

                pLimit = cmd.Parameters.Add("pLimit", DbType.Int32);
                pOffset = cmd.Parameters.Add("pOffset", DbType.Int32);
            }

            public SQLiteTransaction? Transaction
            {
                get { return cmd.Transaction; }
                set { cmd.Transaction = value; }
            }

            public ImmutableList<byte[]> Execute(int limit, int offset)
            {
                pLimit.Value = limit;
                pOffset.Value = offset;
                ImmutableList<byte[]> results = ImmutableList<byte[]>.Empty;
                using (SQLiteDataReader sdr = cmd.ExecuteReader())
                {
                    while (sdr.Read())
                    {
                        byte[] keyBytes = sdr.GetBytes(0);
                        results = results.Add(keyBytes);
                    }
                }
                return results;
            }

            public void Dispose()
            {
                cmd.Dispose();
            }
        }

        public ImmutableList<K> GetKeys(int skip, int? take)
        {
            if (skip < 0) throw new ArgumentException($"{nameof(skip)} must not be negative");
            if (take.HasValue && take.Value < 0) throw new ArgumentException($"{nameof(take)} must not be negative");

            return selectKeyListQuery.Execute(take ?? -1, skip).Select(keyBytes => keyTypeTraits.DeserializeFromBytes(keyBytes)).ToImmutableList();
        }

        public IShelfTransaction BeginTransaction()
        {
            if (transaction is null)
            {
                return new TransactionInternal(this);
            }
            else
            {
                throw new InvalidOperationException("Transaction already in progress (nested transactions are not supported)");
            }
        }

        public void Dispose()
        {
            selectKeysByHashQuery.Dispose();
            selectValueByIdQuery.Dispose();
            selectKeyListQuery.Dispose();
            selectKeyCountQuery.Dispose();

            insertQuery.Dispose();
            updateQuery.Dispose();
            deleteQuery.Dispose();

            if (transaction is not null)
            {
                transaction.Dispose();
                transaction = null;
            }

            conn.Close();
            conn.Dispose();
        }

        private class InsertQuery : IDisposable
        {
            private readonly SQLiteCommand cmd;
            private readonly SQLiteParameter pKeyHash;
            private readonly SQLiteParameter pKey;
            private readonly SQLiteParameter pValue;

            public InsertQuery
            (
                SQLiteConnection conn
            )
            {
                cmd = new SQLiteCommand
                (
                    """
                    INSERT INTO "SunlighterShelf" ( "keyhash", "key", "value" )
                    VALUES ( @pKeyHash, @pKey, @pValue )
                    """,
                    conn
                );
                pKeyHash = cmd.Parameters.Add("pKeyHash", DbType.Binary);
                pKey = cmd.Parameters.Add("pKey", DbType.Binary);
                pValue = cmd.Parameters.Add("pValue", DbType.Binary);
            }

            public SQLiteTransaction? Transaction
            {
                get { return cmd.Transaction; }
                set { cmd.Transaction = value; }
            }

            public bool Execute(byte[] keyHash, byte[] key, byte[] value)
            {
                pKeyHash.Value = keyHash;
                pKey.Value = key;
                pValue.Value = value;
                return cmd.ExecuteNonQuery() == 1;
            }

            public void Dispose()
            {
                cmd.Dispose();
            }
        }

        private class UpdateQuery : IDisposable
        {
            private readonly SQLiteCommand cmd;
            private readonly SQLiteParameter pId;
            private readonly SQLiteParameter pValue;

            public UpdateQuery
            (
                SQLiteConnection conn
            )
            {

                cmd = new SQLiteCommand
                (
                    """
                    UPDATE "SunlighterShelf"
                    SET "value" = @pValue
                    WHERE "id" = @pId
                    """,
                    conn
                );
                pValue = cmd.Parameters.Add("pValue", DbType.Binary);
                pId = cmd.Parameters.Add("pId", DbType.Int64);
            }

            public SQLiteTransaction? Transaction
            {
                get { return cmd.Transaction; }
                set { cmd.Transaction = value; }
            }

            public bool Execute(long id, byte[] valueBytes)
            {
                pId.Value = id;
                pValue.Value = valueBytes;
                return cmd.ExecuteNonQuery() == 1;
            }

            public void Dispose()
            {
                cmd.Dispose();
            }
        }

        private class DeleteQuery : IDisposable
        {
            private readonly SQLiteCommand cmd;
            private readonly SQLiteParameter pId;

            public DeleteQuery
            (
                SQLiteConnection conn
            )
            {
                cmd = new SQLiteCommand
                (
                    """
                    DELETE FROM "SunlighterShelf" WHERE "id" = @pId
                    """,
                    conn
                );
                pId = cmd.Parameters.Add("pId", DbType.Int64);
            }

            public SQLiteTransaction? Transaction
            {
                get { return cmd.Transaction; }
                set { cmd.Transaction = value; }
            }

            public bool Execute(long id)
            {
                pId.Value = id;
                return cmd.ExecuteNonQuery() == 1;
            }

            public void Dispose()
            {
                cmd.Dispose();
            }
        }

        private class TransactionInternal : IShelfTransaction
        {
            private readonly Shelf<K, V> parent;

            public TransactionInternal(Shelf<K, V> parent)
            {
                this.parent = parent;
                parent.SetTransactions(parent.conn.BeginTransaction(IsolationLevel.Serializable));
            }

            public void Commit()
            {
                if (parent.transaction is null) throw new InvalidOperationException("No transaction to commit");
                parent.transaction.Commit();
            }

            public void Dispose()
            {
                if (parent.transaction is null) throw new InvalidOperationException("No transaction to dispose");
                parent.transaction.Dispose();
                parent.SetTransactions(null);
            }
        }
    }

    public static class Shelf
    {
        public static bool AllowsOpen(this CreateOpenMode mode)
        {
            return mode switch
            {
                CreateOpenMode.Open or CreateOpenMode.CreateOrOpen => true,
                _ => false,
            };
        }

        public static bool AllowsCreate(this CreateOpenMode mode)
        {
            return mode switch
            {
                CreateOpenMode.Create or CreateOpenMode.CreateOrOpen => true,
                _ => false,
            };
        }

        public static bool AllowsAdd(this AddReplaceMode mode)
        {
            return mode switch
            {
                AddReplaceMode.Add or AddReplaceMode.AddOrReplace => true,
                _ => false,
            };
        }

        public static bool AllowsReplace(this AddReplaceMode mode)
        {
            return mode switch
            {
                AddReplaceMode.Replace or AddReplaceMode.AddOrReplace => true,
                _ => false,
            };
        }

        public static byte[] GetBytes(this SQLiteDataReader sdr, int column)
        {
            long count = sdr.GetBytes(column, 0L, null, 0, 0);
            if (count < (1L << 30))
            {
                int iCount = unchecked((int)count);
                byte[] buf = new byte[iCount];
                sdr.GetBytes(column, 0L, buf, 0, iCount);
                return buf;
            }
            else
            {
                throw new InvalidOperationException($"Blob too large ({count} bytes)");
            }
        }

        public static Shelf<K, V> Create<K, V>
        (
            string pathName,
            ITypeTraits<K> keyTypeTraits,
            ITypeTraits<V> valueTypeTraits,
            CreateOpenMode mode
        )
        {
            return Shelf<K, V>.Create(pathName, keyTypeTraits, valueTypeTraits, mode);
        }

        public static void WithTransaction<K, V>(this IShelf<K, V> shelf, Action a)
        {
            using (IShelfTransaction t = shelf.BeginTransaction())
            {
                a();
                t.Commit();
            }
        }

        public static T WithTransaction<K, V, T>(this IShelf<K, V> shelf, Func<T> func)
        {
            using (IShelfTransaction t = shelf.BeginTransaction())
            {
                T result = func();
                t.Commit();
                return result;
            }
        }

        public static int ExecuteNonQuery(this SQLiteConnection conn, string query)
        {
            using SQLiteCommand cmd = new SQLiteCommand(query, conn);
            return cmd.ExecuteNonQuery();
        }
    }
}
