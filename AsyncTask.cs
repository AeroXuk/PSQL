using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Threading;
using System.Xml;
using Microsoft.SqlServer.Server;

namespace psql.sqlnotes.info
{
    public class AsyncSessionStore
    {
        object SyncObject = new object();
        List<AsyncTask> buffer = new List<AsyncTask>();
        void SetSize(short size)
        {
            lock (SyncObject)
            {
                while (buffer.Count <= size + 50)
                {
                    buffer.Add(null);
                }
            }
        }
        public AsyncTask GetValue(short spid)
        {
            SetSize(spid);
            return buffer[spid];
        }
        public AsyncTask SetValue(short spid, AsyncTask value)
        {
            SetSize(spid);
            buffer[spid] = value;
            return value;
        }
        public List<AsyncTask> GetAll()
        {
            List<AsyncTask> ret = new List<AsyncTask>();
            lock (SyncObject)
            {
                foreach (AsyncTask item in buffer)
                {
                    if (item != null)
                        ret.Add(item);
                }
            }
            return ret;
        }
    }
    public class AsyncTaskItem
    {
        public short SPID = -1;
        public SqlCommand Command = null;
        public string Status = "Pending", Name = "";
        public DateTime EnqueueDate = DateTime.Now;
        public DateTime ExecutionStartDate = DateTime.MaxValue;
        public DateTime EndDate = DateTime.MaxValue;
        public Exception Error;
    }
    public class AsyncTask
    {
        static AsyncSessionStore SessionTask = new AsyncSessionStore();

        object syncObj = new object();
        int mMaxThreads = 3;
        int runningThreads = 0;
        string Name;
        bool mIsClosed = false, CloseWhenException = true, ForceCancel = true;
        bool IsClosed
        {
            get { lock (syncObj) return mIsClosed; }
            set { lock (syncObj) mIsClosed = value; }
        }
        bool IsRunning
        {
            get
            {
                lock (syncObj)
                {
                    if (IsClosed)
                        return false;
                    return (waitingList.Count > 0) || (runningList.Count > 0) || (runningThreads > 0);
                }
            }
        }
        bool isClientMonitorReady = false;
        Exception mError;

        List<AsyncTaskItem> waitingList = new List<AsyncTaskItem>();
        List<AsyncTaskItem> runningList = new List<AsyncTaskItem>();
        List<AsyncTaskItem> completeList = new List<AsyncTaskItem>();



        string connectionStringPooled, connectionStringNonePooled;

        int MaxThreads { get { return mMaxThreads; } set { lock (syncObj) { mMaxThreads = value; } } }
        Exception Error
        {
            get { return mError; }
            set
            {
                lock (syncObj)
                {
                    mError = value;
                    if (CloseWhenException)
                        Close();
                }
            }
        }
        short CallerSpid;

        void SetItemStatus(AsyncTaskItem item, string status)
        {
            item.Status = status;
            if (status == "Running")
            {
                item.ExecutionStartDate = DateTime.Now;
            }
            else
            {
                item.EndDate = DateTime.Now;
            }
        }
        void StartClientManager()
        {
            (new Thread(() =>
            {
                try
                {
                    using (SqlConnection connection = new SqlConnection(connectionStringNonePooled))
                    {
                        connection.Open();
                        using (SqlCommand cmd = connection.CreateCommand())
                        {
                            cmd.CommandTimeout = 0;
                            cmd.CommandText = @"set xact_abort on
begin transaction
declare @Resource sysname
select @Resource = 'AsyncClient' + cast(@spid as nvarchar(20))
exec sp_getapplock @Resource, 'Exclusive', 'Transaction'
rollback";
                            cmd.Parameters.Add("@spid", SqlDbType.SmallInt).Value = CallerSpid;

                            isClientMonitorReady = true;
                            cmd.ExecuteNonQuery();
                        }
                    }
                }
                catch (Exception e)
                {
                    //main thread terminated
                    Error = e;
                    ForceCancel = true;
                }
                finally
                {
                    
                    Close();
                }
            })).Start();
        }
        public AsyncTask()
        {
            using (SqlConnection connection = GetContextConnection())
            {
                SqlCommand cmd = connection.CreateCommand();
                cmd.CommandText = @"set xact_abort on
set nocount on 
if @@trancount = 0
begin
	raiserror('Transaction is required.', 16, 1)
	return
end
declare @Resource sysname, @ret int
select @Resource = 'AsyncClient' + cast(@@SPID as nvarchar(20))
exec @ret = sp_getapplock @Resource, 'Exclusive', 'Transaction', 0
if @ret <0
begin
	raiserror('Could not acquire lock.', 16,1)
	return
end
select @@servername, db_name(), @@SPID";
                using (SqlDataReader reader = cmd.ExecuteReader())
                {
                    reader.Read();
                    CallerSpid = (short)reader[2];
                    connectionStringPooled = (new SqlConnectionStringBuilder()
                    {
                        Enlist = false,
                        InitialCatalog = reader[1].ToString(),
                        DataSource = reader[0].ToString(),
                        Pooling = true,
                        IntegratedSecurity = true,
                        ApplicationName = "AsyncTask Object"
                    }).ToString();
                    connectionStringNonePooled = (new SqlConnectionStringBuilder()
                    {
                        Enlist = false,
                        InitialCatalog = reader[1].ToString(),
                        DataSource = reader[0].ToString(),
                        Pooling = false,
                        IntegratedSecurity = true,
                        ApplicationName = "AsyncTask Management"
                    }).ToString();
                }
                StartClientManager();
                while (!isClientMonitorReady)
                {
                    Thread.Sleep(1);
                }
                if (IsClosed)
                {
                    if (Error != null)
                        throw Error;
                    else
                        throw new Exception("An unknown error occured.");
                }
                SessionTask.SetValue(CallerSpid, this);
                Thread.Sleep(1);
                StartWorker();
            }
        }
        void Close()
        {
            lock (syncObj)
            {
                if (IsClosed)
                    return;
                IsClosed = true;
                (new Thread(() =>
                {
                    lock (syncObj)
                    {
                        foreach (AsyncTaskItem item in waitingList)
                        {
                            SetItemStatus(item, "Aborted");
                            completeList.Add(item);
                        }
                        waitingList.Clear();
                        if (ForceCancel)
                        {
                            foreach (AsyncTaskItem item in runningList)
                            {
                                try
                                {
                                    item.Command.Cancel();
                                    item.Status = "Cancelled";
                                }
                                catch
                                {
                                }
                            }
                        }
                    }
                })).Start();
            }
        }
        void Enqueue(string commandText)
        {
            SqlCommand cmd = new SqlCommand();
            cmd.CommandText = commandText;
            cmd.CommandTimeout = 0;
            cmd.CommandType = CommandType.Text;
            lock (syncObj)
            {
                if (!IsClosed)
                    waitingList.Add(new AsyncTaskItem() { Command = cmd });
                else
                    completeList.Add(new AsyncTaskItem() { Command = cmd, Status = "Aborted", ExecutionStartDate = DateTime.Now, EndDate = DateTime.Now });
            }
        }
        void RunOne()
        {
            AsyncTaskItem item = null;
            try
            {
                lock (syncObj)
                {
                    if (waitingList.Count == 0)
                        return;
                    item = waitingList[0];
                    runningList.Add(item);
                    waitingList.Remove(item);
                }
                SetItemStatus(item, "Running");
                using (SqlConnection connection = new SqlConnection(connectionStringPooled))
                {
                    item.Command.Connection = connection;
                    if (IsClosed) SetItemStatus(item, "Aborted");
                    connection.Open();
                    item.SPID = GetSessionID(connection);
                    if (IsClosed)
                    {
                        SetItemStatus(item, "Aborted");
                        return;
                    }

                    item.Command.ExecuteNonQuery();
                    SetItemStatus(item, "Finished");
                }
            }
            catch (Exception e)
            {
                item.EndDate = DateTime.Now;
                if (e is SqlException)
                {
                    if (((SqlException)e).Number == 0)
                    {
                        //either session is killed or SQL Command is cancelled.
                        SetItemStatus(item, "Cancelled");

                        //Error = e;
                        //if (IsClosed)
                        //    SetItemStatus(item, "Cancelled");
                        //else
                        //    SetItemStatus(item, "Finished");
                        return;
                    }
                }
                SetItemStatus(item, "Error");
                Error = e;
                item.Error = e;
            }
            finally
            {
                lock (syncObj)
                {
                    runningThreads--;
                    if (item != null)
                    {
                        runningList.Remove(item);
                        completeList.Add(item);
                    }
                }
            }
        }
        void StartWorker()
        {
            (new Thread(() =>
            {
                while (!IsClosed)
                {
                    lock (syncObj)
                    {
                        if (runningThreads < MaxThreads)
                        {
                            runningThreads++;
                            (new Thread(RunOne)).Start();
                        }
                    }
                    while ((!IsClosed) && (waitingList.Count == 0)) Thread.Sleep(1);
                    while ((!IsClosed) && (runningThreads >= MaxThreads)) Thread.Sleep(1);
                }
            })).Start();

        }
        void Wait()
        {
            try
            {
                while (true)
                {
                    lock (syncObj)
                    {
                        if ((waitingList.Count == 0) && (runningList.Count == 0))
                            return;
                    }
                    Thread.Sleep(1);
                }
            }
            finally
            {
                if (CloseWhenException)
                {
                    if (Error != null)
                        throw Error;
                }
            }
        }
        static SqlConnection GetContextConnection()
        {
            SqlConnection connection = new SqlConnection("context connection=true");
            connection.Open();
            return connection;
        }
        static short GetSessionID(SqlConnection connection = null)
        {
            SqlConnection conn = connection;
            if (connection == null)
                conn = GetContextConnection();
            try
            {
                using (SqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "select @@spid";
                    return (short)cmd.ExecuteScalar();
                }
            }
            finally
            {
                if (connection == null)
                    conn.Close();
            }
        }
        static AsyncTask GetAsyncTask(bool create = true)
        {
            short spid = GetSessionID();
            AsyncTask ret = SessionTask.GetValue(spid);
            if (create)
            {
                if ((ret != null))
                {
                    if (ret.IsClosed)
                    {
                        SessionTask.SetValue(spid, null);
                        ret = null;
                    }
                }
                if ((ret == null))
                {
                    ret = new AsyncTask();
                    SessionTask.SetValue(ret.CallerSpid, ret);
                }
            }
            return ret;
        }

        [SqlProcedure]
        public static SqlInt32 AsyncCloseWhenException(SqlBoolean CloseWhenException, SqlBoolean ForceClose)
        {
            AsyncTask task = GetAsyncTask();
            if (task == null)
            {
                return new SqlInt32(-1);
            }
            task.ForceCancel = ForceClose.IsNull ? true : ForceClose.Value;
            task.CloseWhenException = CloseWhenException.Value;
            return new SqlInt32(0);
        }
        [SqlProcedure]
        public static SqlInt32 AsyncMaxThreads(SqlInt32 MaxThreads)
        {
            AsyncTask task = GetAsyncTask();
            if (task == null)
            {
                return new SqlInt32(-1);
            }
            task.MaxThreads = MaxThreads.Value;
            return new SqlInt32(0);
        }
        [SqlProcedure]
        public static SqlInt32 AsyncName(SqlString Name)
        {
            AsyncTask task = GetAsyncTask();
            if (task == null)
            {
                return new SqlInt32(-1);
            }
            task.Name = Name.IsNull ? "" : Name.Value;
            return new SqlInt32(0);
        }
        [SqlProcedure]
        public static SqlInt32 AsyncExecute(SqlChars Command)
        {
            AsyncTask task = GetAsyncTask();
            if (task == null)
            {
                return new SqlInt32(-1);
            }
            task.Enqueue(new string(Command.Value));
            return new SqlInt32(0);
        }
        [SqlProcedure]
        public static void AsyncWait()
        {
            AsyncTask task = GetAsyncTask(false);
            if (task == null)
            {
                return;
            }
            task.Wait();
        }
        [SqlProcedure]
        public static void AsyncClose(SqlInt16 spid, SqlBoolean ForceClose)
        {
            AsyncTask task;
            if (spid.IsNull)
                task = GetAsyncTask(false);
            else
                task = SessionTask.GetValue(spid.Value);
            if (task == null)
            {
                return;
            }
            task.ForceCancel = ForceClose.IsNull ? true : ForceClose.Value;
            task.Close();
        }
        [SqlFunction(
            FillRowMethodName = "AsyncTaskList_FillRow",
            TableDefinition = @"CallerSessionID smallint, MaxThreads int, CloseWhenException bit, RunningThreads int, WaitingTasks int, CompletedTasks int, IsClosed bit, Name nvarchar(4000), Error nvarchar(max)")
        ]
        public static IEnumerable AsyncTaskList()
        {
            return SessionTask.GetAll();
        }
        public static void AsyncTaskList_FillRow(object o, out SqlInt16 CallerSessionID, out SqlInt32 MaxThreads, out SqlBoolean CloseWhenException, out SqlInt32 RunningThreads,
            out SqlInt32 WaitingTasks, out SqlInt32 CompletedTasks, out SqlBoolean IsClosed, out SqlString Name, out SqlString Error)
        {
            AsyncTask t = (AsyncTask)o;
            CallerSessionID = new SqlInt16(t.CallerSpid);
            Name = t.Name == "" ? SqlString.Null : new SqlString(t.Name);
            MaxThreads = new SqlInt32(t.MaxThreads);
            RunningThreads = new SqlInt32(t.runningThreads);
            WaitingTasks = new SqlInt32(t.waitingList.Count);
            CompletedTasks = new SqlInt32(t.completeList.Count);
            IsClosed = new SqlBoolean(t.IsClosed);
            Error = t.Error == null ? SqlString.Null : t.Error.ToString();
            CloseWhenException = new SqlBoolean(t.CloseWhenException);
        }

        [SqlFunction(FillRowMethodName = "AsyncTaskStatus_FillRow", TableDefinition = "SessionID smallint, Status nvarchar(30), Command nvarchar(max), Error nvarchar(max), EnqueueDate datetime, ExecutionStartDate datetime, ExecutionEndDate datetime")]
        public static IEnumerable AsyncTaskStatus(SqlInt16 spid)
        {
            List<AsyncTaskItem> ret = new List<AsyncTaskItem>();
            AsyncTask t = SessionTask.GetValue(spid.Value);
            if (t != null)
            {
                lock (t.syncObj)
                {
                    ret.AddRange(t.waitingList);
                    ret.AddRange(t.runningList);
                    ret.AddRange(t.completeList);
                }
            }
            return ret;
        }
        public static void AsyncTaskStatus_FillRow(object o, out SqlInt16 SessionID, out SqlString Status, out SqlString Command, out SqlString Error, out SqlDateTime EnqueueDate, out SqlDateTime ExecutionStartDate, out SqlDateTime ExecutionEndDate)
        {
            AsyncTaskItem item = (AsyncTaskItem)o;
            SessionID = new SqlInt16(item.SPID);
            Status = new SqlString(item.Status);
            Command = new SqlString(item.Command.CommandText);
            Error = item.Error == null ? SqlString.Null : new SqlString(item.Error.ToString());
            EnqueueDate = new SqlDateTime(item.EnqueueDate);
            ExecutionStartDate = item.ExecutionStartDate == DateTime.MaxValue ? SqlDateTime.Null : new SqlDateTime(item.ExecutionStartDate);
            ExecutionEndDate = item.EndDate == DateTime.MaxValue ? SqlDateTime.Null : new SqlDateTime(item.EndDate);
        }
    }
}
