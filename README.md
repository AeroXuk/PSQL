The goal of this project is to provide a simple way to run T-SQL batch in parallel, monitor, and operate executing processes from the same or other sessions.

## **Highlights**
* Simple way to run SQLs in parallel
* Termination of main session (the launcher session) cancels all executing SQLs and skips all waiting SQLs.
* Support different ways to shutdown executing asynchronous batches
* Adjustable maximum threads in an asynchronous batch
* Effective monitoring tools

## **Run T-SQL in Parallel**
Download and install the code into your SQL Server and let's get started with an example
```sql
set xact_abort on
begin transaction
exec dbo.AsyncName @Name = 'Test'
exec dbo.AsyncMaxThreads @MaxThreads = 3
----Start: those queries will be executed by 3 threads
exec dbo.AsyncExecute @Command = 'waitfor delay ''00:00:40'''
exec dbo.AsyncExecute @Command = 'waitfor delay ''00:00:30'''
exec dbo.AsyncExecute @Command = 'waitfor delay ''00:00:20'''
exec dbo.AsyncExecute @Command = 'waitfor delay ''00:00:10'''
exec dbo.AsyncWait -- wait commands above to complete
commit -- async batch complete
```
The main session is responsible for starting an asynchronous batch, setting execution configurations, queuing tasks, waiting for batch completion, and stopping a batch. 
```sql
set xact_abort on
begin transaction
--Code here...
commit
```
An asynchronous batch must be started within a transaction. The transaction will not be propagated in to any asynchronous commands. When transaction is completed (commit or rollback), the batch will stop automatically. Currently executing asynchronous commands will be cancelled and waiting commands will be aborted. 
It's recommended to set **xact_abort** on. This guarantees that when the main session is cancelled/killed, transaction will be rolled back automatically, hence batch will be cancelled.
**AsyncName**: Optionally you can give current batch a name.
**AsyncMaxThreads**: Set max number of concurrent worker threads
**AsyncExecute**: enqueue a T-SQL command. As long as a command is enqueued, it will be started immediately when there is an available worker threads
**AsyncWait**: this procedure will wait all worker threads to complete.
One more example
```sql
set xact_abort on
begin transaction
exec dbo.AsyncName @Name = 'Test-1'
exec dbo.AsyncMaxThreads @MaxThreads = 3
----Start: those queries will be executed by 3 threads
exec dbo.AsyncExecute @Command = 'waitfor delay ''00:00:40'''
exec dbo.AsyncExecute @Command = 'waitfor delay ''00:00:30'''
exec dbo.AsyncExecute @Command = 'waitfor delay ''00:00:20'''
exec dbo.AsyncExecute @Command = 'waitfor delay ''00:00:10'''
exec dbo.AsyncWait -- wait commands above to complete
--commit
--begin transaction
exec dbo.AsyncName @Name = 'Test-2'
exec dbo.AsyncMaxThreads @MaxThreads = 2
----Start: those queries will be executed by 2 threads
exec dbo.AsyncExecute @Command = 'waitfor delay ''00:00:35'''
exec dbo.AsyncExecute @Command = 'waitfor delay ''00:00:25'''
exec dbo.AsyncExecute @Command = 'waitfor delay ''00:00:15'''
exec dbo.AsyncWait -- wait commands above to complete
commit -- async batch complete
```
Batch name and max threads can be changed any time before a batch is finished. In example above, First 4 asynchronous commands will be run concurrently by 3 threads. After they are done, last 3 commands will be executed by 2 threads.
To splitting this batch into two, you can simply add **commit** after the first **AsyncWait** invocation and add **begin transaction** right after the commit statement you just added. This will not make difference in terms of asynchronous command executions. But it will show differently in the monitor.
## **Monitoring**
While the first demo is running, run following query in a separate session
```sql
select * from dbo.AsyncTaskList()
```
![](http://www.sqlnotes.info/wp-content/uploads/2015/11/PSQL-Result100.png)
Pass the main session id into **AsyncTaskStatus** function to get more details. 
```sql
select * from dbo.AsyncTaskStatus(53)
```
![](http://www.sqlnotes.info/wp-content/uploads/2015/11/PSQL-Result200.png)
Function **AsyncTaskStatus** will give you the statuses of each command in a asynchronous batch. This structure will stay in memory and attached to the main session id (53 in this case) until the next batch gets started(in session 53).  
## **More Supporting Procedures**
* **AsyncCloseWhenException @CloseWhenException, @ForceClose**
This procedure should be run within main session. It defines how the batch behave when there is an exception in one of the worker thread.
When **@CloseWhenException** is 0, exceptions in worker threads will **not terminate** the main session.
When **@CloseWhenException** is 1, exceptions in any worker threads will **terminate** the main session.
**@ForceClose** is applicable when **@CloseWhenException=1**. It tells the framework how to terminate the running workers. The commands in waiting list will be aborted anyways.
When **@ForceClose** is 0, main session will exit until all running workers complete
When **@ForceClose** is 1, running workers will be cancelled. Main session will return once the command cancellation is done
* **AsyncClose @spid, @ForceClose**
This procedure allows user to terminate a running asynchronous batch. The commands enlisted in the batch but not started yet will be aborted.
**@spid**: main session's session id.
When **@ForceClose** is 0, main session will exit until all running workers complete
When **@ForceClose** is 1, running workers will be cancelled. Main session will return once the command cancellation is done.
## **More Coming**
* Plan to support enlisting parameterized  commands
* Plan to support call back events. For instance, a procedure can be  configured and called before a command is started.
* Unload an executing or waiting task.
* Task retry
* Any other ideas will be more than welcome.