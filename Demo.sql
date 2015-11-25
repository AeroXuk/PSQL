set xact_abort on --- required
begin transaction -- management session start
exec dbo.AsyncName 'Test' -- give it a name
exec dbo.AsyncMaxThreads 3 --  set max worker threads
exec dbo.AsyncCloseWhenException 1  -- when this value is one, exception in a query will cause failure of entire batch. When it's 0, fail queries will not stop entire batch.
----Start: those queries will be executed by 3 threads
exec dbo.AsyncExecute 'waitfor delay ''00:00:20'''
exec dbo.AsyncExecute 'waitfor delay ''00:00:19'''
exec dbo.AsyncExecute 'waitfor delay ''00:00:18'''
exec dbo.AsyncExecute 'waitfor delay ''00:00:17'''
--exec dbo.AsyncExecute 'raiserror(''error'', 16, 1)'
exec dbo.AsyncExecute 'waitfor delay ''00:00:16'''
exec dbo.AsyncExecute 'waitfor delay ''00:00:15'''
exec dbo.AsyncExecute 'waitfor delay ''00:00:14'''
exec dbo.AsyncExecute 'waitfor delay ''00:00:13'''
exec dbo.AsyncExecute 'waitfor delay ''00:00:12'''
exec dbo.AsyncExecute 'waitfor delay ''00:00:11'''
exec dbo.AsyncExecute 'waitfor delay ''00:00:10'''
----End: those queries will be executed by 3 threads
exec dbo.AsyncWait -- wait until all queries above to finish
exec dbo.AsyncName 'Test1' -- change the name
exec dbo.AsyncMaxThreads 2 -- change max worker threads
----Start: those queries will be executed by 2 threads
exec dbo.AsyncExecute 'waitfor delay ''00:00:20'''
exec dbo.AsyncExecute 'waitfor delay ''00:00:19'''
exec dbo.AsyncExecute 'waitfor delay ''00:00:18'''
exec dbo.AsyncExecute 'waitfor delay ''00:00:17'''
----End: those queries will be executed by 2 threads
exec dbo.AsyncWait -- wait
commit -- management session end

--Start another windows for monitoring
/*

select * from dbo.AsyncTaskList()
select * from dbo.AsyncTaskStatus(53) -- pass management session_id


*/