/*
 * Copyright (C) 2008-2016 TrinityCore <http://www.trinitycore.org/>
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or (at your
 * option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef _DATABASEWORKERPOOL_H
#define _DATABASEWORKERPOOL_H

#include "Common.h"
#include "QueryCallback.h"
#include "MySQLConnection.h"
#include "Transaction.h"
#include "DatabaseWorker.h"
#include "PreparedStatement.h"
#include "Log.h"
#include "QueryResult.h"
#include "QueryHolder.h"
#include "AdhocStatement.h"
#include "StringFormat.h"

#include <sqlpp11/sqlpp11.h>
#include <mysqld_error.h>
#include <memory>
#include <array>

class PingOperation : public SQLOperation
{
    //! Operation for idle delaythreads
    bool Execute() override
    {
        m_conn->Ping();
        return true;
    }
};

template <class T>
class DatabaseWorkerPool : public sqlpp::connection
{
    private:
        enum InternalIndex
        {
            IDX_ASYNC,
            IDX_SYNCH,
            IDX_SIZE
        };

    public:
        /* Activity state */
        DatabaseWorkerPool();

        ~DatabaseWorkerPool()
        {
            _queue->Cancel();
        }

        void SetConnectionInfo(std::string const& infoString, uint8 const asyncThreads, uint8 const synchThreads);

        uint32 Open();

        void Close();

        //! Prepares all prepared statements
        bool PrepareStatements();

        inline MySQLConnectionInfo const* GetConnectionInfo() const
        {
            return _connectionInfo.get();
        }

        /**
            Delayed one-way statement methods.
        */

        //! Enqueues a one-way SQL operation in string format that will be executed asynchronously.
        //! This method should only be used for queries that are only executed once, e.g during startup.
        void Execute(const char* sql)
        {
            if (Trinity::IsFormatEmptyOrNull(sql))
                return;

            BasicStatementTask* task = new BasicStatementTask(sql);
            Enqueue(task);
        }

        //! Enqueues a one-way SQL operation in string format -with variable args- that will be executed asynchronously.
        //! This method should only be used for queries that are only executed once, e.g during startup.
        template<typename Format, typename... Args>
        void PExecute(Format&& sql, Args&&... args)
        {
            if (Trinity::IsFormatEmptyOrNull(sql))
                return;

            Execute(Trinity::StringFormat(std::forward<Format>(sql), std::forward<Args>(args)...).c_str());
        }

        //! Enqueues a one-way SQL operation in prepared statement format that will be executed asynchronously.
        //! Statement must be prepared with CONNECTION_ASYNC flag.
        void Execute(PreparedStatement* stmt)
        {
            PreparedStatementTask* task = new PreparedStatementTask(stmt);
            Enqueue(task);
        }

        /**
            Direct synchronous one-way statement methods.
        */

        //! Directly executes a one-way SQL operation in string format, that will block the calling thread until finished.
        //! This method should only be used for queries that are only executed once, e.g during startup.
        void DirectExecute(const char* sql)
        {
            if (Trinity::IsFormatEmptyOrNull(sql))
                return;

            T* connection = GetFreeConnection();
            connection->Execute(sql);
            connection->Unlock();
        }

        //! Directly executes a one-way SQL operation in string format -with variable args-, that will block the calling thread until finished.
        //! This method should only be used for queries that are only executed once, e.g during startup.
        template<typename Format, typename... Args>
        void DirectPExecute(Format&& sql, Args&&... args)
        {
            if (Trinity::IsFormatEmptyOrNull(sql))
                return;

            DirectExecute(Trinity::StringFormat(std::forward<Format>(sql), std::forward<Args>(args)...).c_str());
        }

        //! Directly executes a one-way SQL operation in prepared statement format, that will block the calling thread until finished.
        //! Statement must be prepared with the CONNECTION_SYNCH flag.
        void DirectExecute(PreparedStatement* stmt)
        {
            T* connection = GetFreeConnection();
            connection->Execute(stmt);
            connection->Unlock();

            //! Delete proxy-class. Not needed anymore
            delete stmt;
        }

        /**
            Synchronous query (with resultset) methods.
        */

        //! Directly executes an SQL query in string format that will block the calling thread until finished.
        //! Returns reference counted auto pointer, no need for manual memory management in upper level code.
        QueryResult Query(const char* sql, T* connection = nullptr);

        //! Directly executes an SQL query in string format -with variable args- that will block the calling thread until finished.
        //! Returns reference counted auto pointer, no need for manual memory management in upper level code.
        template<typename Format, typename... Args>
        QueryResult PQuery(Format&& sql, T* conn, Args&&... args)
        {
            if (Trinity::IsFormatEmptyOrNull(sql))
                return QueryResult(nullptr);

            return Query(Trinity::StringFormat(std::forward<Format>(sql), std::forward<Args>(args)...).c_str(), conn);
        }

        //! Directly executes an SQL query in string format -with variable args- that will block the calling thread until finished.
        //! Returns reference counted auto pointer, no need for manual memory management in upper level code.
        template<typename Format, typename... Args>
        QueryResult PQuery(Format&& sql, Args&&... args)
        {
            if (Trinity::IsFormatEmptyOrNull(sql))
                return QueryResult(nullptr);

            return Query(Trinity::StringFormat(std::forward<Format>(sql), std::forward<Args>(args)...).c_str());
        }

        //! Directly executes an SQL query in prepared format that will block the calling thread until finished.
        //! Returns reference counted auto pointer, no need for manual memory management in upper level code.
        //! Statement must be prepared with CONNECTION_SYNCH flag.
        PreparedQueryResult Query(PreparedStatement* stmt);

        /**
            Asynchronous query (with resultset) methods.
        */

        //! Enqueues a query in string format that will set the value of the QueryResultFuture return object as soon as the query is executed.
        //! The return value is then processed in ProcessQueryCallback methods.
        QueryResultFuture AsyncQuery(const char* sql);

        //! Enqueues a query in string format -with variable args- that will set the value of the QueryResultFuture return object as soon as the query is executed.
        //! The return value is then processed in ProcessQueryCallback methods.
        template<typename Format, typename... Args>
        QueryResultFuture AsyncPQuery(Format&& sql, Args&&... args)
        {
            return AsyncQuery(Trinity::StringFormat(std::forward<Format>(sql), std::forward<Args>(args)...).c_str());
        }

        //! Enqueues a query in prepared format that will set the value of the PreparedQueryResultFuture return object as soon as the query is executed.
        //! The return value is then processed in ProcessQueryCallback methods.
        //! Statement must be prepared with CONNECTION_ASYNC flag.
        PreparedQueryResultFuture AsyncQuery(PreparedStatement* stmt);

        //! Enqueues a vector of SQL operations (can be both adhoc and prepared) that will set the value of the QueryResultHolderFuture
        //! return object as soon as the query is executed.
        //! The return value is then processed in ProcessQueryCallback methods.
        //! Any prepared statements added to this holder need to be prepared with the CONNECTION_ASYNC flag.
        QueryResultHolderFuture DelayQueryHolder(SQLQueryHolder* holder);

        /**
            Transaction context methods.
        */

        //! Begins an automanaged transaction pointer that will automatically rollback if not commited. (Autocommit=0)
        SQLTransaction BeginTransaction()
        {
            return SQLTransaction(new Transaction([this]() -> std::unique_ptr<SQLSerializer> { return Trinity::make_unique<MySQLSerializer>(_connections[IDX_SYNCH].front().get()); }));
        }

        //! Enqueues a collection of one-way SQL operations (can be both adhoc and prepared). The order in which these operations
        //! were appended to the transaction will be respected during execution.
        void CommitTransaction(SQLTransaction transaction);

        //! Directly executes a collection of one-way SQL operations (can be both adhoc and prepared). The order in which these operations
        //! were appended to the transaction will be respected during execution.
        void DirectCommitTransaction(SQLTransaction& transaction);

        //! Method used to execute prepared statements in a diverse context.
        //! Will be wrapped in a transaction if valid object is present, otherwise executed standalone.
        void ExecuteOrAppend(SQLTransaction& trans, PreparedStatement* stmt)
        {
            if (!trans)
                Execute(stmt);
            else
                trans->Append(stmt);
        }

        //! Method used to execute ad-hoc statements in a diverse context.
        //! Will be wrapped in a transaction if valid object is present, otherwise executed standalone.
        void ExecuteOrAppend(SQLTransaction& trans, const char* sql)
        {
            if (!trans)
                Execute(sql);
            else
                trans->Append(sql);
        }

        /**
            Other
        */

        typedef typename T::Statements PreparedStatementIndex;

        //! Automanaged (internally) pointer to a prepared statement object for usage in upper level code.
        //! Pointer is deleted in this->DirectExecute(PreparedStatement*), this->Query(PreparedStatement*) or PreparedStatementTask::~PreparedStatementTask.
        //! This object is not tied to the prepared statement on the MySQL context yet until execution.
        PreparedStatement* GetPreparedStatement(PreparedStatementIndex index)
        {
            return new PreparedStatement(index);
        }

        //! Apply escape string'ing for current collation. (utf8)
        void EscapeString(std::string& str)
        {
            _connections[IDX_SYNCH].front()->EscapeString(str);
        }

        //! Keeps all our MySQL connections alive, prevent the server from disconnecting us.
        void KeepAlive();

        using _prepared_statement_t = TypedPreparedStatement;
        using _serializer_context_t = MySQLSerializer;
        using _interpreter_context_t = MySQLSerializer;

        struct _tags
        {
            using _null_result_is_trivial_value = std::true_type;
        };

        template <typename Statement>
        static MySQLSerializer& _serialize_interpretable(Statement const& statement, MySQLSerializer& context)
        {
            return ::sqlpp::serialize(statement, context);
        }

        template <typename Statement>
        static MySQLSerializer& _interpret_interpretable(Statement const& statement, MySQLSerializer& context)
        {
            return ::sqlpp::serialize(statement, context);
        }

        // type hint
        template<typename Select> TypedQueryResult select(Select const&);
        template<typename PreparedSelect> TypedPreparedQueryResult run_prepared_select(PreparedSelect const&);

        template <typename Select>
        auto TQuery(Select const& s) -> decltype(this->_TQuery(s, sqlpp::run_check_t<MySQLSerializer, Select>{}, sqlpp::is_prepared_statement_t<Select>{}))
        {
            return _TQuery(s, sqlpp::run_check_t<MySQLSerializer, Select>{}, sqlpp::is_prepared_statement_t<Select>{});
        }

        template <typename ExecuteStatement>
        void TExecute(ExecuteStatement const& q)
        {
            _TExecute(q, sqlpp::run_check_t<MySQLSerializer, ExecuteStatement>{}, sqlpp::is_prepared_statement_t<ExecuteStatement>{});
        }

        template <typename ExecuteStatement>
        void DirectTExecute(ExecuteStatement const& e)
        {
            _DirectTExecute(e, sqlpp::run_check_t<MySQLSerializer, ExecuteStatement>{}, sqlpp::is_prepared_statement_t<ExecuteStatement>{});
        }

        template <typename Statement>
        auto TGetPreparedStatement() -> decltype(std::declval<Statement::StatementType>()._prepare(*this))
        {
            using index = typename Statement::Index;
            using StatementType = decltype(std::declval<Statement::StatementType>()._prepare(*this));
            StatementType stmt;
            stmt._prepared_statement._stmt = GetPreparedStatement(index::value);
            return stmt;
        }

    private:
        uint32 OpenConnections(InternalIndex type, uint8 numConnections);

        void Enqueue(SQLOperation* op)
        {
            _queue->Push(op);
        }

        //! Gets a free connection in the synchronous connection pool.
        //! Caller MUST call t->Unlock() after touching the MySQL context to prevent deadlocks.
        T* GetFreeConnection();

        char const* GetDatabaseName() const
        {
            return _connectionInfo->database.c_str();
        }

        template <typename Select>
        auto _TQuery(Select const& s, ::sqlpp::consistent_t, std::false_type) -> decltype(s._run(*this))
        {
            MySQLSerializer context(_connections[IDX_SYNCH].front().get());
            ::sqlpp::serialize(s, context);
            return{ TypedQueryResult(Query(context.c_str())), s.get_dynamic_names() };
        }

        template <typename PreparedSelect>
        auto _TQuery(PreparedSelect const& s, ::sqlpp::consistent_t, std::true_type) -> decltype(s._run(*this))
        {
            s._bind_params();
            return{ TypedPreparedQueryResult(Query(s._prepared_statement._stmt)), s._dynamic_names };
        }

        template <typename Check, typename Select, typename IsPrepared>
        auto _TQuery(Select const&, Check, IsPrepared) -> Check;

        template <typename ExecuteStatement>
        void _TExecute(ExecuteStatement const& e, ::sqlpp::consistent_t, std::false_type)
        {
            MySQLSerializer context(_connections[IDX_SYNCH].front().get());
            ::sqlpp::serialize(e, context);
            Execute(context.c_str());
        }

        template <typename PreparedExecuteStatement>
        void _TExecute(PreparedExecuteStatement const& e, ::sqlpp::consistent_t, std::true_type)
        {
            s._bind_params();
            Execute(e._stmt);
        }

        template <typename Check, typename ExecuteStatement, typename IsPrepared>
        void _TExecute(ExecuteStatement const&, Check, IsPrepared);

        template <typename ExecuteStatement>
        void _DirectTExecute(ExecuteStatement const& e, ::sqlpp::consistent_t, std::false_type)
        {
            MySQLSerializer context(_connections[IDX_SYNCH].front().get());
            ::sqlpp::serialize(e, context);
            DirectExecute(context.c_str());
        }

        template <typename PreparedExecuteStatement>
        void _DirectTExecute(PreparedExecuteStatement const& e, ::sqlpp::consistent_t, std::true_type)
        {
            s._bind_params();
            Execute(e._stmt);
        }

        template <typename Check, typename ExecuteStatement, typename IsPrepared>
        void _DirectTExecute(ExecuteStatement const&, Check, IsPrepared);

        //! Queue shared by async worker threads.
        std::unique_ptr<ProducerConsumerQueue<SQLOperation*>> _queue;
        std::array<std::vector<std::unique_ptr<T>>, IDX_SIZE> _connections;
        std::unique_ptr<MySQLConnectionInfo> _connectionInfo;
        uint8 _async_threads, _synch_threads;
};

#endif
