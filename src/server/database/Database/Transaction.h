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

#ifndef _TRANSACTION_H
#define _TRANSACTION_H

#include "SQLOperation.h"
#include "SQLSerializer.h"
#include "StringFormat.h"
#include <sqlpp11/serialize.h>
#include <sqlpp11/type_traits.h>

//- Forward declare (don't include header to prevent circular includes)
class PreparedStatement;

/*! Transactions, high level class. */
class TC_DATABASE_API Transaction
{
    friend class TransactionTask;
    friend class MySQLConnection;

    template <typename T>
    friend class DatabaseWorkerPool;

    public:
        Transaction(std::function<std::unique_ptr<SQLSerializer>()>&& querySerializer) : _querySerializer(std::move(querySerializer)), _cleanedUp(false) { }
        ~Transaction() { Cleanup(); }

        void Append(PreparedStatement* statement);
        void Append(const char* sql);
        template<typename Format, typename... Args>
        void PAppend(Format&& sql, Args&&... args)
        {
            Append(Trinity::StringFormat(std::forward<Format>(sql), std::forward<Args>(args)...).c_str());
        }
        template <typename TypedStatement>
        void TAppend(TypedStatement const& stmt)
        {
            _TAppend(stmt, sqlpp::run_check_t<MySQLSerializer, TypedStatement>{}, sqlpp::is_prepared_statement_t<TypedStatement>{});
        }

        size_t GetSize() const { return m_queries.size(); }

    protected:
        void Cleanup();
        std::list<SQLElementData> m_queries;

    private:
        template <typename TypedStatement>
        void _TAppend(TypedStatement const& s, ::sqlpp::consistent_t, std::false_type)
        {
            std::unique_ptr<SQLSerializer> context = _querySerializer();
            ::sqlpp::serialize(s, *context);
            Append(context->c_str());
        }

        template <typename TypedStatement>
        void _TAppend(TypedStatement const& s, ::sqlpp::consistent_t, std::true_type)
        {
            s._bind_params();
            Append(s._prepared_statement._stmt);
        }

        template <typename Check, typename TypedStatement, typename IsPrepared>
        void _TAppend(TypedStatement const&, Check, IsPrepared);

        std::function<std::unique_ptr<SQLSerializer>()> _querySerializer;
        bool _cleanedUp;
};

typedef std::shared_ptr<Transaction> SQLTransaction;

/*! Low level class*/
class TC_DATABASE_API TransactionTask : public SQLOperation
{
    template <class T> friend class DatabaseWorkerPool;
    friend class DatabaseWorker;

    public:
        TransactionTask(SQLTransaction trans) : m_trans(trans) { }
        ~TransactionTask() { }

    protected:
        bool Execute() override;

        SQLTransaction m_trans;
        static std::mutex _deadlockLock;
};

#endif
