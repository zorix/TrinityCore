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

#ifndef MySQLSerializer_h__
#define MySQLSerializer_h__

#include "StringFormat.h"
#include "Common.h"

class TC_DATABASE_API SQLSerializer
{
public:
    template <typename Fragment>
    fmt::BasicWriter<char>& operator<<(Fragment&& fragment) { return _writer.operator<<(std::forward<Fragment>(fragment)); }

    virtual std::string& escape(std::string& arg) = 0;

    char const* c_str() const;

private:
    fmt::MemoryWriter _writer;
};

class MySQLConnection;

class TC_DATABASE_API MySQLSerializer : public SQLSerializer
{
public:
    MySQLSerializer(MySQLConnection* connection);

    std::string& escape(std::string& arg) override;

private:
    MySQLConnection* _connection;
};

#endif // MySQLSerializer_h__
