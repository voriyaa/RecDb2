#include "query_builder.hpp"

namespace recdb2::utils {

QueryBuilder& QueryBuilder::InsertInto(const std::string& table,
                                       const std::vector<std::string>& columns) {
    buffer_ += "INSERT INTO " + table + "(";
    for (std::size_t i = 0; i < columns.size(); ++i) {
        if (i > 0)
            buffer_ += ", ";
        buffer_ += columns[i];
    }
    buffer_ += ") ";
    return *this;
}

QueryBuilder& QueryBuilder::Select(const std::vector<std::string>& exprs) {
    buffer_ += "SELECT ";
    for (std::size_t i = 0; i < exprs.size(); ++i) {
        if (i > 0)
            buffer_ += ", ";
        buffer_ += exprs[i];
    }
    buffer_ += " ";
    return *this;
}

QueryBuilder& QueryBuilder::From(const std::string& table) {
    buffer_ += "FROM " + table + " ";
    return *this;
}

QueryBuilder& QueryBuilder::Where(const std::string& condition) {
    buffer_ += "WHERE " + condition + " ";
    return *this;
}

QueryBuilder& QueryBuilder::AndWhere(const std::string& condition) {
    buffer_ += "AND " + condition + " ";
    return *this;
}

QueryBuilder& QueryBuilder::GroupBy(const std::string& column) {
    buffer_ += "GROUP BY " + column + " ";
    return *this;
}

QueryBuilder& QueryBuilder::OrderBy(const std::string& column, const std::string& direction) {
    buffer_ += "ORDER BY " + column + " " + direction + " ";
    return *this;
}

QueryBuilder& QueryBuilder::Limit(const std::string& param) {
    buffer_ += "LIMIT " + param + " ";
    return *this;
}

QueryBuilder& QueryBuilder::Raw(const std::string& sql) {
    buffer_ += sql + " ";
    return *this;
}

std::string QueryBuilder::Build() const {
    return buffer_;
}

}  // namespace recdb2::utils