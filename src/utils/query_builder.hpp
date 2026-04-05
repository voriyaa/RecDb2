#pragma once

#include <string>
#include <vector>

namespace recdb2::utils {

class QueryBuilder final {
   public:
    QueryBuilder& InsertInto(const std::string& table, const std::vector<std::string>& columns);
    QueryBuilder& Select(const std::vector<std::string>& exprs);
    QueryBuilder& From(const std::string& table);
    QueryBuilder& Where(const std::string& condition);
    QueryBuilder& AndWhere(const std::string& condition);
    QueryBuilder& GroupBy(const std::string& column);
    QueryBuilder& OrderBy(const std::string& column, const std::string& direction = "ASC");
    QueryBuilder& Limit(const std::string& param);
    QueryBuilder& Raw(const std::string& sql);

    std::string Build() const;

   private:
    std::string buffer_;
};

}  // namespace recdb2::utils