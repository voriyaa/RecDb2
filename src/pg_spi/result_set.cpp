#include "result_set.hpp"

#include <charconv>
#include <cstdint>
#include <cstdlib>

extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}

namespace recdb2::pg_spi {

Field::Field(std::optional<std::string>&& value) : value_(std::move(value)) {}

bool Field::IsNull() const {
    return !value_.has_value();
}

std::string_view Field::AsStringView() const {
    if (IsNull()) {
        ereport(ERROR, (errmsg("recdb2: field is NULL")));
    }
    return value_.value();
}

Row::Row(std::vector<Field>&& fields) : fields_(std::move(fields)) {}

std::size_t Row::Size() const {
    return fields_.size();
}

const Field& Row::operator[](std::size_t col) const {
    const auto fields_size = Size();

    if (col >= fields_size) {
        ereport(ERROR,
                (errmsg("recdb2: column index out of range (col=%lu size=%lu)", col, fields_size)));
    }
    return fields_[col];
}

ResultSet::ResultSet(std::vector<Row>&& rows) : rows_(std::move(rows)) {}

std::size_t ResultSet::Size() const {
    return rows_.size();
}

bool ResultSet::IsEmpty() const {
    return rows_.empty();
}

const Row& ResultSet::operator[](std::size_t i) const {
    const auto rows_size = Size();

    if (i >= rows_size) {
        ereport(ERROR, (errmsg("recdb2: row index out of range (row=%lu size=%lu)", i, rows_size)));
    }

    return rows_[i];
}

std::vector<Row>::const_iterator ResultSet::begin() const {
    return rows_.begin();
}

std::vector<Row>::const_iterator ResultSet::end() const {
    return rows_.end();
}

const Row& ResultSet::SingleRow() const {
    const auto rows_size = Size();

    if (rows_size != 1) {
        ereport(ERROR, (errmsg("recdb2: expected single row, got %lu", rows_size)));
    }

    return rows_[0];
}

std::optional<Row> ResultSet::OptionalSingleRow() const {
    if (IsEmpty()) {
        return std::nullopt;
    }

    const auto rows_size = Size();

    if (rows_size > 1) {
        ereport(ERROR, (errmsg("recdb2: expected 0 or 1 row, got %lu", rows_size)));
    }

    return rows_[0];
}

template <>
std::string Field::As<std::string>() const {
    return std::string(AsStringView());
}

template <>
std::int64_t Field::As<std::int64_t>() const {
    const auto sv = AsStringView();
    std::int64_t x = 0;
    auto b = sv.data();
    auto e = sv.data() + sv.size();

    auto [ptr, ec] = std::from_chars(b, e, x);
    if (ec != std::errc{} || ptr != e) {
        ereport(ERROR, (errmsg("recdb2: failed to parse int64")));
    }

    return x;
}

template <>
double Field::As<double>() const {
    const auto sv = AsStringView();
    char* end = nullptr;
    double val = std::strtod(sv.data(), &end);
    if (end != sv.data() + sv.size()) {
        ereport(ERROR, (errmsg("recdb2: failed to parse double")));
    }
    return val;
}

}  // namespace recdb2::pg_spi
