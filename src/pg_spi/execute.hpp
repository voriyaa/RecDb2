#pragma once

#include <array>
#include <cstdint>

extern "C" {
#include "postgres.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/elog.h"
}

#include "result_set.hpp"

namespace recdb2::pg_spi {

namespace {

inline Oid OidOf(const std::string&) {
    return TEXTOID;
}
inline Oid OidOf(const char*) {
    return TEXTOID;
}
inline Oid OidOf(const std::int64_t) {
    return INT8OID;
}
inline Oid OidOf(const int) {
    return INT4OID;
}
inline Oid OidOf(const double) {
    return FLOAT8OID;
}

inline Datum DatumOf(const std::string& value) {
    return CStringGetTextDatum(value.c_str());
}
inline Datum DatumOf(const char* value) {
    return CStringGetTextDatum(value);
}
inline Datum DatumOf(const std::int64_t value) {
    return Int64GetDatum(value);
}
inline Datum DatumOf(const int value) {
    return Int32GetDatum(value);
}
inline Datum DatumOf(const double value) {
    return Float8GetDatum(value);
}

template <typename T>
struct Param final {
    Oid oid{};
    Datum datum{};
    char is_null{' '};
};

template <typename T>
Param<T> MakeParam(const T& value) {
    return Param<T>{OidOf(value), DatumOf(value), ' '};
}

template <typename T>
Param<std::optional<T>> MakeParam(const std::optional<T>& value) {
    if (!value.has_value()) {
        return {
            .oid = OidOf(T{}),
            .datum = Datum{},
            .is_null = 'n',
        };
    }
    return {
        .oid = OidOf(value.value()),
        .datum = DatumOf(value.value()),
        .is_null = ' ',
    };
}

inline ResultSet CopyResultToOwned() {
    if (SPI_tuptable == nullptr) {
        return ResultSet{std::vector<Row>{}};
    }

    TupleDesc desc = SPI_tuptable->tupdesc;
    const std::size_t ncols = static_cast<std::size_t>(desc->natts);
    const std::size_t nrows = static_cast<std::size_t>(SPI_processed);

    std::vector<Row> rows;
    rows.reserve(nrows);

    for (std::size_t row = 0; row < nrows; ++row) {
        HeapTuple tuple = SPI_tuptable->vals[row];

        std::vector<Field> fields;
        fields.reserve(ncols);

        for (std::size_t col = 1; col <= ncols; ++col) {
            bool is_null = false;
            (void)SPI_getbinval(tuple, desc, static_cast<int>(col), &is_null);

            if (is_null) {
                fields.emplace_back(Field{std::nullopt});
                continue;
            }

            char* s = SPI_getvalue(tuple, desc, static_cast<int>(col));
            if (!s) {
                ereport(ERROR, (errmsg("recdb2: SPI_getvalue failed")));
            }

            std::string owned{s};
            pfree(s);
            fields.emplace_back(Field{std::optional<std::string>{std::move(owned)}});
        }

        rows.emplace_back(Row{std::move(fields)});
    }

    return ResultSet{std::move(rows)};
}

}  // namespace

template <typename... Args>
ResultSet Execute(const char* query, const Args&... args) {
    constexpr int kN = sizeof...(Args);

    std::array<Oid, kN> argtypes{};
    std::array<Datum, kN> values{};
    std::array<char, kN> nulls{};

    int i = 0;
    auto push = [&](const auto& a) {
        auto param = MakeParam(a);
        argtypes[i] = param.oid;
        values[i] = param.datum;
        nulls[i] = param.is_null;
        ++i;
    };

    (push(args), ...);

    const int rc =
        SPI_execute_with_args(query, kN, argtypes.data(), values.data(), nulls.data(), false, 0);

    if (rc < 0) {
        ereport(ERROR, (errmsg("recdb2: SPI_execute_with_args failed, rc=%d", rc)));
    }

    return CopyResultToOwned();
}

}  // namespace recdb2::pg_spi