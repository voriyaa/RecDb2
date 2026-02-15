#pragma once

#include <optional>
#include <string>
#include <vector>

namespace recdb2::pg_spi {

class Field final {
public:
    Field() = default;

    explicit Field(std::optional<std::string>&& value);

    bool IsNull() const;
    std::string_view AsStringView() const;

    template <typename T>
    T As() const;
private:
    std::optional<std::string> value_;
};

class Row final {
public:
    explicit Row(std::vector<Field>&& fields);

    std::size_t Size() const;
    const Field& operator[](std::size_t col) const;

private:
    std::vector<Field> fields_;
};

class ResultSet final {
public:
    explicit ResultSet(std::vector<Row>&& rows);

    std::size_t Size() const;
    bool IsEmpty() const;

    const Row& operator[](std::size_t i) const;

    std::vector<Row>::const_iterator begin() const;
    std::vector<Row>::const_iterator end() const;

    const Row& SingleRow() const;
    std::optional<Row> OptionalSingleRow() const;

private:
    std::vector<Row> rows_;
};

} // namespace recdb2::pg_spi