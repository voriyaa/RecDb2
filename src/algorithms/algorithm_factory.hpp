#pragma once

#include "popularity.hpp"
#include <memory>

extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}

namespace recdb2::algorithm {

inline std::unique_ptr<Algorithm> CreateAlgorithm(const std::string& name) {
    if (name == "popularity") {
        return std::make_unique<PopularityAlgorithm>();
    }

    ereport(ERROR, (errmsg("recdb2: unknown algorithm '%s'", name.c_str())));
    __builtin_unreachable();
}

}  // namespace recdb2::algorithm