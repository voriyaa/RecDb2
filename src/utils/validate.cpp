#include "validate.hpp"

extern "C" {
#include "postgres.h"
#include "utils/elog.h"
}

namespace recdb2::utils {

void ValidateIdentifier(const std::string& name, const char* label) {
    for (char c : name) {
        if (!std::isalnum(static_cast<unsigned char>(c)) && c != '_') {
            ereport(ERROR, (errmsg("recdb2: invalid character in %s: '%s'", label, name.c_str())));
        }
    }
}

}  // namespace recdb2::utils