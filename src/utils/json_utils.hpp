#pragma once

#include <optional>
#include <string>

namespace recdb2::utils {

std::optional<std::string> JsonGetOptional(const std::string& json_text, const char* key);

std::string JsonGet(const std::string& json_text, const char* key);

}  // namespace recdb2::utils