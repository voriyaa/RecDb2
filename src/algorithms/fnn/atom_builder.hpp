#pragma once

#include "config.hpp"
#include "model.hpp"

#include <string>
#include <vector>

namespace recdb2::algorithm::fnn {

std::vector<AtomDef> BuildResolvedAtoms(const FnnConfig& cfg);

std::string AggregateExprFor(const AtomDef& a);

std::string AtomSqlExpression(const AtomDef& a, const std::string& items_alias,
                               const std::string& users_alias);

}  // namespace recdb2::algorithm::fnn
