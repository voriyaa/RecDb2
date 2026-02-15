extern "C" {

#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(recdb2_hello);
Datum recdb2_hello(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(recdb2_spi_ping);
Datum recdb2_spi_ping(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(recdb2_create_recommender);
Datum recdb2_create_recommender(PG_FUNCTION_ARGS);
}

#include "../core/service.hpp"
#include "../pg_spi/spi_session.hpp"

Datum recdb2_hello(PG_FUNCTION_ARGS) {
    recdb2::core::RecommenderService svc;
    const std::string message = svc.Hello();
    PG_RETURN_TEXT_P(cstring_to_text(message.c_str()));
}

Datum recdb2_spi_ping(PG_FUNCTION_ARGS) {
    recdb2::pg_spi::SpiSession::RunVoid([]() {});
    PG_RETURN_TEXT_P(cstring_to_text("recdb2: SPI ok"));
}

Datum recdb2_create_recommender(PG_FUNCTION_ARGS) {
    text* name_text = PG_GETARG_TEXT_PP(0);
    text* algo_text = PG_GETARG_TEXT_PP(1);
    text* cfg_text = PG_GETARG_TEXT_PP(2);

    char* name_c = text_to_cstring(name_text);
    char* algo_c = text_to_cstring(algo_text);
    char* cfg_c = text_to_cstring(cfg_text);

    recdb2::core::RecommenderService svc;
    const std::int64_t id = svc.CreateRecommender(name_c, algo_c, cfg_c);

    PG_RETURN_INT64(id);
}
