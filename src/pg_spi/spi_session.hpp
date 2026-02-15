#pragma once

extern "C" {
    #include "postgres.h"
    #include "executor/spi.h"
    #include "utils/elog.h"
}

namespace recdb2::pg_spi {

namespace {

static constexpr auto kSpiConnectFailed = "recdb2: SPI_connect failed";
static constexpr auto kSpiFinishFailed = "recdb:2 SPI_finish failed";

} // namespace

class SpiSession final {
public:
    template<typename F>
    static void RunVoid(F&& f) {
        if (SPI_connect() != SPI_OK_CONNECT) {
            ereport(ERROR, (errmsg(kSpiConnectFailed)));
        }

        PG_TRY();
        {
            f();
            if (SPI_finish() != SPI_OK_FINISH) {
                ereport(ERROR, (errmsg(kSpiFinishFailed)));
            }
        }

        PG_CATCH();
        {
            SPI_finish();
            PG_RE_THROW();
        }

        PG_END_TRY();
    }

    template<typename F>
    static auto Run(F&& f) -> decltype(f()) {
        using R = decltype(f());

        if (SPI_connect() != SPI_OK_CONNECT) {
            ereport(ERROR, (errmsg(kSpiConnectFailed)));
        }

        PG_TRY();
        {
            R result = f();

            if (SPI_finish() != SPI_OK_FINISH) {
                ereport(ERROR, (errmsg(kSpiFinishFailed)));
            }

            return result;
        }

        PG_CATCH();
        {
            SPI_finish();
            PG_RE_THROW();
        }

        PG_END_TRY();

        __builtin_unreachable();
    }
};

} //recdb2::pg_spi