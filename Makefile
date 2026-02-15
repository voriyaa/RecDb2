EXTENSION = recdb2
MODULE_big = recdb2

SRC := $(wildcard src/*.cpp src/*/*.cpp src/*/*/*.cpp)
OBJS := $(patsubst %.cpp,%.o,$(SRC))

DATA = $(wildcard sql/recdb2--*.sql)

PG_CONFIG = $(HOME)/pg/bin/pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

CXX = g++
CXXFLAGS += -std=c++20
SHLIB_LINK += -lstdc++