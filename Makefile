CXX       = g++
CXXFLAGS  = -Wall -Werror -g -std=c++20 -pthread -lncursesw -I$(INC_DIR)
MAKEFLAGS += -j4

.PRECIOUS: %.o %.gch %.out

BLD_DIR := .
SRC_DIR := ./src
INC_DIR := ./include
OBJ_DIR := ./src
TST_DIR := ./test

TARGETS := main.out map.out
TARGETS := $(TARGETS:%=$(BLD_DIR)/%)
SOURCES := $(shell find $(SRC_DIR) -name '*.cc')
OBJECTS := $(SOURCES:$(SRC_DIR)/%.cc=$(OBJ_DIR)/%.o)
OBJECTS := $(filter-out $(TARGETS:$(BLD_DIR)/%.out=$(OBJ_DIR)/%.o),$(OBJECTS))
TESTS   := $(shell find $(TST_DIR) -name '*.cc')
TESTS   := $(TESTS:%.cc=%.out)

.PHONY: all clean test

all: $(TARGETS) 

test: $(INC_DIR)/header.hh.gch $(TESTS)
	./test.sh

clean:
	rm -rf */*.o */*.gch */*.out */*.log *.out

# Automatically make necessary directories.
./%:
	mkdir $@
$(OBJECTS): | $(OBJ_DIR)
$(TARGETS): | $(BLD_DIR)

# Making object files.
$(OBJ_DIR)/%.o: $(SRC_DIR)/%.cc $(INC_DIR)/%.hh 
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Making Executables.
$(BLD_DIR)/%.out: $(OBJ_DIR)/%.cc $(OBJECTS)
	$(CXX) $(CXXFLAGS) $^ -o $@

# Compiling testcases. 
$(TST_DIR)/%.out: $(TST_DIR)/%.cc $(OBJECTS)
	$(CXX) $(CXXFLAGS) $^ -o $@

# Rule for creating precompiled header files.
$(INC_DIR)/%.hh.gch: $(INC_DIR)/%.hh $(INC_DIR)/*.hh
	$(CXX) $(CXXFLAGS) -c $< 

