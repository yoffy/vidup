TARGET=vidup
CXXFLAGS=-Wall -Wextra -Ofast -std=c++17 -march=haswell
LDFLAGS=-lsqlite3

.PHONY: all
all: $(TARGET)

.PHONY: clean
clean:
	$(RM) $(TARGET) *.o

.PHONY: format
format:
	clang-format -i *.cpp

$(TARGET): main.o
	$(CXX) $< $(LDFLAGS) -o $(TARGET)
