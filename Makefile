TARGET=vidup
CXXFLAGS=-Wall -Wextra -g -Og -std=c++17 -march=haswell
LDFLAGS=-lsqlite3

.PHONY: all
all: $(TARGET)

.PHONY: clean
clean:
	$(RM) $(TARGET) *.o

$(TARGET): main.o
	$(CXX) $< $(LDFLAGS) -o $(TARGET)