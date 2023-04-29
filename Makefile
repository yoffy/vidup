TARGET=vidup
CXXFLAGS=-Wall -Wextra -g -Ofast -std=c++17 -march=haswell
LDFLAGS=-lsqlite3

.PHONY: all
all: $(TARGET)

.PHONY: clean
clean:
	$(RM) $(TARGET) *.o

$(TARGET): main.o
	$(CXX) $< $(LDFLAGS) -o $(TARGET)