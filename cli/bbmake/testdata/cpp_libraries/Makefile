all: main

main: main.cpp lib1.o lib2.o common.o
	$(CXX) $^ -o $@

%.o: %.cpp
	$(CXX) -c $^ -o $@

clean:
	rm -rf main *.o
