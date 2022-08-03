#include <iostream>

#include "lib1.hpp"
#include "lib2.hpp"


int main() {
  std::cout << "The answer is " << (lib1() + lib2()) << std::endl;
  return 0;
}