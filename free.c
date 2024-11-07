#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdio.h>

int main() {
    size_t mem_size = 5000000;
    printf("Total memory size: %zu bytes\n", mem_size);
    void *buffer = malloc(mem_size);
    if (buffer) {
        memset(buffer, 0, mem_size);
        free(buffer);
    }
    return 0;
}

