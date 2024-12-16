#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#define MB (1024 * 1024)

int read_mem(int mb_count, int value) {
    int i;
    char *ptr = NULL;
    size_t size = mb_count * MB * sizeof(char);
    do {
        ptr = mmap(
            NULL,
            size,
            PROT_READ | PROT_WRITE,
            MAP_ANONYMOUS | MAP_PRIVATE,
            -1,
            0
        );
    } while (ptr == MAP_FAILED);
    memset(ptr, 1, size);

    // Iterate through allocated memory and make sure every byte equals 1
    for (size_t i = 0; i < size; i++) {
        if (ptr[i] != 1) {
            printf("Byte %zu is not 1! It is %d\n", i, ptr[i]);
            return 1;
        }
    }

    return 0;
}
int main(int argc, char *const argv[]) {
    if (argc != 3) {
        printf("Usage: ./readmem mb_count value\n");
        return -1;
    }

    int mb_count = atoi(argv[1]);
    int value = atoi(argv[2]);
    return read_mem(mb_count, value);
}