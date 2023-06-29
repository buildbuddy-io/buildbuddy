#include <stdio.h>
#include <fcntl.h>
#include <linux/ioctl.h>
#include <linux/userfaultfd.h>

// The ioctl functions are defined as addresses in /usr/include/linux/userfaultfd.h
// To manually generate the addresses, run this script
// Copy and paste the IOWR definition of the ioctl function from the userfaultfd.h file
// Run the script:
// gcc -o print_const print_const.c
// ./print_const

int main() {
	printf("(hex)%x\n", _IOWR(UFFDIO, _UFFDIO_WRITEPROTECT, struct uffdio_writeprotect));
}
