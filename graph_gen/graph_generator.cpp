#include <stdio.h>
#include <stdlib.h>
#include <time.h>

int main(int argc, char* argv[]) {
    if (argc != 4) {
        printf("Usage: %s n p filename\n", argv[0]);
        return 1;
    }

    int n = atoi(argv[1]);
    float p = atof(argv[2]);
    const char* filename = argv[3];

    FILE* f = fopen(filename, "w");
    if (f == NULL) {
        perror("Error opening file");
        return 1;
    }

    srand(time(NULL)); // Seed the random number generator with the current time

    for (int src = 0; src < n; src++) {
        for (int dst = 0; dst < n; dst++) {
            if ((float)rand() / RAND_MAX < p) {
                fprintf(f, "%d %d\n", src, dst);
            }
        }
    }

    fclose(f);
    return 0;
}