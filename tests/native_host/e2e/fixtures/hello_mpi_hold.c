// Copyright (c) 2026 Advanced Micro Devices, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Holds open past MPI_Init (via barrier) so a test can restart the agent
// while ranks are alive and past init, not during it.

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

int main(int argc, char **argv)
{
    int rank = 0;
    int size = 0;
    int hold_secs = 30;

    if (argc > 1) {
        hold_secs = atoi(argv[1]);
    }

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    // Every rank must reach this before any of them prints WIRED-UP, so the
    // marker really does mean "past wireup", not "rank 0 got there first".
    MPI_Barrier(MPI_COMM_WORLD);
    printf("WIRED-UP rank=%d size=%d\n", rank, size);
    fflush(stdout);

    sleep(hold_secs);

    printf("rank=%d size=%d\n", rank, size);
    fflush(stdout);

    MPI_Finalize();
    return 0;
}
