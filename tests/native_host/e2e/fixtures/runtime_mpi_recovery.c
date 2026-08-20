// Copyright (c) 2026 Advanced Micro Devices, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

#include <mpi.h>
#include <stdio.h>
#include <unistd.h>

int main(int argc, char **argv)
{
    int rank = 0;
    int size = 0;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Barrier(MPI_COMM_WORLD);
    printf("before-restart rank=%d size=%d\n", rank, size);
    fflush(stdout);
    sleep(15);
    MPI_Barrier(MPI_COMM_WORLD);
    printf("after-restart rank=%d size=%d\n", rank, size);
    fflush(stdout);
    MPI_Finalize();
    return 0;
}
