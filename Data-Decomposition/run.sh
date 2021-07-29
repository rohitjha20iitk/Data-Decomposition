#!/bin/bash

echo "*****Running Assignment 3*****" 


make


~/UGP/allocator/src/allocator.out 1 1
mpiexec -np 1 ./code tdata.csv



~/UGP/allocator/src/allocator.out 2 2
mpiexec -np 2 ./code tdata.csv



~/UGP/allocator/src/allocator.out 4 4
mpiexec -np 4 ./code tdata.csv



~/UGP/allocator/src/allocator.out 2 1
mpiexec -np 2 -hostfile hosts ./code tdata.csv



~/UGP/allocator/src/allocator.out 4 2
mpiexec -np 4 -hostfile hosts ./code tdata.csv



~/UGP/allocator/src/allocator.out 8 4
mpiexec -np 8 -hostfile hosts ./code tdata.csv


echo "*****Completed Assignment 3*****"
