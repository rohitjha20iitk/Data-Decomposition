#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "mpi.h"

float min, finalmin, globalmin = 100000.00;
double sTime, eTime, time, maxTime;


int main(int argc, char *argv[]){
    
    MPI_Init (&argc, &argv);
    int size, myrank;
    MPI_Comm_rank (MPI_COMM_WORLD, &myrank);
    MPI_Comm_size (MPI_COMM_WORLD, &size);
    MPI_Status status;
    
    
    int row = 0, col = 0, c = 0, r = 0, i = 0, j = 0;
    char buffer[1024];
    char *record, *line;
    
    
    // Counting number of rows and columns of given file
    if(myrank == 0){
        FILE *fstream = fopen(argv[1], "r");
        if(fstream == NULL){
            printf("\n file opening failed ");
            return -1;
        }
        while((line = fgets(buffer, sizeof(buffer), fstream)) != NULL){
            row++;
            record = strtok(line, ",");
            col = 0;
            while(record != NULL){
                record = strtok(NULL, ",");
                col++;
            }
            i++;
        }
    }
    
    
    // If size is greater than 1 then communicate the number of rows and columns to other processes
    if(size > 1){
        MPI_Bcast(&row, 1, MPI_INT, 0, MPI_COMM_WORLD);
        MPI_Bcast(&col, 1, MPI_INT, 0, MPI_COMM_WORLD);
    }
    
    
    // Subtracting header count for rows and columns
    col = col - 2;
    row = row - 1;
    
    
    // Calculating the number of year data need to be sent to each process
    int partsize = col / size;
    
    
    // Calculation of extra year data
    int left = col % size;
    
    
    // Declaring matrix of size col * row
    float** mat = (float**)malloc(col * sizeof(float*));
        float* data=(float*)malloc(col * row * sizeof(float));
        for (int i=0; i < col; i++){
            mat[i]=&(data[row * i]);
    }
    
    
    // Storing year wise data in a row of matrix
    if(myrank == 0){
        j = 0;
        FILE *f = fopen(argv[1], "r");
        
        while((line = fgets(buffer, sizeof(buffer), f)) != NULL){
            r++;
            if(r > 1){
                i = 0;
                c = 0;
                record = strtok(line, ",");
                while(record != NULL){
                    c++;
                    if(c > 2){
                        char *ptr;
                        mat[i++][j] = strtod(record, &ptr);
                    }
                    record = strtok(NULL, ",");
                }
                j++;
            }
        }
    }
    
    
    // If process count is 1 simply find minimum of each row and store in buf
    if(size == 1){
        
        sTime = MPI_Wtime();
        
        float *buf = (float*) malloc(col * sizeof(float));
        
        for(int i = 0; i < col; i++){
            min = mat[i][0];
            for(int j = 1; j < row; j++){
                if(mat[i][j] < min){
                    min = mat[i][j];
                }
            }
            buf[i] = min;
            if(globalmin > min){
                globalmin = min;
            }
        }
        
        eTime = MPI_Wtime();
        time = eTime - sTime;
        //printf("process1 = %lf\n", time);
        
        
        // Writing data to the output file
        FILE *fp = fopen("output.txt", "w");
        for(int n = 0; n < col; n++) {
            fprintf(fp, "%.2f", buf[n]);
            if(n != col - 1){
                fprintf(fp, ",");
            }
        }
        fprintf(fp, "\n");
        fprintf(fp, "%.2f\n", globalmin);
        fprintf(fp, "%lf\n", time);
        fclose(fp);
    }
    
    
    // If size is greater than 2
    else{
        
        sTime = MPI_Wtime(); 
        
        // Storing the partial min of each year at root process
        float *recvbuf = (float*) malloc(col * sizeof(float));
        
        // buffer to store the scattered data
        float *receive = (float*) malloc(partsize * row * sizeof(float));
        
        // buffer to store the partial min by each process
        float *buf = (float*) malloc((partsize + left) * sizeof(float));
    
        // Creating a vector datatype
        int count = partsize;
        int blocklen = row;
        int stride = row;
        MPI_Datatype newvtype;
        MPI_Type_vector (count, blocklen, stride, MPI_FLOAT, &newvtype);
        MPI_Type_commit (&newvtype);
    
         
        // Scattering data to each process
        MPI_Scatter(&mat[0][0], 1, newvtype, receive, 1, newvtype, 0, MPI_COMM_WORLD);
        
        
        // Finding partial minimum by each process
        int k = 0;
        for(int i = 0; i < partsize * row; ){
            min = receive[i];
            for(int j = i + 1; j < i + row; j++){
                if(receive[j] < min){
                    min = receive[j];
                }
            }
            buf[k++] = min;
            if(globalmin > min){
                globalmin = min;
            }
            i += row;
        }
        
        // Gathering all minimums of each process at root
        MPI_Gather(buf, partsize, MPI_FLOAT, recvbuf, partsize, MPI_FLOAT, 0, MPI_COMM_WORLD);
        
        
        // Computing minimum tempearture of extra left years by rank 0 and placing it in recvbuf
        if(myrank == 0){
            int k = partsize * size;
            for(int i = size * partsize; i < col; i++){
                min = mat[i][0];
                for(int j = 1; j < row; j++){
                    if(mat[i][j] < min){
                        min = mat[i][j];
                    }
                }
                recvbuf[k++] = min;
                if(globalmin > min){
                    globalmin = min;
                }
            }
        }
        
        // Reducing globalmin (i.e. minimum of each row) to finalmin
        MPI_Reduce (&globalmin, &finalmin, 1, MPI_FLOAT, MPI_MIN, 0, MPI_COMM_WORLD);
        
        eTime = MPI_Wtime();
        time = eTime - sTime;
     
        MPI_Reduce (&time, &maxTime, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        
        //if(!myrank) printf("process%d = %lf\n", size, maxTime);
        
        // Writing data to the output file
        if(myrank==0){
            FILE *fp = fopen("output.txt", "w");
            for(int n = 0; n < col; n++){
                fprintf(fp, "%.2f", recvbuf[n]);
                if(n != col - 1){
                    fprintf(fp, ",");
                }
	        }
	        fprintf(fp, "\n");
	        fprintf(fp, "%.2f\n", finalmin);
	        fprintf(fp, "%lf\n", maxTime);
	        fclose(fp);
        }
    }
    MPI_Finalize();
    return 0;
}
