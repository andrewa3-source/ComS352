/*
    Allocation of data block, data block bitmaps and mutex to guard M.E. access; 
    routines for managing them
*/

#include "def.h"


//allocation of data block and data block bitmaps
void *data_blocks[BLOCK_SIZE];
int data_bitmap[NUM_DBLOCKS];
pthread_mutex_t data_bitmap_mutex;


//to allocate an empty data block and return the block-number;
//if no free data block is available, return -1
int allocate_data_block(){

    int block_number=-1; //init

    pthread_mutex_lock(&data_bitmap_mutex);

    for(int i=0; i<NUM_DBLOCKS; i++){
        if(data_bitmap[i]==0){//find an available data block
            block_number=i;
            data_bitmap[i]=1; //mark it as allocated
            break;
        }
    }

    pthread_mutex_unlock(&data_bitmap_mutex);

    return block_number;
}

//to free a data block with the provided block_number
void free_data_block(int block_number){

    pthread_mutex_lock(&data_bitmap_mutex);

    data_bitmap[block_number]=0; //reset it to available

    pthread_mutex_unlock(&data_bitmap_mutex);
}

