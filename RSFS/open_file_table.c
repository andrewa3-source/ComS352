/*
    allocation of global open_file_table and its guarding mutex; 
    routines for open file entry
*/

#include "def.h"

struct open_file_entry open_file_table[NUM_OPEN_FILE];
pthread_mutex_t open_file_table_mutex;

//allocate an available entry in open file table and return fd (file descriptor);
//return -1 if no entry is found
int allocate_open_file_entry(int access_flag, struct dir_entry *dir_entry){
    
    int fd=-1;
    
    pthread_mutex_lock(&open_file_table_mutex);
    for(int i=0; i<NUM_OPEN_FILE; i++){
        struct open_file_entry *entry = &open_file_table[i];
        if(entry->used==0){
            fd=i; //find a valid fd
            entry->used = 1; //mark it as used

            //set up the entry
            entry->access_flag = access_flag;
            entry->dir_entry = dir_entry; 

            //init position
            entry->position = 0; 
            
            break;
        }
    }
    pthread_mutex_unlock(&open_file_table_mutex);

    return fd;
}


void free_open_file_entry(int fd){
    pthread_mutex_lock(&open_file_table_mutex);
    open_file_table[fd].used=0;
    pthread_mutex_unlock(&open_file_table_mutex);
}