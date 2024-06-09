/*
    implementation of API
*/

#include "def.h"

pthread_mutex_t mutex_for_fs_stat;

//initialize file system - should be called as the first thing before accessing this file system 
int RSFS_init(){

    //initialize data blocks
    for(int i=0; i<NUM_DBLOCKS; i++){
      void *block = malloc(BLOCK_SIZE); //a data block is allocated from memory
      if(block==NULL){
        printf("[init] fails to init data_blocks\n");
        return -1;
      }
      data_blocks[i] = block;  
    } 

    //initialize bitmaps
    for(int i=0; i<NUM_DBLOCKS; i++) data_bitmap[i]=0;
    pthread_mutex_init(&data_bitmap_mutex,NULL);
    for(int i=0; i<NUM_INODES; i++) inode_bitmap[i]=0;
    pthread_mutex_init(&inode_bitmap_mutex,NULL);    

    //initialize inodes
    for(int i=0; i<NUM_INODES; i++){
        inodes[i].length=0;
        for(int j=0; j<NUM_POINTER; j++) 
            inodes[i].block[j]=-1; //pointer value -1 means the pointer is not used
        inodes[i].num_current_reader=0;
        pthread_mutex_init(&inodes[i].rw_mutex,NULL);
        pthread_mutex_init(&inodes[i].read_mutex,NULL);
    }
    pthread_mutex_init(&inodes_mutex,NULL); 

    //initialize open file table
    for(int i=0; i<NUM_OPEN_FILE; i++){
        struct open_file_entry entry=open_file_table[i];
        entry.used=0; //each entry is not used initially
        pthread_mutex_init(&entry.entry_mutex,NULL);
        entry.position=0;
        entry.access_flag=-1;
    }
    pthread_mutex_init(&open_file_table_mutex,NULL); 

    //initialize root directory
    root_dir.head = root_dir.tail = NULL;

    //initialize mutex_for_fs_stat
    pthread_mutex_init(&mutex_for_fs_stat,NULL);

    //return 0 means success
    return 0;
}


//create file
//if file does not exist, create the file and return 0;
//if file_name already exists, return -1; 
//otherwise, return -2.
int RSFS_create(char *file_name){

    //search root_dir for dir_entry matching provided file_name
    struct dir_entry *dir_entry = search_dir(file_name);

    if(dir_entry){//already exists
        printf("[create] file (%s) already exists.\n", file_name);
        return -1;
    }else{

        if(DEBUG) printf("[create] file (%s) does not exist.\n", file_name);

        //construct and insert a new dir_entry with given file_name
        dir_entry = insert_dir(file_name);
        if(DEBUG) printf("[create] insert a dir_entry with file_name:%s.\n", dir_entry->name);
        
        //access inode-bitmap to get a free inode 
        int inode_number = allocate_inode();
        if(inode_number<0){
            printf("[create] fail to allocate an inode.\n");
            return -2;
        } 
        if(DEBUG) printf("[create] allocate inode with number:%d.\n", inode_number);

        //save inode-number to dir-entry
        dir_entry->inode_number = inode_number;
        
        return 0;
    }
}



//open a file with RSFS_RDONLY or RSFS_RDWR flags
//When flag=RSFS_RDONLY: 
//  if the file is currently opened with RSFS_RDWR (by a process/thread)=> the caller should be blocked (wait); 
//  otherwise, the file is opened and the descriptor (i.e., index of the open_file_entry in the open_file_table) is returned
//When flag=RSFS_RDWR:
//  if the file is currently opened with RSFS_RDWR (by a process/thread) or RSFS_RDONLY (by one or multiple processes/threads) 
//      => the caller should be blocked (i.e. wait);
//  otherwise, the file is opened and the desrcriptor is returned
int RSFS_open(char *file_name, int access_flag){

    //Check to make sure access_flag is either RSFS_RDONLY or RSFS_RDWR
    if (access_flag != RSFS_RDONLY && access_flag != RSFS_RDWR) {
        printf("[open] access_flag is not RSFS_RDONLY or RSFS_RDWR.\n");
        return -1;
    }
    
    //Find dir_entry matching file_name
    struct dir_entry *dir = search_dir(file_name);

    //Find the corresponding inode 
    struct inode *inode = &inodes[dir->inode_number];
    
    //Based on the requested access_flag and the current "open" status of this file to block the caller if needed
    //(refer to solution to reader/writer problem) 
    if (access_flag == RSFS_RDONLY) {
        //Increment the num readers and lock the rw_mutex if this is the first reader
        pthread_mutex_lock(&inode->read_mutex);
        inode->num_current_reader++;
        if (inode->num_current_reader == 1) {
            pthread_mutex_lock(&inode->rw_mutex);
        }
        pthread_mutex_unlock(&inode->read_mutex);
    } else {
        //Writer must have the rw_mutex to open the file
        pthread_mutex_lock(&inode->rw_mutex);
    }

    //Find an unused open-file-entry in open-file-table and fill the fields of the entry properly
    int fd = allocate_open_file_entry(access_flag, dir);
    
    //Return the index of the open-file-entry in open-file-table as file descriptor
    return fd; 
}



//append the content in buf to the end of the file of descriptor fd
int RSFS_append(int fd, void *buf, int size){
    //Sanity check
    if (fd < 0 || fd >= NUM_OPEN_FILE || size <= 0) {
        printf("[write] fd is not in [0,NUM_OPEN_FILE] or size <= 0.\n");
        return -1;
    }
    
    //Get the corresponding open file entry
    struct open_file_entry *entry = &open_file_table[fd];
    struct dir_entry *dir_entry = entry->dir_entry;
    struct inode *inode = &inodes[dir_entry->inode_number];

    //Check if the file is opened with RSFS_RDWR mode
    if (entry->access_flag != RSFS_RDWR) {
        printf("[write] file is not opened with RSFS_RDWR mode.\n");
        return -1;
    }

    //Get the current position along with the block position and offset
    int current_position = entry->position;
    int block_position = current_position / BLOCK_SIZE;
    int offset = current_position % BLOCK_SIZE;
    int bytes_written = 0;

    //While we have not written all the bytes in the buffer
    while (bytes_written < size) {
        //Get the block position and offset
        block_position = current_position / BLOCK_SIZE;
        offset = current_position % BLOCK_SIZE;

        //Check if the block is allocated, if not allocate a new block
        int block = inode->block[block_position];
        if (block == -1) {
            block = allocate_data_block();
            inode->block[block_position] = block;
        }

        //Check how much space is available in the block
        int space_available = BLOCK_SIZE - offset;

        //Check how many bytes we have left to write
        int bytes_to_write = size;

        //If we have more space available than bytes left to write, only write the bytes left
        if (space_available > size - bytes_written) {
            bytes_to_write = size - bytes_written;
        }

        //If we have more bytes left to write than space available, only write the space available
        if (space_available < size - bytes_written) {
            bytes_to_write = space_available;
        }

        //Copy the data from the buffer to the data block
        memcpy(data_blocks[block] + offset, buf + bytes_written, bytes_to_write);
        bytes_written += bytes_to_write;
        current_position += bytes_to_write;

        //Update the length of the file if needed
        if (current_position > inode->length) {
            inode->length = current_position;
        }
    }
    
    //Update the current position in the open file entry
    entry->position = current_position;
    
    //Return the number of bytes written
    return bytes_written;
}







//update current position of the file (which is in the open_file_entry) to offset
int RSFS_fseek(int fd, int offset){

    //Sanity Check  
    if (fd < 0 || fd >= NUM_OPEN_FILE) {
        printf("[fseek] fd is not in [0,NUM_OPEN_FILE].\n");
        return -1;
    }

    //Get the correspondng open file entry
    struct open_file_entry *entry = &open_file_table[fd];
    
    //Get the current position
    int current_position = entry->position;
    
    //Get the corresponding dir entry
    struct dir_entry *dir_entry = entry->dir_entry;
    
    //Get the corresponding inode and file length
    struct inode *inode = &inodes[dir_entry->inode_number];
    
    //Check if argument offset is not within 0...length
    if (offset < 0 || offset > inode->length) {
        printf("[fseek] offset is not within 0...length.\n");
        return current_position;
    }
    
    //Update the current position to offset
    entry->position = offset;
    
    //Return the current poisiton
    return entry->position; 
}






//Read from file from the current position for up to size bytes
int RSFS_read(int fd, void *buf, int size){
    //Sanity check
    if (fd < 0 || fd >= NUM_OPEN_FILE || size <= 0) {
        printf("[read] fd is not in [0,NUM_OPEN_FILE] or size <= 0.\n");
        return -1;
    }
    
    //Get the corresponding open file entry
    struct open_file_entry *entry = &open_file_table[fd];

    //Get the current position
    int current_position = entry->position;

    //Get the corresponding directory entry
    struct dir_entry *dir_entry = entry->dir_entry;
    
    //Get the corresponding inode 
    struct inode *inode = &inodes[dir_entry->inode_number];
    
    //Read the content of the file from current position for up to size bytes and copy it to the buffer buf
    //Get the current block position and offset based on the current position
    int block_position = current_position / BLOCK_SIZE;
    int offset = current_position % BLOCK_SIZE;
    int bytes_read = 0;
    
    //While we have not read all the bytes in the buffer
    while (bytes_read < size && bytes_read < inode->length) {
        //Update the block position and offset
        block_position = current_position / BLOCK_SIZE;
        offset = current_position % BLOCK_SIZE;

        //Get the block and check if it is allocated
        int block = inode->block[block_position];
        if (block == -1) {
            break;
        }
        
        //Make sure we dont try to read past the end of the block
        int bytes_to_read = size - bytes_read;
        if (bytes_to_read > BLOCK_SIZE - offset) {
            bytes_to_read = BLOCK_SIZE - offset;
        }

        //Make sure we only read to the length of the file
        if (bytes_to_read > inode->length - current_position) {
            bytes_to_read = inode->length - current_position;
        }

        //Copy the data from the block to the buffer
        memcpy(buf + bytes_read, data_blocks[block] + offset, bytes_to_read);
        bytes_read += bytes_to_read;
        current_position += bytes_to_read;
    }
    
    
    //Update the current position in open file entry
    entry->position = current_position;
    
    //Return the actual number of bytes read
    return bytes_read; //placeholder 
}


//close file: return 0 if succeed
int RSFS_close(int fd){

    //Sanity Check  
    if (fd < 0 || fd >= NUM_OPEN_FILE) {
        printf("[close] fd is not in [0,NUM_OPEN_FILE].\n");
        return -1;
    }

    //Get the corresponding open file entry
    struct open_file_entry *entry = &open_file_table[fd];

    //Get the corresponding dir entry
    struct dir_entry *dir_entry = entry->dir_entry;
    
    //Get the corresponding inode 
    struct inode *inode = &inodes[dir_entry->inode_number];

    //Depending on the way that the file was open (RSFS_RDONLY or RSFS_RDWR), update the corresponding mutex and/or count 
    //(refer to the solution to the readers/writers problem)
    if (entry->access_flag == RSFS_RDWR) {
        //Writer must release the rw_mutex when closing the file
        pthread_mutex_unlock(&inode->rw_mutex);

    } else {
        //Check if this is the last reader and release the rw_mutex if it is
        pthread_mutex_lock(&inode->read_mutex);
        inode->num_current_reader--;
        if (inode->num_current_reader == 0) {
            pthread_mutex_unlock(&inode->rw_mutex);
        }
        pthread_mutex_unlock(&inode->read_mutex);
    }
    
    //Release this open file entry in the open file table
    free_open_file_entry(fd);
    return 0;
}









//delete file
int RSFS_delete(char *file_name){

    //Find the corresponding dir_entry
    struct dir_entry *dir_entry = search_dir(file_name);

    //Find the corresponding inode
    struct inode *inode = &inodes[dir_entry->inode_number];

    //Free the data-blocks
    int file_length = inode->length;

    for (int i = 0; i <= file_length / BLOCK_SIZE; i++){
        //Wipe the data that is currently in the block
        memset(data_blocks[inode->block[i]], 0, BLOCK_SIZE);
        //Free the data block in the data bitmap
        free_data_block(inode->block[i]);
    }

    //Free the inode
    free_inode(dir_entry->inode_number);

    //Free the directory entry
    delete_dir(file_name);
    
    return 0;
}


//Print status of the file system
void RSFS_stat(){

    pthread_mutex_lock(&mutex_for_fs_stat);

    printf("\nCurrent status of the file system:\n\n %16s%10s%10s\n", "File Name", "Length", "iNode #");

    //list files
    struct dir_entry *dir_entry = root_dir.head;
    while(dir_entry!=NULL){

        int inode_number = dir_entry->inode_number;
        struct inode *inode = &inodes[inode_number];
        
        printf("%16s%10d%10d\n", dir_entry->name, inode->length, inode_number);
        dir_entry = dir_entry->next;
    }
    
    //data blocks
    int db_used=0;
    for(int i=0; i<NUM_DBLOCKS; i++) db_used+=data_bitmap[i];
    printf("\nTotal Data Blocks: %4d,  Used: %d,  Unused: %d\n", NUM_DBLOCKS, db_used, NUM_DBLOCKS-db_used);

    //inodes
    int inodes_used=0;
    for(int i=0; i<NUM_INODES; i++) inodes_used+=inode_bitmap[i];
    printf("Total iNode Blocks: %3d,  Used: %d,  Unused: %d\n", NUM_INODES, inodes_used, NUM_INODES-inodes_used);

    //open files
    int of_num=0;
    for(int i=0; i<NUM_OPEN_FILE; i++) of_num+=open_file_table[i].used;
    printf("Total Opened Files: %3d\n\n", of_num);

    pthread_mutex_unlock(&mutex_for_fs_stat);
}



//Write the content of size (bytes) in buf to the file (of descripter fd) from current position for up to size bytes 
int RSFS_write(int fd, void *buf, int size){

    //Sanity check
    if (fd < 0 || fd >= NUM_OPEN_FILE || size <= 0) {
        printf("[write] fd is not in [0,NUM_OPEN_FILE] or size <= 0.\n");
        return -1;
    }
    
    //Get the corresponding information from the given file descriptor
    struct open_file_entry *entry = &open_file_table[fd];
    struct dir_entry *dir_entry = entry->dir_entry;
    struct inode *inode = &inodes[dir_entry->inode_number];
    //Ensure that the file is opened with RSFS_RDWR mode
    if (entry->access_flag != RSFS_RDWR) {
        printf("[write] file is not opened with RSFS_RDWR mode.\n");
        return -1;
    }

    //Get the current position and the block position and offset
    int current_position = entry->position;
    int block_position = current_position / BLOCK_SIZE;
    int offset = current_position % BLOCK_SIZE;
    int bytes_written = 0;

    //Need to remove the content from the current position to the end of the file if the current position is not at the end of the file
    int remove_from = current_position + size;
    int remove_to = inode->length;

    //Remove the information from the current position to the end of the file
    if (remove_from < remove_to){
        while (remove_from < remove_to) {
            int block = inode->block[remove_from / BLOCK_SIZE];
            int offset = remove_from % BLOCK_SIZE;

            memset(data_blocks[block] + offset, 0, BLOCK_SIZE - offset);
            if (offset == 0){
                free_data_block(block);
            }
            remove_from += BLOCK_SIZE - offset;
        }

    }

    //Write the content of size (bytes) in buf to the file (of descripter fd) from current position for up to size bytes
    //Note this is very similar to append but we are just adding the before step of removing the content from the current position to the end of the file
    while (bytes_written < size) {
        //Get the block and allocate a new block if it is not allocated
        block_position = current_position / BLOCK_SIZE;
        offset = current_position % BLOCK_SIZE;
        int block = inode->block[block_position];
        if (block == -1) {
            block = allocate_data_block();
            inode->block[block_position] = block;
        }

        //Check how much space is available in the block
        int space_available = BLOCK_SIZE - offset;
        int bytes_to_write = size;

        //If we have more space available than bytes left to write, only write the bytes left
        if (space_available > size - bytes_written) {
            bytes_to_write = size - bytes_written;
        }

        //If we have more bytes left to write than space available, only write the space available
        if (space_available < size - bytes_written) {
            bytes_to_write = space_available;
        }

        //Copy the data from the buffer to the data block
        memcpy(data_blocks[block] + offset, buf + bytes_written, bytes_to_write);
        bytes_written += bytes_to_write;
        current_position += bytes_to_write;

        //Update the length of the file if needed
        if (current_position > inode->length) {
            inode->length = current_position;
        }
    }
    
    //Update the current position in the open file entry
    entry->position = current_position;
    
    //Return the number of bytes written
    return bytes_written;
}




//cut the content from the current position for up to size (bytes) from the file of descriptor fd
int RSFS_cut(int fd, int size){
    //Sanity check
    if (fd < 0 || fd >= NUM_OPEN_FILE || size <= 0) {
        printf("[cut] fd is not in [0,NUM_OPEN_FILE] or size <= 0.\n");
        return -1;
    }

    //Get the corresponding open file entry
    struct open_file_entry *entry = &open_file_table[fd];

    //Get the current position
    int current_position = entry->position;
    
    //Get the corresponding directory entry
    struct dir_entry *dir_entry = entry->dir_entry;

    //Get the corresponding inode
    struct inode *inode = &inodes[dir_entry->inode_number];

    //Need to memcpy all the information from current_position + size to the end of the file over the
    // current memory starting at current_position. If there are multiple data_blocks, need to account for that.
    
    //Get the position to start cutting the file and stop cutting the file
    int cut_start = current_position + size;
    int cut_end = inode->length;
    //Create a buf to store the data that is not being cut
    char buf[NUM_POINTER*BLOCK_SIZE];
    memset(buf,0,NUM_POINTER*BLOCK_SIZE);

    //Copy the data that is not being cut to the buffer
    int bytes_to_copy = cut_end - cut_start;
    int bytes_copied = 0;
    while (bytes_to_copy > 0) {
        int block = inode->block[cut_start / BLOCK_SIZE];
        int offset = cut_start % BLOCK_SIZE;

        int bytes_to_read = bytes_to_copy;
        if (bytes_to_read > BLOCK_SIZE - offset) {
            bytes_to_read = BLOCK_SIZE - offset;
        }

        //copy the data from the block to the buffer
        memcpy(buf + bytes_copied, data_blocks[block] + offset, bytes_to_read);

        cut_start += bytes_to_read;
        bytes_to_copy -= bytes_to_read;
        bytes_copied += bytes_to_read;
    }

    //Copy the data from the buffer back to the file
    //Note that thsi is the same as the append function but we are copying the data from the buffer we created to the file
    current_position = entry->position;
    int block_position = current_position / BLOCK_SIZE;
    int offset = current_position % BLOCK_SIZE;
    int bytes_written = 0;

    while (bytes_written < bytes_copied) {
        block_position = current_position / BLOCK_SIZE;
        offset = current_position % BLOCK_SIZE;

        int block = inode->block[block_position];
        if (block == -1) {
            block = allocate_data_block();
            inode->block[block_position] = block;
        }
        int space_available = BLOCK_SIZE - offset;
        int bytes_to_write = bytes_copied;

        if (space_available > bytes_copied - bytes_written) {
            bytes_to_write = bytes_copied - bytes_written;
        }

        if (space_available < bytes_copied - bytes_written) {
            bytes_to_write = space_available;
        }

        memcpy(data_blocks[block] + offset, buf + bytes_written, bytes_to_write);
        bytes_written += bytes_to_write;
        current_position += bytes_to_write;

        if (current_position > inode->length) {
            inode->length = current_position;
        }
    }

    //memset the remaining data in the file to 0
    while (current_position < cut_end) {
        int block = inode->block[current_position / BLOCK_SIZE];
        int offset = current_position % BLOCK_SIZE;
        memset(data_blocks[block] + offset, 0, BLOCK_SIZE - offset);
        if (offset == 0){
            free_data_block(block);
        }
        current_position += BLOCK_SIZE - offset;
    }

    //Update the current position and inode length 
    inode->length -= size;
    current_position = inode->length;
    entry->position = current_position;

    //Return the number of bytes cut
    return size; 
}



