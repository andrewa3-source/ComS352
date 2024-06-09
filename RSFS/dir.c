/*
    allocation of global variable root_dir (root directory); 
    routines for directory management
*/


#include "def.h"

struct root_dir root_dir; //global root directory

//helper: search for dir entry matching provided file_name
struct dir_entry *search_dir_internal(char *file_name){
    //start from head entry
    struct dir_entry *dir_entry = root_dir.head;
    
    //loop through all entries
    while(dir_entry){
        if(strcmp(dir_entry->name,file_name)==0){
            break; //break when finding a match
        } 
        dir_entry = dir_entry->next;
    }
    
    //return the found match; NULL is not found
    return dir_entry;
}

//search for the dir_entry for provided file_name
struct dir_entry *search_dir(char *file_name){    

    pthread_mutex_lock(&root_dir.mutex);

    struct dir_entry *dir_entry = search_dir_internal(file_name);
    
    pthread_mutex_unlock(&root_dir.mutex);

    return dir_entry;
}

//insert an entry with provided file_name and return it;
//if such entry exists already, return it directly
struct dir_entry *insert_dir(char *file_name){
    
    pthread_mutex_lock(&root_dir.mutex);

    //search for the entry
    struct dir_entry *dir_entry = search_dir_internal(file_name);
    
    if(!dir_entry){//if not found
        
        //construct a new dir_entry
        dir_entry = (struct dir_entry *)malloc(sizeof(struct dir_entry));
        if(dir_entry==NULL){
            printf("[insert_dir] fail to allocate a space for dir_entry.\n");
            pthread_mutex_unlock(&root_dir.mutex);
            return NULL;
        }

        dir_entry->name = file_name;
        dir_entry->inode_number = -1; //mark that inode_number is not assigned 
        dir_entry->next = dir_entry->prev = NULL; //initialize the links

        //append the dir_entry to root_dir
        if(root_dir.tail){//the root_dir is non-empty
            root_dir.tail->next = dir_entry;
            dir_entry->prev = root_dir.tail;
            root_dir.tail = dir_entry;
        }else{//the root_dir is empty
            root_dir.head = root_dir.tail = dir_entry;
        }
    } 

    pthread_mutex_unlock(&root_dir.mutex);

    return dir_entry;
}

//delete the entry matching provided file_name if it exists;
//return 0 if succeed (found and deleted) or -1 if errs
int delete_dir(char *file_name){

    pthread_mutex_lock(&root_dir.mutex);

    int ret = -1;

    //search for the matching dir_entry
    struct dir_entry *dir_entry = search_dir_internal(file_name); 

    //if found, delete it
    if(dir_entry){
        if(dir_entry->prev){//not the head entry
            dir_entry->prev->next = dir_entry->next;
            if(dir_entry->next){//it is not the tail entry
                dir_entry->next->prev = dir_entry->prev;
            }else{//it is the tail entry
                root_dir.tail = dir_entry->prev;
            } 
        }else{//it is the head entry
            root_dir.head = dir_entry->next;
            if(dir_entry->next){//it is not the tail entry
                dir_entry->next->prev=NULL;
            }else{//it is the tail entry
                root_dir.tail = NULL;
            }
        }
        ret = 0;
    }

    pthread_mutex_unlock(&root_dir.mutex);

    return ret;
}

