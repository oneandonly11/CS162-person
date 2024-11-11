/*
 * mm_alloc.c
 */

#include "mm_alloc.h"

#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <stddef.h>
#include <string.h>

struct Heap* get_heap_from_data(void* data){
  return (struct Heap*)((char*)data - offsetof(struct Heap, data));
}

struct Heap* create_heap(size_t size){
  struct Heap* new_heap = sbrk(size + sizeof(struct Heap));
  if(new_heap == (void*)-1){
    return NULL;
  }
  memset(new_heap, 0, size + sizeof(struct Heap));
  new_heap->size = size;
  new_heap->free = 1;
  return new_heap;
}

void* mm_malloc(size_t size) {
  if(size == 0) {
    return NULL;
  }
  struct Heap* current = heap_start;
  if(!heap_start){
    heap_start = create_heap(size);
    if(!heap_start)
      return NULL;
    heap_start->next = NULL;
    heap_start->free = 0;
    return heap_start->data;
  }
  
  while(current != NULL){
    if(current->free && current->size >= size){
      current->free = 0;
      return current->data;
    }
    current = current->next;
  }

  struct Heap* new_heap = create_heap(size);
  if(!new_heap)
    return NULL;
  new_heap->next = heap_start;
  heap_start = new_heap;
  new_heap->free = 0;
  return new_heap->data;
}

void* mm_realloc(void* ptr, size_t size) {
  if(size == 0){
    mm_free(ptr);
    return NULL;
  }
  if(ptr == NULL)
    return mm_malloc(size);
  struct Heap* current = get_heap_from_data(ptr);
  if(current->size >= size){
    return ptr;
  }
  else{
    void* new_ptr = mm_malloc(size);
    if(new_ptr == NULL)
      return NULL;
    memcpy(new_ptr, ptr, current->size);
    mm_free(ptr);
    return new_ptr;
  }

  return NULL;
}

void mm_free(void* ptr) {
  if(ptr == NULL)
    return;
  struct Heap* current = get_heap_from_data(ptr);
  current->free = 1;
  while (current->next != NULL && current->next->free){
    current->size += current->next->size + sizeof(struct Heap);
    current->next = current->next->next;
    memset(current->data, 0, current->size);
  }
}
