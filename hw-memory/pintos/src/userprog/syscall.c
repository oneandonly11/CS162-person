#include "userprog/syscall.h"
#include <stdio.h>
#include <string.h>
#include <syscall-nr.h>
#include "threads/interrupt.h"
#include "threads/thread.h"
#include "threads/vaddr.h"
#include "filesys/filesys.h"
#include "filesys/file.h"
#include "threads/palloc.h"
#include "userprog/pagedir.h"

static void syscall_handler(struct intr_frame*);

void syscall_init(void) { intr_register_int(0x30, 3, INTR_ON, syscall_handler, "syscall"); }

void syscall_exit(int status) {
  printf("%s: exit(%d)\n", thread_current()->name, status);
  thread_exit();
}

/*
 * This does not check that the buffer consists of only mapped pages; it merely
 * checks the buffer exists entirely below PHYS_BASE.
 */
static void validate_buffer_in_user_region(const void* buffer, size_t length) {
  uintptr_t delta = PHYS_BASE - buffer;
  if (!is_user_vaddr(buffer) || length > delta)
    syscall_exit(-1);
}

/*
 * This does not check that the string consists of only mapped pages; it merely
 * checks the string exists entirely below PHYS_BASE.
 */
static void validate_string_in_user_region(const char* string) {
  uintptr_t delta = PHYS_BASE - (const void*)string;
  if (!is_user_vaddr(string) || strnlen(string, delta) == delta)
    syscall_exit(-1);
}

static int syscall_open(const char* filename) {
  struct thread* t = thread_current();
  if (t->open_file != NULL)
    return -1;

  t->open_file = filesys_open(filename);
  if (t->open_file == NULL)
    return -1;

  return 2;
}

static int syscall_write(int fd, void* buffer, unsigned size) {
  struct thread* t = thread_current();
  if (fd == STDOUT_FILENO) {
    putbuf(buffer, size);
    return size;
  } else if (fd != 2 || t->open_file == NULL)
    return -1;

  return (int)file_write(t->open_file, buffer, size);
}

static int syscall_read(int fd, void* buffer, unsigned size) {
  struct thread* t = thread_current();
  if (fd != 2 || t->open_file == NULL)
    return -1;

  return (int)file_read(t->open_file, buffer, size);
}

static void syscall_close(int fd) {
  struct thread* t = thread_current();
  if (fd == 2 && t->open_file != NULL) {
    file_close(t->open_file);
    t->open_file = NULL;
  }
}

static bool alloc_page_in_user(void* upage, bool writable) {
  struct thread* t = thread_current();
  void* kpage = palloc_get_page(PAL_USER | PAL_ZERO);
  if (kpage == NULL)
    return false;

  if (!pagedir_set_page(t->pagedir, upage, kpage, writable)) {
    palloc_free_page(kpage);
    return false;
  }

  return true;
}

static void free_page_in_user(void* upage) {
  struct thread* t = thread_current();
  palloc_free_page(pagedir_get_page(t->pagedir, upage));
  pagedir_clear_page(t->pagedir, upage);
}

static bool alloc_page_in_heap(intptr_t increment){
  struct thread* t = thread_current();
  uint8_t* new_heap_end = t->heap_end + increment;
  size_t num_pages = (pg_round_up(new_heap_end) - pg_round_up(t->heap_end)) / PGSIZE;
  for (int i = 0; i < num_pages; i++) {
    if (!alloc_page_in_user((uint8_t*)pg_round_up(t->heap_end) + i * PGSIZE, true)) {
      for (int j = 0; j < i; j++)
        free_page_in_user((uint8_t*)pg_round_up(t->heap_end) + j * PGSIZE);
      return false;
    }
  }

  return true;
}

static void free_page_in_heap(intptr_t increment) {
  struct thread* t = thread_current();
  uint8_t* new_heap_end = t->heap_end + increment;
  size_t num_pages = (pg_round_up(t->heap_end) - pg_round_up(new_heap_end)) / PGSIZE;
  for (int i = 0; i < num_pages; i++)
    free_page_in_user((uint8_t*)pg_round_up(new_heap_end) + i * PGSIZE);

}

static void* syscall_sbrk(intptr_t increment) {
  struct thread* t = thread_current();
  uint8_t* old_heap_end = t->heap_end;
  
  if (increment > 0) {
    if (!alloc_page_in_heap(increment))
      return (void*)-1;
  } else if (increment < 0) {
    free_page_in_heap(increment);
  }

  t->heap_end += increment;

  return old_heap_end;
}

static void syscall_handler(struct intr_frame* f) {
  uint32_t* args = (uint32_t*)f->esp;
  struct thread* t = thread_current();
  t->in_syscall = true;

  validate_buffer_in_user_region(args, sizeof(uint32_t));
  switch (args[0]) {
    case SYS_EXIT:
      validate_buffer_in_user_region(&args[1], sizeof(uint32_t));
      syscall_exit((int)args[1]);
      break;

    case SYS_OPEN:
      validate_buffer_in_user_region(&args[1], sizeof(uint32_t));
      validate_string_in_user_region((char*)args[1]);
      f->eax = (uint32_t)syscall_open((char*)args[1]);
      break;

    case SYS_WRITE:
      validate_buffer_in_user_region(&args[1], 3 * sizeof(uint32_t));
      validate_buffer_in_user_region((void*)args[2], (unsigned)args[3]);
      f->eax = (uint32_t)syscall_write((int)args[1], (void*)args[2], (unsigned)args[3]);
      break;

    case SYS_READ:
      validate_buffer_in_user_region(&args[1], 3 * sizeof(uint32_t));
      validate_buffer_in_user_region((void*)args[2], (unsigned)args[3]);
      f->eax = (uint32_t)syscall_read((int)args[1], (void*)args[2], (unsigned)args[3]);
      break;

    case SYS_CLOSE:
      validate_buffer_in_user_region(&args[1], sizeof(uint32_t));
      syscall_close((int)args[1]);
      break;
    
    case SYS_SBRK:
      validate_buffer_in_user_region(&args[1], sizeof(uint32_t));
      f->eax = (uint32_t)syscall_sbrk((intptr_t)args[1]);
      break;

    default:
      printf("Unimplemented system call: %d\n", (int)args[0]);
      break;
  }

  t->in_syscall = false;
}
