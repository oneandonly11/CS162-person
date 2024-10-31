#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <signal.h>
#include <sys/wait.h>
#include <termios.h>
#include <unistd.h>

#include "tokenizer.h"

/* Convenience macro to silence compiler warnings about unused function parameters. */
#define unused __attribute__((unused))

/* Whether the shell is connected to an actual terminal or not. */
bool shell_is_interactive;

/* File descriptor for the shell input */
int shell_terminal;

/* Terminal mode settings for the shell */
struct termios shell_tmodes;

/* Process group id for the shell */
pid_t shell_pgid;

int cmd_exit(struct tokens* tokens);
int cmd_help(struct tokens* tokens);
int cmd_cd(struct tokens* tokens);
int cmd_pwd(struct tokens* tokens);
int cmd_wait(struct tokens* tokens);

/* Built-in command functions take token array (see parse.h) and return int */
typedef int cmd_fun_t(struct tokens* tokens);

/* Built-in command struct and lookup table */
typedef struct fun_desc {
  cmd_fun_t* fun;
  char* cmd;
  char* doc;
} fun_desc_t;

typedef struct pipe {
  int fd[2];
} pipe_t;

fun_desc_t cmd_table[] = {
    {cmd_help, "?", "show this help menu"},
    {cmd_exit, "exit", "exit the command shell"},
    {cmd_cd, "cd", "change the current working directory"},
    {cmd_pwd, "pwd", "print the current working directory"},
    {cmd_wait, "wait", "wait for background processes to finish"},
};

/* Prints a helpful description for the given command */
int cmd_help(unused struct tokens* tokens) {
  for (unsigned int i = 0; i < sizeof(cmd_table) / sizeof(fun_desc_t); i++)
    printf("%s - %s\n", cmd_table[i].cmd, cmd_table[i].doc);
  return 1;
}

/* Exits this shell */
int cmd_exit(unused struct tokens* tokens) { exit(0); }

/* Changes the current working directory */
int cmd_cd(struct tokens* tokens) {
  if (tokens_get_length(tokens) != 2) {
    fprintf(stderr, "cd: missing argument\n");
    return -1;
  }

  if (chdir(tokens_get_token(tokens, 1)) == -1) {
    fprintf(stderr, "cd: %s: %s\n", tokens_get_token(tokens, 1), strerror(errno));
    return -1;
  }

  return 0;
}

/* Prints the current working directory */
int cmd_pwd(unused struct tokens* tokens) {
  char cwd[4096];
  if (getcwd(cwd, sizeof(cwd)) == NULL) {
    fprintf(stderr, "pwd: %s\n", strerror(errno));
    return -1;
  }

  fprintf(stdout, "%s\n", cwd);
  return 0;
}

int cmd_wait(unused struct tokens* tokens) {
  int status;
  while(wait(&status) > 0);
  return 0;
}

/* Looks up the built-in command, if it exists. */
int lookup(char cmd[]) {
  for (unsigned int i = 0; i < sizeof(cmd_table) / sizeof(fun_desc_t); i++)
    if (cmd && (strcmp(cmd_table[i].cmd, cmd) == 0))
      return i;
  return -1;
}


/* Intialization procedures for this shell */
void init_shell() {
  /* Our shell is connected to standard input. */
  shell_terminal = STDIN_FILENO;

  /* Check if we are running interactively */
  shell_is_interactive = isatty(shell_terminal);

  struct sigaction sa = {
      .sa_handler = SIG_IGN,
  };

  sigaction(SIGINT, &sa, NULL);
  sigaction(SIGTTOU, &sa, NULL);
  sigaction(SIGTTIN, &sa, NULL);
  sigaction(SIGTSTP, &sa, NULL);


  if (shell_is_interactive) {
    /* If the shell is not currently in the foreground, we must pause the shell until it becomes a
     * foreground process. We use SIGTTIN to pause the shell. When the shell gets moved to the
     * foreground, we'll receive a SIGCONT. */
    while (tcgetpgrp(shell_terminal) != (shell_pgid = getpgrp()))
      kill(-shell_pgid, SIGTTIN);

    /* Saves the shell's process id */
    shell_pgid = getpid();

    /* Take control of the terminal */
    tcsetpgrp(shell_terminal, shell_pgid);

    /* Save the current termios to a variable, so it can be restored later. */
    tcgetattr(shell_terminal, &shell_tmodes);
  }
}

void get_args(struct tokens* tokens, char** args, int index, bool background) {

  int args_index = 0;
  int num_tokens = tokens_get_length(tokens);
  if(background) {
    num_tokens--;
  }
   for (int i = 0; i < num_tokens; i++) {
    if (index > 0) {
      if (tokens_get_token(tokens, i)[0] == '|') {
        index--;
      }
      continue;
    } else {
      if (tokens_get_token(tokens, i)[0] == '|') {
        break;
      }
    }
    args[args_index] = tokens_get_token(tokens, i);
    args_index++;
  }
  args[args_index] = NULL;
  
}

void redirect(char** args) {
  int i = 0;
  while(args[i] != NULL) {
    if (strcmp(args[i], ">") == 0) {
      if(args[i + 1] == NULL) {
        fprintf(stderr, ">: missing argument\n");
        exit(1);
      }
      freopen(args[i + 1], "w", stdout);
      args[i] = NULL;
    } else if (strcmp(args[i], "<") == 0) {
      if(args[i + 1] == NULL) {
        fprintf(stderr, "<: missing argument\n");
        exit(1);
      }
      freopen(args[i + 1], "r", stdin);
      args[i] = NULL;
    }
    i++;
  }
}

int create_pipe(pid_t* pids, int num_pipe) {
  int num_processes = num_pipe + 1;
  pipe_t pipes[num_pipe];
  for (size_t i = 0; i < num_pipe; i++) {
    if (pipe(pipes[i].fd) == -1) {
      fprintf(stderr, "pipe: %s\n", strerror(errno));
      return -1;
    }
  }
  int index = -1;
  int pgid = getpid();
  for (size_t i = 0; i < num_processes; i++) {
    int pid = fork();
    if(pid == 0) {
      setpgid(getpid(), pgid);
      struct sigaction sa = {
        .sa_handler = SIG_DFL,
      };
      sigaction(SIGINT, &sa, NULL);
      sigaction(SIGTTOU, &sa, NULL);
      sigaction(SIGTTIN, &sa, NULL);
      sigaction(SIGTSTP, &sa, NULL);
      index = i;
      break;
    }
    else{
      pids[i] = pid;
    }
  }
  if(index == 0) {
    dup2(pipes[0].fd[1], STDOUT_FILENO);
  } else if (index == num_processes - 1) {
    dup2(pipes[num_pipe - 1].fd[0], STDIN_FILENO);
  } else if(index > 0 && index < num_processes - 1) {
    dup2(pipes[index - 1].fd[0], STDIN_FILENO);
    dup2(pipes[index].fd[1], STDOUT_FILENO);
  }
  for (size_t i = 0; i < num_pipe; i++) {
      close(pipes[i].fd[0]);
      close(pipes[i].fd[1]);
    }
  return index;
  
  
}

bool is_background(struct tokens* tokens) {
  if (tokens_get_length(tokens) < 1) {
    return false;
  }
  return strcmp(tokens_get_token(tokens, tokens_get_length(tokens) - 1), "&") == 0;
}

int exec_program(struct tokens* tokens) {
  bool background = is_background(tokens);
  if (tokens_get_length(tokens) < 1)
  {
    return 0;
  }
  int num_pipes = 0;
  for (size_t i = 0; i < tokens_get_length(tokens); i++) {
    if (strcmp(tokens_get_token(tokens, i), "|") == 0) {
      num_pipes++;
    }
  }
  int num_processes = num_pipes + 1;
  pid_t pids[num_processes];
  int index = create_pipe(pids, num_pipes);
  if (index >= 0) {
    
    char* args[tokens_get_length(tokens) + 1];
    get_args(tokens, args, index, background);
    redirect(args);
    char* program = args[0];

    if(program[0] == '/'){
      execv(program, args);
    } else {
      char* path = getenv("PATH");
      char* path_copy = strdup(path);
      char* path_token = strtok(path_copy, ":");
      while (path_token != NULL) {
        char* full_path = malloc(strlen(path_token) + strlen(program) + 2);
        strcpy(full_path, path_token);
        strcat(full_path, "/");
        strcat(full_path, program);
        execv(full_path, args);
        free(full_path);
        path_token = strtok(NULL, ":");
      }
      free(path_copy);
    }
    fprintf(stderr, "%s: %s\n", program, strerror(errno));
    exit(1);
  } 
  else {
    if(!background) {
      int status;
      for(size_t i = 0; i < num_processes; i++) {
        waitpid(pids[i], &status, 0);
      }
      if (WIFEXITED(status)) {
        return WEXITSTATUS(status);
      } else {
        return -1;
      }
    }
  }
  return 0;
}

int main(unused int argc, unused char* argv[]) {
  init_shell();

  static char line[4096];
  int line_num = 0;

  /* Please only print shell prompts when standard input is not a tty */
  if (shell_is_interactive)
    fprintf(stdout, "%d: ", line_num);

  while (fgets(line, 4096, stdin)) {
    /* Split our line into words. */
    struct tokens* tokens = tokenize(line);

    /* Find which built-in function to run. */
    int fundex = lookup(tokens_get_token(tokens, 0));

    if (fundex >= 0) {
      cmd_table[fundex].fun(tokens);
    } else {
      exec_program(tokens);
    }

    if (shell_is_interactive)
      /* Please only print shell prompts when standard input is not a tty */
      fprintf(stdout, "%d: ", ++line_num);

    /* Clean up memory */
    tokens_destroy(tokens);
  }

  return 0;
}
