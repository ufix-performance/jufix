
#ifndef _U_FIX_PORTABLE_
#define _U_FIX_PORTABLE_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define BILLION 1000000000
#define MILLION 1000000
#define THOUSAND 1000
#define socket_send(x, y, z) send(x, y, z, MSG_NOSIGNAL)

typedef unsigned long long ULONG;

typedef unsigned int UINT;

#ifdef _POSIX_
#include <pthread.h>
#include <errno.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <poll.h>

#define unlikely(expr) __builtin_expect(!!(expr), 0)
#define likely(expr) __builtin_expect(!!(expr), 1)

#define pthread_main void *
#define thread_create(x, y, z) pthread_create(x, NULL, y, z)

#define socket_handle int
#define SOCKET_INVALID -1
#define SOCKET_ERROR -1 
#define API_SUCCESS 0
#define API_ERROR -1

#define _READ_MEMORY_BAR_
#define _WRITE_MEMORY_BAR_
#define _MEMORY_BAR_ asm volatile("mfence" ::: "memory")

#define get_errno() errno


#define msleep(x) usleep(x*THOUSAND)
#define socket_close(x) close(x)

#define DIR_DELIM '/'

inline void pthread_wait(pthread_cond_t * event, pthread_mutex_t * mutex, ULONG time_ms, int * status) {   
  if (time_ms == 0) {
    pthread_cond_wait(event, mutex);
  } else {
    timespec ts;    
    clock_gettime(CLOCK_MONOTONIC, &ts);
    
    ULONG nsecs = ((ULONG) ts.tv_sec)*BILLION + ts.tv_nsec + time_ms*MILLION;
    ts.tv_sec =  nsecs/BILLION;    
    ts.tv_nsec = nsecs%BILLION;
    *status = pthread_cond_timedwait(event, mutex, &ts);
  }
}

inline int socket_set_recv_timeout(socket_handle socket, ULONG ms) {
  struct timeval tv;
  tv.tv_sec = ms/THOUSAND; 
  tv.tv_usec = 0L;
  return setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO, (char *)&tv, sizeof(struct timeval));
}

inline int socket_set_send_timeout(socket_handle socket, ULONG ms) {
  struct timeval tv;
  tv.tv_sec = ms/THOUSAND;
  tv.tv_usec = 0L;
  return setsockopt(socket, SOL_SOCKET, SO_SNDTIMEO, (char *)&tv, sizeof(struct timeval));
}

#elif _WINDOWS_
#define pthread_main unsigned int _stdcall
#define thread_create(x, y, z) _beginthreadex(NULL, 0, y, z, 0, x) 

#define socket_handle SOCKET
#define SOCKET_INVALID INVALID_SOCKET
#define get_errno() GetLastError()
#define msleep(x) Sleep(x)
#define socket_close(x) closesocket(x)

#define DIR_DELIM '\\'

#define pthread_cond_t HANDLE
#define pthread_mutex_t int 
#define pthread_cond_init(x, y) *x=CreateEvent(y, false, false, NULL)
#define pthread_mutex_init(x, y) {}

inline void pthread_wait(pthread_cond_t event, pthread_mutex_t mutex, ULONG time_ms, int * status) {
  *status = WaitForSingleObject(event, time_ms);
}

inline int socket_set_recv_timeout(socket_handle socket, ULONG s) {  
  int timeout = s;
  return setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO, (const char*) &timeout, sizeof(int));        
}

inline int socket_set_send_timeout(socket_handle socket, ULONG s) {
  int timeout = s;
  return setsockopt(socket, SOL_SOCKET, SO_SNDTIMEO, (const char*) &timeout, sizeof(int));   
}  
#endif

#endif
