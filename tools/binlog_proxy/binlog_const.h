#ifndef BINLOG_CONST_H_
#define BINLOG_CONST_H_

//repl_state_
//#define PIKA_REPL_NO_CONNECT 0
//#define PIKA_REPL_CONNECT 1
//#define PIKA_REPL_CONNECTING 2
//#define PIKA_REPL_CONNECTED 3
//#define PIKA_REPL_WAIT_DBSYNC 4
//#define PIKA_REPL_ERROR 5
#define PIKA_REPL_RETRANS 6

#include <string>
std::string PikaState(int state);

std::string PikaRole(int role) ;

// //role
// #define PIKA_ROLE_SINGLE 0
// #define PIKA_ROLE_SLAVE 1
// #define PIKA_ROLE_MASTER 2
// #define PIKA_ROLE_DOUBLE_MASTER 3
#define PIKA_ROLE_PROXY 4


#endif
