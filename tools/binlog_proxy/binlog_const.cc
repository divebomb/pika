#include "binlog_const.h"

#include "include/pika_define.h"

std::string PikaState(int state) {
  switch (state) {
    case PIKA_REPL_NO_CONNECT:
    return "PIKA_REPL_NO_CONNECT";

    case PIKA_REPL_CONNECT:
    return "PIKA_REPL_CONNECT";

    case PIKA_REPL_CONNECTING:
    return "PIKA_REPL_CONNECTING";

    case PIKA_REPL_CONNECTED:
    return "PIKA_REPL_CONNECTED";

    case PIKA_REPL_WAIT_DBSYNC:
    return "PIKA_REPL_WAIT_DBSYNC";

    case PIKA_REPL_ERROR:
    return "PIKA_REPL_ERROR";

    case PIKA_REPL_RETRANS:
    return "PIKA_REPL_RETRANS";

    default:
    return "PIKA_REPL_UNKNOWN";
  }

  return "PIKA_REPL_UNKNOWN";
}

std::string PikaRole(int role) {
  switch (role) {
    case PIKA_ROLE_SINGLE:
    return "PIKA_ROLE_SINGLE";

    case PIKA_ROLE_SLAVE:
    return "PIKA_ROLE_SLAVE";

    case PIKA_ROLE_MASTER:
    return "PIKA_ROLE_MASTER";

    case PIKA_ROLE_DOUBLE_MASTER:
    return "PIKA_ROLE_DOUBLE_MASTER";

    case PIKA_ROLE_PROXY:
    return "PIKA_ROLE_PROXY";

    default:
    return "PIKA_ROLE_UNKNOWN";
  }

  return "PIKA_ROLE_UNKNOWN";
}

