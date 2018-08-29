#ifndef BINLOG_CONF_H_
#define BINLOG_CONF_H_

#include <string>

class BinlogConf {
public:
  BinlogConf() {
    local_ip = "127.0.0.1";
    local_port = 0;
    master_ip = "127.0.0.1";
    master_port = 0;
    filenum = size_t(UINT32_MAX); // src/pika_trysync_thread.cc:48
    offset = 0;
    log_path = "./log/";
    dump_path = "./rsync_dump/";
  }

public:
    size_t filenum;
    size_t offset;
    std::string local_ip;
    int local_port;
    std::string master_ip;
    int master_port;
    std::string passwd;
    std::string log_path;
    std::string dump_path;
};

extern BinlogConf g_binlog_conf;

#endif
