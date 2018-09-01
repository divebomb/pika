// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <glog/logging.h>
#include "master_conn.h"
#include "binlog_receiver_thread.h"
#include "pika_proxy.h"

extern PikaProxy *g_pika_proxy;

MasterConn::MasterConn(int fd, std::string ip_port,
                       BinlogReceiverThread* binlog_receiver)
      : RedisConn(fd, ip_port, NULL),
        self_thread_(binlog_receiver) {
}

void MasterConn::RestoreArgs() {
  raw_args_.clear();
  size_t num = argv_.size() - 4;
  RedisAppendLen(raw_args_, num, "*");
  PikaCmdArgsType::const_iterator it = argv_.begin();
  for (size_t idx = 0; idx < num && it != argv_.end(); ++ it, ++ idx) {
    RedisAppendLen(raw_args_, (*it).size(), "$");
    RedisAppendContent(raw_args_, *it);
  }
}

// int MasterConn::DealMessage() {
int MasterConn::DealMessage(pink::RedisCmdArgsType& argv, std::string* response) {
  //no reply
  //eq set_is_reply(false);

  // if (argv.empty()) {
  if (argv.size() < 5) { // special chars: __PIKA_X#$SKGI\r\n1\r\n[\r\n
    // return -2;
	return 0;
  }

  // if (argv[0] == "auth") {
  //	return 0;
  // }

  RestoreArgs();
  // //g_pika_proxy->logger_->Lock();
  // g_pika_proxy->logger()->Put(raw_args_);
  // //g_pika_proxy->logger_->Unlock();
 
  int ret = g_pika_proxy->SendRedisCommand(raw_args_);
  if (ret != 0) {
    DLOG(WARNING) << "send redis command:" << raw_args_ << ", ret:%d" << ret;
  }

  return 0;
}
