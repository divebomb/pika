// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <glog/logging.h>
#include "master_conn.h"
#include "binlog_receiver_thread.h"
#include "binlog_proxy.h"

extern BinlogProxy *g_binlog_proxy;

MasterConn::MasterConn(int fd, std::string ip_port,
                       BinlogReceiverThread* binlog_receiver)
      : RedisConn(fd, ip_port, NULL),
        self_thread_(binlog_receiver) {
}

void MasterConn::RestoreArgs() {
  raw_args_.clear();
  RedisAppendLen(raw_args_, argv_.size(), "*");
  PikaCmdArgsType::const_iterator it = argv_.begin();
  for ( ; it != argv_.end(); ++it) {
    RedisAppendLen(raw_args_, (*it).size(), "$");
    RedisAppendContent(raw_args_, *it);
  }
}

int MasterConn::DealMessage() {
  //no reply
  //eq set_is_reply(false);

  if (argv_.empty()) {
    return -2;
  }

  RestoreArgs();

  //g_binlog_proxy->logger_->Lock();
  g_binlog_proxy->logger()->Put(raw_args_);
  //g_binlog_proxy->logger_->Unlock();

  return 0;
}
