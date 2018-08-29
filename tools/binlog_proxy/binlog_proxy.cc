// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <glog/logging.h>
#include <assert.h>
#include <sys/ioctl.h>
#include <net/if.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "slash/include/env.h"
#include "slash/include/slash_string.h"
#include "slash/include/rsync.h"
#include "binlog_proxy.h"

BinlogProxy::BinlogProxy(
    int64_t filenum,
    int64_t offset,
    std::string& local_ip,
    int port,
    std::string& master_ip,
    int master_port,
    std::string& passwd,
    std::string& log_path,
    std::string& dump_path
  ) :
  sid_(0),
  filenum_(filenum),
  offset_(offset) ,
  ping_thread_(NULL), 
  host_(local_ip),
  port_(port),
  master_ip_(master_ip),
  master_port_(master_port),
  master_connection_(0),
  role_(PIKA_ROLE_SINGLE),
  repl_state_(PIKA_REPL_NO_CONNECT),
  requirepass_(passwd),
  log_path_(log_path),
  dump_path_(dump_path) {

  pthread_rwlockattr_t attr;
  pthread_rwlockattr_init(&attr);
  pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
  pthread_rwlock_init(&rwlock_, &attr);
  
  //Init ip host
  if (!Init()) {
    LOG(FATAL) << "Init iotcl error";
  }

  // Create thread
  binlog_receiver_thread_ = new BinlogReceiverThread(port_ + 1000, 1000);
  trysync_thread_ = new TrysyncThread();
  
  pthread_rwlock_init(&state_protector_, NULL);
  logger_ = new Binlog(log_path, 104857600);
}

BinlogProxy::~BinlogProxy() {
  LOG(INFO) << "Ending...";
  delete trysync_thread_;
  delete ping_thread_;
  sleep(1);
  delete binlog_receiver_thread_;

  delete logger_;
  pthread_rwlock_destroy(&state_protector_);
  pthread_rwlock_destroy(&rwlock_);

  DLOG(INFO) << "BinlogProxy " << pthread_self() << " exit!!!";
}

bool BinlogProxy::Init() {
  // DLOG(INFO) << "host: " << host_ << " port: " << port_;
  return true;
}

void BinlogProxy::Cleanup() {
  // shutdown server
//  if (g_pika_conf->daemonize()) {
//    unlink(g_pika_conf->pidfile().c_str());
//  }

  delete this;
  ::google::ShutdownGoogleLogging();
}

void BinlogProxy::Start() {
  trysync_thread_->StartThread();
  binlog_receiver_thread_->StartThread();

  if (filenum_ >= 0 && filenum_ != UINT32_MAX && offset_ >= 0) {
    logger_->SetProducerStatus(filenum_, offset_);
  }
  SetMaster(master_ip_, master_port_);

  mutex_.Lock();
  mutex_.Lock();
  mutex_.Unlock();
  DLOG(INFO) << "Goodbye...";
  Cleanup();
}

bool BinlogProxy::SetMaster(std::string& master_ip, int master_port) {
  slash::RWLock l(&state_protector_, true);
  if ((role_ ^ PIKA_ROLE_SLAVE) && repl_state_ == PIKA_REPL_NO_CONNECT) {
    master_ip_ = master_ip;
    master_port_ = master_port;
    role_ |= PIKA_ROLE_SLAVE;
    repl_state_ = PIKA_REPL_CONNECT;
    DLOG(INFO) << "set role_ = PIKA_ROLE_SLAVE, repl_state_ = PIKA_REPL_CONNECT";
    return true;
  }
  return false;
}

bool BinlogProxy::ShouldConnectMaster() {
  slash::RWLock l(&state_protector_, false);
  DLOG(INFO) << "repl_state: " << repl_state_ << " role: " << role_ << " master_connection: " << master_connection_;
  if (repl_state_ == PIKA_REPL_CONNECT) {
    return true;
  }
  return false;
}

void BinlogProxy::ConnectMasterDone() {
  slash::RWLock l(&state_protector_, true);
  if (repl_state_ == PIKA_REPL_CONNECT) {
    repl_state_ = PIKA_REPL_CONNECTING;
  }
}

bool BinlogProxy::ShouldStartPingMaster() {
  slash::RWLock l(&state_protector_, false);
  DLOG(INFO) << "ShouldStartPingMaster: master_connection " << master_connection_ << " repl_state " << repl_state_;
  if (repl_state_ == PIKA_REPL_CONNECTING && master_connection_ < 2) {
    return true;
  }
  return false;
}

void BinlogProxy::MinusMasterConnection() {
  slash::RWLock l(&state_protector_, true);
  if (master_connection_ > 0) {
    if ((--master_connection_) <= 0) {
      // two connection with master has been deleted
      if (role_ & PIKA_ROLE_SLAVE) {
        repl_state_ = PIKA_REPL_CONNECT; // not change by slaveof no one, so set repl_state = PIKA_REPL_CONNECT, continue to connect master
      } else {
        repl_state_ = PIKA_REPL_NO_CONNECT; // change by slaveof no one, so set repl_state = PIKA_REPL_NO_CONNECT, reset to SINGLE state
      }
      master_connection_ = 0;
    }
  }
}

void BinlogProxy::PlusMasterConnection() {
  slash::RWLock l(&state_protector_, true);
  if (master_connection_ < 2) {
    if ((++master_connection_) >= 2) {
      // two connection with master has been established
      repl_state_ = PIKA_REPL_CONNECTED;
      LOG(INFO) << "Start Sync...";
      master_connection_ = 2;
    }
  }
}

bool BinlogProxy::ShouldAccessConnAsMaster(const std::string& ip) {
  slash::RWLock l(&state_protector_, false);
  DLOG(INFO) << "ShouldAccessConnAsMaster, repl_state_: " << repl_state_ << " ip: " << ip << " master_ip: " << master_ip_;
  if (repl_state_ != PIKA_REPL_NO_CONNECT && ip == master_ip_) {
    return true;
  }
  return false;
}

void BinlogProxy::RemoveMaster() {

  {
  slash::RWLock l(&state_protector_, true);
  repl_state_ = PIKA_REPL_NO_CONNECT;
  role_ &= ~PIKA_ROLE_SLAVE;
  master_ip_ = "";
  master_port_ = -1;
  }
  if (ping_thread_ != NULL) {
    int err = ping_thread_->StopThread();
    if (err != 0) {
      std::string msg = "can't join thread " + std::string(strerror(err));
      LOG(WARNING) << msg;
    }
    delete ping_thread_;
    ping_thread_ = NULL;
  }
}

bool BinlogProxy::WaitingDBSync() {
  slash::RWLock l(&state_protector_, false);
  DLOG(INFO) << "repl_state: " << repl_state_ << " role: " << role_ << " master_connection: " << master_connection_;
  if (repl_state_ == PIKA_REPL_WAIT_DBSYNC) {
    return true;
  }
  return false;
}

void BinlogProxy::NeedWaitDBSync() {
  slash::RWLock l(&state_protector_, true);
  repl_state_ = PIKA_REPL_WAIT_DBSYNC;
}

void BinlogProxy::WaitDBSyncFinish() {
  slash::RWLock l(&state_protector_, true);
  if (repl_state_ == PIKA_REPL_WAIT_DBSYNC) {
    repl_state_ = PIKA_REPL_CONNECT;
  }
}

