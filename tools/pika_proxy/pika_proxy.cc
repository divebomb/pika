// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <functional>

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
#include "pika_proxy.h"
#include "binlog_const.h"
#include "proxy_conf.h"

PikaProxy::PikaProxy(std::string& master_ip, int master_port, std::string& passwd)
  : sid_(0),
  ping_thread_(NULL), 
  master_ip_(master_ip),
  master_port_(master_port),
  master_connection_(0),
  role_(PIKA_ROLE_PROXY),
  repl_state_(PIKA_REPL_NO_CONNECT),
  requirepass_(passwd),
  // cli_(NULL),
  should_exit_(false) {

  pthread_rwlockattr_t attr;
  pthread_rwlockattr_init(&attr);
  pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
  pthread_rwlock_init(&rwlock_, &attr);
  
  //Init ip host
  if (!Init()) {
    LOG(FATAL) << "Init iotcl error";
  }

  // Create redis sender 
  size_t thread_num = g_proxy_conf.forward_thread_num;
  for (size_t i = 0; i < thread_num; i++) { 
    senders_.emplace_back(new RedisSender(int(i), g_proxy_conf.forward_ip,
	       g_proxy_conf.forward_port, g_proxy_conf.forward_passwd));
  }

  // Create thread
  binlog_receiver_thread_ = new BinlogReceiverThread(g_proxy_conf.local_port + 1000, 1000);
  trysync_thread_ = new TrysyncThread();
  
  pthread_rwlock_init(&state_protector_, NULL);
  logger_ = new Binlog(g_proxy_conf.log_path, 104857600);
}

PikaProxy::~PikaProxy() {
  LOG(INFO) << "Ending...";
  delete trysync_thread_;
  delete ping_thread_;
  sleep(1);
  delete binlog_receiver_thread_;

  delete logger_;


  pthread_rwlock_destroy(&state_protector_);
  pthread_rwlock_destroy(&rwlock_);

  DLOG(INFO) << "PikaProxy " << pthread_self() << " exit!!!";
}

bool PikaProxy::Init() {
  // DLOG(INFO) << "host: " << g_proxy_conf.local_ip << " port: " << g_proxy_conf.local_port;
  return true;
}

void PikaProxy::Cleanup() {
  // shutdown server
//  if (g_proxy_conf->daemonize()) {
//    unlink(g_proxy_conf->pidfile().c_str());
//  }

  // sender_->Stop();
  // sender_->JoinThread();
  // delete cli_;
  // delete sender_;
  size_t thread_num = g_proxy_conf.forward_thread_num;
  for(size_t i = 0; i < thread_num; i++) {
    senders_[i]->Stop();
  }
  for (size_t i = 0; i < thread_num; i++) {
    senders_[i]->JoinThread();
  }
  int64_t replies = 0;
  for (size_t i = 0; i < thread_num; i++) {
    replies += senders_[i]->elements();
    delete senders_[i];
  }
  DLOG(INFO) << "=============== Syncing =====================" << std::endl;
  DLOG(INFO) << "Total replies : " << replies << " received from redis server";

  delete this; // PikaProxy is a global object
  ::google::ShutdownGoogleLogging();
}

void PikaProxy::Start() {
  // start redis sender threads
  size_t thread_num = g_proxy_conf.forward_thread_num;
  for (size_t i = 0; i < thread_num; i++) {
    senders_[i]->StartThread();
  }

  // sender_->StartThread();
  trysync_thread_->StartThread();
  binlog_receiver_thread_->StartThread();

  // if (g_proxy_conf.filenum >= 0 && g_proxy_conf.filenum != UINT32_MAX && g_proxy_conf.offset >= 0) {
  if (g_proxy_conf.filenum != UINT32_MAX) {
    logger_->SetProducerStatus(g_proxy_conf.filenum, g_proxy_conf.offset);
  }
  SetMaster(master_ip_, master_port_);

  mutex_.Lock();
  mutex_.Lock();
  mutex_.Unlock();
  DLOG(INFO) << "Goodbye...";
  Cleanup();
}

void PikaProxy::Stop() {
  mutex_.Unlock();
}

int PikaProxy::SendRedisCommand(std::string &command, std::string &key) {
  // Send command
  size_t idx = std::hash<std::string>()(key) % g_proxy_conf.forward_thread_num;
  senders_[idx]->SendRedisCommand(command);
  
  return 0;
}

bool PikaProxy::SetMaster(std::string& master_ip, int master_port) {
  slash::RWLock l(&state_protector_, true);
  if ((role_ ^ PIKA_ROLE_SLAVE) && repl_state_ == PIKA_REPL_NO_CONNECT) {
    master_ip_ = master_ip;
    master_port_ = master_port;
    // role_ |= PIKA_ROLE_SLAVE;
    role_ = PIKA_ROLE_PROXY;
    repl_state_ = PIKA_REPL_CONNECT;
    DLOG(INFO) << "set role_ = PIKA_ROLE_PROXY, repl_state_ = PIKA_REPL_CONNECT";
    return true;
  }
  return false;
}

bool PikaProxy::ShouldConnectMaster() {
  slash::RWLock l(&state_protector_, false);
  // DLOG(INFO) << "repl_state: " << PikaState(repl_state_)
  //            << " role: " << PikaRole(role_)
  //   		 << " master_connection: " << master_connection_;
  if (repl_state_ == PIKA_REPL_CONNECT) {
    return true;
  }
  return false;
}

void PikaProxy::ConnectMasterDone() {
  slash::RWLock l(&state_protector_, true);
  if (repl_state_ == PIKA_REPL_CONNECT) {
    repl_state_ = PIKA_REPL_CONNECTING;
  }
}

bool PikaProxy::ShouldStartPingMaster() {
  slash::RWLock l(&state_protector_, false);
  DLOG(INFO) << "ShouldStartPingMaster: master_connection " << master_connection_
             << " repl_state " << PikaState(repl_state_);
  if (repl_state_ == PIKA_REPL_CONNECTING && master_connection_ < 2) {
    return true;
  }

  return false;
}

void PikaProxy::MinusMasterConnection() {
  slash::RWLock l(&state_protector_, true);
  if (master_connection_ > 0) {
    if ((--master_connection_) <= 0) {
      // two connection with master has been deleted
      if ((role_ & PIKA_ROLE_SLAVE) || (role_ & PIKA_ROLE_PROXY)) {
		// not change by slaveof no one, so set repl_state = PIKA_REPL_CONNECT, continue to connect master
        repl_state_ = PIKA_REPL_CONNECT;
      } else {
		// change by slaveof no one, so set repl_state = PIKA_REPL_NO_CONNECT, reset to SINGLE state
        repl_state_ = PIKA_REPL_NO_CONNECT;
      }
      master_connection_ = 0;
    }
  }
}

void PikaProxy::PlusMasterConnection() {
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

bool PikaProxy::ShouldAccessConnAsMaster(const std::string& ip) {
  slash::RWLock l(&state_protector_, false);
  DLOG(INFO) << "ShouldAccessConnAsMaster, repl_state_: " << PikaState(repl_state_)
         << " ip: " << ip << " master_ip: " << master_ip_;
  if (repl_state_ != PIKA_REPL_NO_CONNECT && ip == master_ip_) {
    return true;
  }
  return false;
}

void PikaProxy::RemoveMaster() {
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

bool PikaProxy::IsWaitingDBSync() {
  slash::RWLock l(&state_protector_, false);
  // DLOG(INFO) << "repl_state: " << PikaState(repl_state_)
  //     << " role: " << PikaRole(role_) << " master_connection: " << master_connection_;
  if (repl_state_ == PIKA_REPL_WAIT_DBSYNC) {
    return true;
  }
  return false;
}

void PikaProxy::NeedWaitDBSync() {
  slash::RWLock l(&state_protector_, true);
  repl_state_ = PIKA_REPL_WAIT_DBSYNC;
}

void PikaProxy::WaitDBSyncFinish() {
  slash::RWLock l(&state_protector_, true);
  if (repl_state_ == PIKA_REPL_WAIT_DBSYNC) {
    repl_state_ = PIKA_REPL_CONNECT;
  }
}

