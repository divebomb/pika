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
#include "pika_proxy.h"
#include "binlog_const.h"
#include "binlog_conf.h"

void PikaProxy::ConnectRedis() {
  while (cli_ == NULL) {
    // Connect to redis
    cli_ = pink::NewRedisCli();
    cli_->set_connect_timeout(1000);
    slash::Status s = cli_->Connect(g_binlog_conf.forward_ip, g_binlog_conf.forward_port);
    if (!s.ok()) {
      cli_ = NULL;
      LOG(INFO) << "Can not connect to " << g_binlog_conf.forward_ip.data() << ":"
	            << g_binlog_conf.forward_port << ", status:" << s.ToString();
      continue;
    } else {
      // Connect success
      LOG(INFO) << "Connect to " << g_binlog_conf.forward_ip.data() << ":"
	            << g_binlog_conf.forward_port << ", status:" << s.ToString();
      // Authentication
      if (!g_binlog_conf.forward_passwd.empty()) {
        pink::RedisCmdArgsType argv, resp;
        std::string cmd;

        argv.push_back("AUTH");
        argv.push_back(g_binlog_conf.forward_passwd);
        pink::SerializeRedisCommand(argv, &cmd);
        slash::Status s = cli_->Send(&cmd);

        if (s.ok()) {
          s = cli_->Recv(&resp);
          if (resp[0] == "OK") {
            LOG(INFO) << "Authentic success";
          } else {
            cli_->Close();
            LOG(WARNING) << "Invalid password";
            cli_ = NULL;
            should_exit_ = true;
            return;
          }
        } else {
         cli_->Close();
          LOG(INFO) << s.ToString();
          cli_ = NULL;
          continue;
        }
      } else {
        // If forget to input password
        pink::RedisCmdArgsType argv, resp;
        std::string cmd;

        argv.push_back("PING");
        pink::SerializeRedisCommand(argv, &cmd);
        slash::Status s = cli_->Send(&cmd);

        if (s.ok()) {
          s = cli_->Recv(&resp);
          if (s.ok()) {
            if (resp[0] == "NOAUTH Authentication required.") {
              cli_->Close();
              LOG(WARNING) << "Authentication required";
              cli_ = NULL;
              should_exit_ = true;
              return;
            }
          } else {
            cli_->Close();
            LOG(INFO) <<  s.ToString();
            cli_ = NULL;
          }
        }
      }
    }
  }
}

int PikaProxy::SendRedisCommand(std::string &command) {
  // Send command
  slash::Status s = cli_->Send(&command);
  if (!s.ok()) {
    cli_->Close();
    LOG(WARNING) << "failed to send redis command " << command << " to forward redis/pika";
    cli_ = NULL;
    ConnectRedis();
  }

  return 0;
}

PikaProxy::PikaProxy(
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
  role_(PIKA_ROLE_PROXY),
  repl_state_(PIKA_REPL_NO_CONNECT),
  requirepass_(passwd),
  log_path_(log_path),
  dump_path_(dump_path),
  cli_(NULL),
  should_exit_(false) {

  pthread_rwlockattr_t attr;
  pthread_rwlockattr_init(&attr);
  pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
  pthread_rwlock_init(&rwlock_, &attr);
  
  //Init ip host
  if (!Init()) {
    LOG(FATAL) << "Init iotcl error";
  }

  // connect forward redis/pika
  ConnectRedis();
  if (should_exit_) {
    LOG(FATAL) << "Failed to connect forward redis/pika";
  }

  // Create thread
  binlog_receiver_thread_ = new BinlogReceiverThread(port_ + 1000, 1000);
  trysync_thread_ = new TrysyncThread();
  
  pthread_rwlock_init(&state_protector_, NULL);
  logger_ = new Binlog(log_path, 104857600);
}

PikaProxy::~PikaProxy() {
  LOG(INFO) << "Ending...";
  delete trysync_thread_;
  delete ping_thread_;
  sleep(1);
  delete binlog_receiver_thread_;

  delete logger_;

  delete cli_;

  pthread_rwlock_destroy(&state_protector_);
  pthread_rwlock_destroy(&rwlock_);

  DLOG(INFO) << "PikaProxy " << pthread_self() << " exit!!!";
}

bool PikaProxy::Init() {
  // DLOG(INFO) << "host: " << host_ << " port: " << port_;
  return true;
}

void PikaProxy::Cleanup() {
  // shutdown server
//  if (g_binlog_conf->daemonize()) {
//    unlink(g_binlog_conf->pidfile().c_str());
//  }

  delete this;
  ::google::ShutdownGoogleLogging();
}

void PikaProxy::Start() {
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

