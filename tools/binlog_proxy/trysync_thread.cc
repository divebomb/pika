// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <fstream>
#include <glog/logging.h>
#include <poll.h>
#include <sys/types.h>
#include <unistd.h>
#include <signal.h>

#include "slash/include/slash_status.h"
#include "slash/include/rsync.h"
#include "slaveping_thread.h"

#include "include/pika_define.h"

#include "trysync_thread.h"
#include "binlog_proxy.h"
#include "binlog_conf.h"

extern BinlogProxy* g_binlog_proxy;

TrysyncThread::~TrysyncThread() {
  StopThread();
  delete cli_;
  DLOG(INFO) << " Trysync thread " << pthread_self() << " exit!!!";
}

void TrysyncThread::PrepareRsync() {
  std::string db_sync_path = g_binlog_conf.dump_path;
  slash::StopRsync(db_sync_path);
  slash::CreatePath(db_sync_path);
  slash::CreatePath(db_sync_path + "kv");
  slash::CreatePath(db_sync_path + "hash");
  slash::CreatePath(db_sync_path + "list");
  slash::CreatePath(db_sync_path + "set");
  slash::CreatePath(db_sync_path + "zset");
}

bool TrysyncThread::Send() {
  pink::RedisCmdArgsType argv;
  std::string wbuf_str;
  std::string requirepass = g_binlog_proxy->requirepass();
  if (requirepass != "") {
    argv.push_back("auth");
    argv.push_back(requirepass);
    pink::SerializeRedisCommand(argv, &wbuf_str);
  }

  argv.clear();
  std::string tbuf_str;
  argv.push_back("trysync");
  // argv.push_back(g_binlog_proxy->host());
  // argv.push_back(std::to_string(g_binlog_proxy->port()));
  argv.push_back(g_binlog_conf.local_ip);
  argv.push_back(std::to_string(g_binlog_conf.local_port));
  uint32_t filenum;
  uint64_t pro_offset;
  g_binlog_proxy->logger()->GetProducerStatus(&filenum, &pro_offset);
  LOG(WARNING) << "producer filenum: " << filenum << ", producer offset:" << pro_offset;
  
  argv.push_back(std::to_string(filenum));
  argv.push_back(std::to_string(pro_offset));
  
  pink::SerializeRedisCommand(argv, &tbuf_str);

  wbuf_str.append(tbuf_str);
  // DLOG(DEBUG) << wbuf_str;

  slash::Status s;
  s = cli_->Send(&wbuf_str);
  if (!s.ok()) {
    LOG(WARNING) << "Connect master, Send, error: " <<strerror(errno);
    return false;
  }
  return true;
}

// if send command {trysync slaveip slaveport 0 0}, the reply = wait.
// if send command {trysync slaveip slaveport 11 38709514}, the reply = "sid:.
// it means that slave sid is allocated by master.
bool TrysyncThread::RecvProc() {
  bool should_auth = g_binlog_proxy->requirepass() == "" ? false : true;
  bool is_authed = false;
  slash::Status s;
  std::string reply;

  pink::RedisCmdArgsType argv;
  while (1) {
    s = cli_->Recv(&argv);
    if (!s.ok()) {
      LOG(WARNING) << "Connect master, Recv, error: " <<strerror(errno);
      return false;
    }

    reply = argv[0];
    DLOG(INFO) << "Reply from master after trysync: " << reply;
    if (!is_authed && should_auth) {
      if (kInnerReplOk != slash::StringToLower(reply)) {
        g_binlog_proxy->RemoveMaster();
        return false;
      }
      is_authed = true;
    } else {
      // pinfo("xxxxxx argv size %zu, reply %s", argv.size(), reply.data());
      if (argv.size() == 1 &&
          slash::string2l(reply.data(), reply.size(), &sid_)) {
        // Luckly, I got your point, the sync is comming
        DLOG(INFO) << "Recv sid from master: " << sid_;
        g_binlog_proxy->SetSid(sid_);
        break;
      }

      // Failed
      if (reply == kInnerReplWait) {
        // You can't sync this time, but may be different next time,
        // This may happened when
        // 1, Master do bgsave first.
        // 2, Master waiting for an existing bgsaving process
        // 3, Master do dbsyncing
        LOG(INFO) << "Need wait to sync";
        g_binlog_proxy->NeedWaitDBSync();
        // break;
      } else {
        LOG(INFO) << "Sync Error, Quit";
        kill(getpid(), SIGQUIT);
        g_binlog_proxy->RemoveMaster();
      }
      return false;
    }
  }

  return true;
}

// Try to update master offset
// This may happend when dbsync from master finished
// Here we do:
// 1, Check dbsync finished, got the new binlog offset
// 2, Replace the old db
// 3, Update master offset, and the PikaTrysyncThread cron will connect and do slaveof task with master
bool TrysyncThread::TryUpdateMasterOffset() {
  // Check dbsync finished
  std::string db_sync_path = g_binlog_conf.dump_path;
  std::string info_path = db_sync_path + kBgsaveInfoFile;
  if (!slash::FileExists(info_path)) {
    return false;
  }

  // Got new binlog offset
  std::ifstream is(info_path);
  if (!is) {
    LOG(WARNING) << "Failed to open info file after db sync";
    return false;
  }
  std::string line, master_ip;
  int lineno = 0;
  int64_t filenum = 0, offset = 0, tmp = 0, master_port = 0;
  while (std::getline(is, line)) {
    lineno++;
    if (lineno == 2) {
      master_ip = line;
    } else if (lineno > 2 && lineno < 6) {
      if (!slash::string2l(line.data(), line.size(), &tmp) || tmp < 0) {
        LOG(WARNING) << "Format of info file after db sync error, line : " << line;
        is.close();
        return false;
      }
      if (lineno == 3) { master_port = tmp; }
      else if (lineno == 4) { filenum = tmp; }
      else { offset = tmp; }
   } else if (lineno > 5) {
      LOG(WARNING) << "Format of info file after db sync error, line : " << line;
      is.close();
      return false;
    }
  }
  is.close();
  LOG(INFO) << "Information from dbsync info. master_ip: " << master_ip
    << ", master_port: " << master_port
    << ", filenum: " << filenum
    << ", offset: " << offset;

  // Sanity check
  if (master_ip != g_binlog_conf.master_ip ||
      master_port != g_binlog_conf.master_port) {
    LOG(WARNING) << "Error master ip port: " << master_ip << ":" << master_port;
    return false;
  }

  // Replace the old db
  slash::StopRsync(db_sync_path);
  slash::DeleteFile(info_path);

  // Update master offset
  g_binlog_proxy->logger()->SetProducerStatus(filenum, offset);
  g_binlog_proxy->WaitDBSyncFinish();
  // g_pika_server->SetForceFullSync(false);

  return true;
}

void* TrysyncThread::ThreadMain() {
  while (!should_stop()) {
    sleep(1);

    if (g_binlog_proxy->WaitingDBSync()) {
      LOG(INFO) << "Waiting db sync";
      //Try to update offset by db sync
      if (TryUpdateMasterOffset()) {
        LOG(INFO) << "Success Update Master Offset";
      }
    }

    if (!g_binlog_proxy->ShouldConnectMaster()) {
      continue;
    }
    sleep(2);
    DLOG(INFO) << "Should connect master";
    
    std::string master_ip = g_binlog_conf.master_ip;
    int master_port = g_binlog_conf.master_port;
    std::string dbsync_path = g_binlog_conf.dump_path;

    // Start rsync service
    PrepareRsync();
    std::string ip_port = slash::IpPortString(g_binlog_conf.master_ip, g_binlog_conf.master_port);
    int ret = slash::StartRsync(dbsync_path, kDBSyncModule + "_" + ip_port,
	                g_binlog_conf.local_ip, g_binlog_conf.local_port + 3000);
    if (0 != ret) {
      LOG(WARNING) << "Failed to start rsync, path:" << dbsync_path << " error : " << ret;
      return false;
    }
    LOG(INFO) << "Finish to start rsync, path:" << dbsync_path;

    if ((cli_->Connect(master_ip, master_port, g_binlog_conf.local_ip)).ok()) {
      LOG(INFO) << "Connect to master{ip:" << master_ip << ", port: " << master_port << "}";
      cli_->set_send_timeout(5000);
      cli_->set_recv_timeout(5000);
      if (Send() && RecvProc()) {
        g_binlog_proxy->ConnectMasterDone();
        // Stop rsync, binlog sync with master is begin
        slash::StopRsync(dbsync_path);
        
        delete g_binlog_proxy->ping_thread_;
        g_binlog_proxy->ping_thread_ = new SlavepingThread(sid_);
        g_binlog_proxy->ping_thread_->StartThread();
        DLOG(INFO) << "Trysync success";
      }
      cli_->Close();
    } else {
      LOG(WARNING) << "Failed to connect to master, " << master_ip << ":" << master_port;
    }
  }
  return NULL;
}
