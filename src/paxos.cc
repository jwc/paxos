#include "header.hh"

// Log:

slot_t Log::getPendingStart() { return pendingStart; }

slot_t Log::getPendingEnd() { return pendingEnd; }

Value Log::getValue(slot_t slot) { return values[slot]; }

Vote Log::getVote(slot_t slot) { return votes[slot]; }

ballot_t Log::getBallot(slot_t slot) { return ballots[slot]; }

bool Log::isPending(slot_t slot) 
{ return isFilled(slot) && ! votes[slot].hasMajorityOf(maxVotes); }

bool Log::isConfirmed(slot_t slot) 
{ return isFilled(slot) && votes[slot].hasMajorityOf(maxVotes); }

bool Log::isFilled(slot_t slot) 
{ return values.contains(slot) && votes.contains(slot); }

bool Log::isWritable(slot_t slot) 
{ return slot >= pendingStart && slot < pendingStart + MAX_PENDING; }

bool Log::insert(slot_t slot, Value value, Vote vote, ballot_t ballot) {
  if ( ! isWritable(slot)) return false;

  if (isFilled(slot) && ballot < ballots[slot]) return false;

  if (values[slot] == value && ballots[slot] == ballot) {
    votes[slot].votes |= vote.votes; 

  } else {
    values[slot] = value;
    votes[slot] = vote;
    ballots[slot] = ballot;
  }
  printf("INSERT(): slot:%d value:%d vote:%d\n", slot, value, votes[slot].votes);

  if (pendingEnd < slot) pendingEnd = slot;
    
  while (isConfirmed(pendingStart)) {
    printf("CONFIRMED: slot:%d value:%d\n", pendingStart, values[pendingStart]);
    pendingStart++;
  }

  return true;
}

bool Log::insert(slot_t slot, Value value, ballot_t ballot) 
{ return insert(slot, value, Vote(), ballot); }

slot_t Log::getEmptySlot() {
  while (isFilled(freeSlot)) freeSlot++;

  return freeSlot;
}

void Log::castVote(slot_t slot, node_t voter) { 
  if (isFilled(slot) && isWritable(slot)) votes[slot].cast(voter); 

  while (isConfirmed(pendingStart)) {
    printf("CONFIRMED: slot:%d value:%d\n", pendingStart, values[pendingStart]);
    pendingStart++;
  }
}






// Paxos Public:

Paxos::Paxos(std::string name, std::string address) : net(name, address) {
  servers.push_back(name);
  net.registerApp(this);
}

void Paxos::addServer(std::string name, std::string address) {
  if (numServers > 0) return;

  net.addNode(name, address);

  servers.push_back(name);
}

void Paxos::finalizeServers() {
  printf("finalizeServers()\n");
  if (numServers > 0) return;
  printf("finalize %ld\n", servers.size());

  numServers = servers.size();
  std::string name = servers[0];

  std::sort(servers.begin(), servers.end());

  for (int i = 0; i < numServers; i++) {
    if (servers[i] == name) {
      id = i; 
      break;
    }
  }

  myBallot = id + numServers;
  printf("myBallot:%d\n", myBallot);

  printf("new PaxosTask\n:");
  new PaxosTask(this);
}

void Paxos::requestValue(Value value) {
  if ( ! isLeader()) return;

  slot_t slot = log.getEmptySlot();
  if ( ! log.insert(slot, value, latestBallot)) {
    return;
  }

  log.castVote(slot, id);

  assert(latestBallot % numServers == id);
  char rawMessage[Paxos::AcceptMsg::messageSize] = {0};
  Paxos::AcceptMsg accept = Paxos::AcceptMsg(rawMessage,
                                             0, id, latestBallot, slot, value);
  for (node_t i = 0; i < numServers; i++) {
    if (i == id) continue;
    accept.setTo(i);
    net.sendMessage(servers[i], Paxos::AcceptMsg::messageSize, rawMessage);
  }
}

// Paxos Private:

void Paxos::processMessage(int length, char *message) {
  //std::cout << "processMessage()\n";
  if (numServers <= 0) {
    std::cerr << "Message received before servers setup.\n";
    // Paxos servers not yet established: do nothing.
    return;
  }

  if (length < Message::messageSize) {
    std::cerr << "ERROR: Message Too Short!\n";
    return;
  }

  Message m = Message(message);
  switch (m.getType()) {
    case PREPARE:
      {
        PrepareMsg prep = PrepareMsg(message);
        prep.print();
        handlePrepare(prep);
        break;
      }
    case PROMISE:
      {
        if (length < PromiseMsg::messageSize) {
          std::cerr << "ERROR: Prom. Msg. Too Short!\n";
          return;
        }
        PromiseMsg prom = PromiseMsg(message);
        prom.print();
        handlePromise(prom);
        break;
      }
    case ACCEPT:
      {
        AcceptMsg acpt = AcceptMsg(message);
        acpt.print();
        handleAccept(acpt);
        break;
      }
    case ACCEPTED:
      {
        AcceptedMsg accepted = AcceptedMsg(message);
        accepted.print();
        handleAccepted(accepted);
        break;
      }
    case HEARTBEAT:
      {
        HeartbeatMsg heartbeat = HeartbeatMsg(message);
        heartbeat.print();
        handleHeartbeat(heartbeat);
        break;
      }
    case REQUEST:
      //m.print();
      //break;
    default:
      std::cerr << "ERROR: Invalid Message Type Recieved!\n";
      m.print();
  }
}

void Paxos::handlePrepare(Paxos::PrepareMsg &prep) {
  std::cout<<"handlePrep(): "<<latestBallot<<" vs "<<prep.getBallot()<<"\n";
  if (prep.getBallot() > latestBallot) {
    //std::cout << "TRUE\n";

    intervalsWithoutLeader = 0;
    latestBallot = prep.getBallot();
    char bytes[Paxos::PromiseMsg::messageSize];
    for (int i = 0; i < Paxos::PromiseMsg::messageSize; i++) bytes[i] = 0;
    PromiseMsg prom = Paxos::PromiseMsg(bytes, 0, id, latestBallot, log.getPendingStart(), log.getPendingEnd());

    //std::cout << "HI1\n";
    for (slot_t i = log.getPendingStart(); i < log.getPendingEnd(); i++) {
      if (log.isFilled(i)) {
        prom.setValue(i, log.getValue(i));
        prom.setVote(i, log.getVote(i));
      }
    }
    
    printf("# servers: %d\n", numServers);
    //std::cout << "numServers:" << numServers << "\n";
    for (node_t i = 0; i < numServers; i++) {
      if (i == id) continue;
      prom.setTo(i);
      //std::cout << id << "->" << i << std::endl;
      net.sendMessage(servers[i], Paxos::PromiseMsg::messageSize, bytes);
    }
  }
}

void Paxos::handlePromise(Paxos::PromiseMsg &prom) {
  printf("count:%d leader:%d\n", leaderVote.count(), isLeader());
  
  if (isLeader() || prom.getBallot() != myBallot) return;

  leaderVote.cast(prom.getFrom());

  slot_t slot;
  for (slot = prom.getPromisedStart(); slot < prom.getPromisedEnd(); slot++) {
    log.insert(slot, prom.getValue(slot), prom.getVote(slot), prom.getBallot());
    log.castVote(slot, id);
  }
}

void Paxos::handleAccept(Paxos::AcceptMsg &acpt) {
  if (acpt.getBallot() < latestBallot) return;

  if (acpt.getBallot() > latestBallot) {
    latestBallot = acpt.getBallot();
  }

  intervalsWithoutLeader = 0;

  if ( ! log.insert(acpt.getSlot(), acpt.getValue(), acpt.getBallot())) {
    return;
  }

  log.castVote(acpt.getSlot(), id);
  log.castVote(acpt.getSlot(), acpt.getFrom());

  char rawMessage[Paxos::AcceptedMsg::messageSize] = {0};
  AcceptedMsg accepted = AcceptedMsg(rawMessage, 
                                     acpt.getFrom(), 
                                     id, 
                                     latestBallot, 
                                     acpt.getSlot(), 
                                     acpt.getValue());
  for (node_t i = 0; i < numServers; i++) {
    if (i == id) continue;
    accepted.setTo(i);
    net.sendMessage(servers[i], Paxos::AcceptedMsg::messageSize, rawMessage);
  }
}

void Paxos::handleAccepted(Paxos::AcceptedMsg &accepted) {
  if (accepted.getBallot() < latestBallot) return;

  if (accepted.getBallot() > latestBallot) {
    latestBallot = accepted.getBallot();
    intervalsWithoutLeader = 0;
  }

  log.insert(accepted.getSlot(), accepted.getValue(), accepted.getBallot());
  log.castVote(accepted.getSlot(), id);
  log.castVote(accepted.getSlot(), accepted.getFrom());
}

void Paxos::handleHeartbeat(Paxos::HeartbeatMsg &heartbeat) {
  if (latestBallot == heartbeat.getBallot()) {
    intervalsWithoutLeader = 0;

  } else if (latestBallot < heartbeat.getBallot()) {
    latestBallot = heartbeat.getBallot();
    intervalsWithoutLeader = 0;
  } 
}

bool Paxos::isLeader() 
{ return latestBallot == myBallot && leaderVote.hasMajorityOf(numServers); }

