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
  //printf("INSERT(): slot:%d val:%d vote:%d\n", slot, value, votes[slot].votes);

  if (pendingEnd < slot) pendingEnd = slot;
    
  while (isConfirmed(pendingStart)) {
    //printf("CONFIRMED: slot:%d val:%d\n", pendingStart, values[pendingStart]);
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
    //printf("CONFIRMED: slot:%d val:%d\n", pendingStart, values[pendingStart]);
    pendingStart++;
  }
}

// Paxos Public:

Paxos::Paxos(std::string name, std::string address) : net(name, address) {
  servers.push_back(name);
  net.registerConsensus(this);
}

void Paxos::addServer(std::string name, std::string address) {
  if (numServers > 0) return;

  net.addNode(name, address);

  servers.push_back(name);
}

void Paxos::finalizeServers() {
  //printf("finalizeServers()\n");
  if (numServers > 0) return;
  //printf("finalize %ld\n", servers.size());

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
  //printf("myBallot:%d\n", myBallot);

  //printf("new PaxosTask\n:");
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
  /*
  char rawMessage[Paxos::AcceptMsg::messageSize] = {0};
  Paxos::AcceptMsg accept = Paxos::AcceptMsg(rawMessage,
                                             0, id, latestBallot, slot, value);
  for (node_t i = 0; i < numServers; i++) {
    if (i == id) continue;
    accept.setTo(i);
    net.sendMessage(servers[i], Paxos::AcceptMsg::messageSize, rawMessage);
  }
  */
  sendAccept(slot);
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
    case QUERY:
      {
        QueryMsg query = QueryMsg(message);
        query.print();
        handleQuery(query);
        break;
      }
    case CONFIRMED:
      {
        ConfirmedMsg confirmed = ConfirmedMsg(message);
        confirmed.print();
        handleConfirmed(confirmed);
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
    intervalsWithoutLeader = 0;
    latestBallot = prep.getBallot();
    char bytes[Paxos::PromiseMsg::messageSize];
    for (int i = 0; i < Paxos::PromiseMsg::messageSize; i++) bytes[i] = 0;
    PromiseMsg prom = Paxos::PromiseMsg(bytes, 0, id, latestBallot, 
                                  log.getPendingStart(), log.getPendingEnd());

    for (slot_t i = log.getPendingStart(); i < log.getPendingEnd(); i++) {
      if (log.isFilled(i)) {
        prom.setValue(i, log.getValue(i));
        prom.setVote(i, log.getVote(i));
      }
    }
    
    //printf("# servers: %d\n", numServers);
    for (node_t i = 0; i < numServers; i++) {
      if (i == id) continue;
      prom.setTo(i);
      net.sendMessage(servers[i], Paxos::PromiseMsg::messageSize, bytes);
    }
  }
}

void Paxos::handlePromise(Paxos::PromiseMsg &prom) {
  //printf("count:%d leader:%d\n", leaderVote.count(), isLeader());
  
  if (isLeader() || prom.getBallot() != myBallot) return;

  leaderVote.cast(prom.getFrom());

  slot_t slot;
  for (slot = prom.getPromisedStart(); slot < prom.getPromisedEnd(); slot++) {
    log.insert(slot, prom.getValue(slot), 
               prom.getVote(slot), prom.getBallot());
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
    //char bytes[Paxos::QueryMsg::messageSize] = {0};
    //QueryMsg q = QueryMsg(bytes, acpt.getFrom(), id, 
                          //latestBallot, log.getPendingStart(), acpt.getSlot());
    //net.sendMessage(servers[q.getTo()], Paxos::QueryMsg::messageSize, bytes);
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
  if (heartbeat.getBallot() < latestBallot) {
    std::cerr << "Bad Ballot\n";
    return;
  }

  latestBallot = heartbeat.getBallot();
  intervalsWithoutLeader = 0;

  if (heartbeat.getUpto() > log.getPendingStart()) {
    char bytes[Paxos::QueryMsg::messageSize] = {0};
    QueryMsg q = QueryMsg(bytes, heartbeat.getFrom(), id, latestBallot, 
                          log.getPendingStart(), heartbeat.getUpto());
    net.sendMessage(servers[q.getTo()], Paxos::QueryMsg::messageSize, bytes);
  }
}

void Paxos::handleQuery(Paxos::QueryMsg &query) {
  char bytes[Paxos::ConfirmedMsg::messageSize] = {0};
  for (slot_t i = query.getStartSlot(); i < query.getEndSlot(); i++) {
    if (log.isConfirmed(i)) {
      ConfirmedMsg confirmed = ConfirmedMsg(bytes, query.getFrom(), id, 
                                            query.getBallot(), i,
                                            log.getValue(i), log.getVote(i));
      net.sendMessage(servers[confirmed.getTo()], 
                      ConfirmedMsg::messageSize, bytes);
    }
  }
}

void Paxos::handleConfirmed(Paxos::ConfirmedMsg &confirmed) {
  if (log.insert(confirmed.getSlot(), confirmed.getValue(), 
                 confirmed.getVote(), confirmed.getBallot())) { 
    for (node_t i = 0; i < numServers; i++) {
      log.castVote(confirmed.getSlot(), i);
    }
  }
}

bool Paxos::isLeader() { 
  return latestBallot == myBallot && leaderVote.hasMajorityOf(numServers); 
}

// PaxosTask:

Paxos::PaxosTask::PaxosTask(Paxos *pax) 
    : Task(Type::NON_BLOCKING, WAIT_TIME_MS), pax(pax) { 
  ready(); 
}

void Paxos::PaxosTask::executeTask() {
  Log & log = pax->log;
  //printf("%d Timer int:%d\n", pax->id, pax->intervalsWithoutLeader);

  if (pax == nullptr) return;

  if (pax->numServers <= 0) { 
    // Paxos servers not yet established: do nothing.
    new PaxosTask(pax);
    return;
  }

  if (pax->isLeader()) {
    // send heartbeat message
    char msgRaw[Paxos::HeartbeatMsg::messageSize] = {0};
    HeartbeatMsg msg = HeartbeatMsg(msgRaw, pax->id, 
                                    pax->id, pax->latestBallot);
    msg.setUpto(pax->log.getPendingStart());
    for (node_t i = 0; i < pax->numServers; i++) {
      if (i == pax->id) continue;
      msg.setTo(i);
      pax->net.sendMessage(pax->servers[i], HeartbeatMsg::messageSize, msgRaw);
    }

    if (log.isPending(log.getPendingStart())) {
      // Resend oldest pending
      pax->sendAccept(log.getPendingStart());
    }

  } else {
    // determine if leader is live
    if (pax->intervalsWithoutLeader++ > MAX_INTERVALS_WO_LEADER) {
      while (pax->myBallot < pax->latestBallot) 
        pax->myBallot += pax->numServers;
      pax->leaderVote.clear();
      pax->leaderVote.cast(pax->id);
      pax->latestBallot = pax->myBallot;

      char msgRaw[Paxos::PrepareMsg::messageSize] = {0};
      Paxos::PrepareMsg msg = Paxos::PrepareMsg(msgRaw, pax->id, pax->id, 
                                                pax->latestBallot);
      for (node_t i = 0; i < pax->numServers; i++) {
        if (i == pax->id) continue;
        msg.setTo(i);
        pax->net.sendMessage(pax->servers[i], PrepareMsg::messageSize, msgRaw);
      }
    }
  }

  // Passing confirmed values to the app.
  while (pax->app != nullptr && pax->log.isConfirmed(pax->processedUpTo)) 
    pax->app->processValue(pax->log.getValue(pax->processedUpTo++));

  new PaxosTask(pax);
}

//Paxos Message Methods:

Paxos::Message::Message(char * data) : data(data) {}

Paxos::Message::Message(char * data, 
                        Paxos::Type type, 
                        node_t to, 
                        node_t from, 
                        ballot_t ballot) 
    : data(data) {
  setType(type);
  setTo(to);
  setFrom(from);
  setBallot(ballot);
}

/*
Paxos::Type Paxos::Message::getType() { 
  //return *((Paxos::Type *) (data + typeOffset)); 
  return get<Paxos::Type>(typeOffset);
}

void Paxos::Message::setType(Paxos::Type type) { 
  *((Paxos::Type *) (data + typeOffset)) = type; 
}

node_t Paxos::Message::getTo() { return *((node_t *) (data + toOffset)); }

void Paxos::Message::setTo(node_t to) { *((node_t *) (data + toOffset)) = to; }

node_t Paxos::Message::getFrom() { return *((node_t *) (data + fromOffset)); }

void Paxos::Message::setFrom(node_t from) { 
  *((node_t *) (data + fromOffset)) = from; 
} 

ballot_t Paxos::Message::getBallot() { 
  return *((ballot_t *) (data + ballotOffset)); 
}

void Paxos::Message::setBallot(ballot_t ballot) { 
  *((ballot_t *) (data + ballotOffset)) = ballot; 
}
*/


void Paxos::Message::print() { 
  printf("MSG{type:%hhd to:%d  from:%d  bal:%d}\n", 
      getType(), getTo(), getFrom(), getBallot());
}

Paxos::PrepareMsg::PrepareMsg(char * data) : Message(data) {}

Paxos::PrepareMsg::PrepareMsg(char * data, 
                              node_t to, 
                              node_t from, 
                              ballot_t ballot)
    : Message(data, Paxos::Type::PREPARE, to, from, ballot) {}

void Paxos::PrepareMsg::print() { 
  printf("PREP{to:%d  from:%d  bal:%d}\n", getTo(), getFrom(), getBallot());
}


Paxos::PromiseMsg::PromiseMsg(char * data) : Message(data) {
  if (getPromisedEnd() - getPromisedStart() > maxPendingValues) {
    setPromisedEnd(getPromisedStart() + maxPendingValues);
  }
}

Paxos::PromiseMsg::PromiseMsg(char * data, 
                              node_t to, 
                              node_t from, 
                              ballot_t ballot, 
                              slot_t promisedStart, 
                              slot_t promisedEnd) 
    : Message(data, Paxos::Type::PROMISE, to, from, ballot) {
  setPromisedStart(promisedStart);
  setPromisedEnd(promisedEnd);

  if (getPromisedEnd() - getPromisedStart() > maxPendingValues) {
    setPromisedEnd(getPromisedStart() + maxPendingValues);
  }
}


slot_t Paxos::PromiseMsg::getPromisedStart() { 
  return *((slot_t *) (data + promisedStartOffset)); 
}

void Paxos::PromiseMsg::setPromisedStart(slot_t slot) { 
  *((slot_t *) (data + promisedStartOffset)) = slot; 
}

slot_t Paxos::PromiseMsg::getPromisedEnd() { 
  return *((slot_t *) (data + promisedEndOffset)); 
}

void Paxos::PromiseMsg::setPromisedEnd(slot_t slot) { 
  *((slot_t *) (data + promisedEndOffset)) = slot; 
}

Value Paxos::PromiseMsg::getValue(slot_t slot) { 
  return ((Value *) (data + valuesOffset))[slot - getPromisedStart()]; 
}

void Paxos::PromiseMsg::setValue(slot_t slot, Value val) { 
  ((Value *) (data + valuesOffset))[slot - getPromisedStart()] = val; 
}

Vote Paxos::PromiseMsg::getVote(slot_t slot) { 
  return ((Vote *) (data + votesOffset))[slot - getPromisedStart()]; 
}

void Paxos::PromiseMsg::setVote(slot_t slot, Vote vote) { 
  ((Vote *) (data + votesOffset))[slot - getPromisedStart()] = vote; 
}

void Paxos::PromiseMsg::print() {
  printf("PROM{to:%d  from:%d  bal:%d", getTo(), getFrom(), getBallot());
  //for (slot_t i = getPromisedStart(); i < getPromisedEnd(); i++)
    //printf("\t%ud:%ud:%ud\n", i, getValue(i), getVote(i).count());
}


Paxos::AcceptMsg::AcceptMsg(char * data) : Message(data) {}

Paxos::AcceptMsg::AcceptMsg(char * data, 
                            node_t to, 
                            node_t from, 
                            ballot_t ballot, 
                            slot_t slot, 
                            Value value) 
    : Message(data, Type::ACCEPT, to, from, ballot) {
  setSlot(slot);
  setValue(value);
}

slot_t Paxos::AcceptMsg::getSlot() { return *((slot_t *) (data+slotOffset)); }

void Paxos::AcceptMsg::setSlot(slot_t slot) { 
  *((slot_t *) (data + slotOffset)) = slot; 
}

Value Paxos::AcceptMsg::getValue() { return *((Value *) (data+valueOffset)); }

void Paxos::AcceptMsg::setValue(Value value) { 
  *((Value *) (data + valueOffset)) = value; 
}
  
void Paxos::AcceptMsg::print() {
  printf("ACPT{to:%d from:%d bal:%d slot:%d val:%d}\n", getTo(), getFrom(),
    getBallot(), getSlot(), getValue());
}

Paxos::AcceptedMsg::AcceptedMsg(char * data) : AcceptMsg(data) {}

Paxos::AcceptedMsg::AcceptedMsg(char * data, 
            node_t to, 
            node_t from, 
            ballot_t ballot, 
            slot_t slot, 
            Value value) 
    : AcceptMsg(data, to, from, ballot, slot, value) {
  setType(Type::ACCEPTED);
}

void Paxos::AcceptedMsg::print() {
  printf("ACPTED{to:%d from:%d bal:%d slot:%d val:%d}\n", getTo(), 
      getFrom(), getBallot(), getSlot(), getValue());
}



Paxos::HeartbeatMsg::HeartbeatMsg(char * data) : Message(data) {}

Paxos::HeartbeatMsg::HeartbeatMsg(char * data, 
                                  node_t to, 
                                  node_t from, 
                                  ballot_t ballot) 
    : Message(data, Type::HEARTBEAT, to, from, ballot) {}

void Paxos::HeartbeatMsg::print() {
  printf("HEART{to:%d from:%d bal:%d, upto:%d}\n", 
         getTo(), getFrom(), getBallot(), getUpto());
}


Paxos::QueryMsg::QueryMsg(char * data) : Message(data) {}

Paxos::QueryMsg::QueryMsg(char * data, 
                          node_t to, 
                          node_t from,
                          ballot_t ballot, 
                          slot_t start, 
                          slot_t end) 
    : Message(data, Type::QUERY, to, from, ballot) {
  setStartSlot(start);
  setEndSlot(end);
}

slot_t Paxos::QueryMsg::getStartSlot() { 
  return *((slot_t *) (data+startSlotOffset)); 
}

void Paxos::QueryMsg::setStartSlot(slot_t slot) { 
  *((slot_t *) (data + startSlotOffset)) = slot; 
}

slot_t Paxos::QueryMsg::getEndSlot() { 
  return *((slot_t *) (data+endSlotOffset)); 
}

void Paxos::QueryMsg::setEndSlot(slot_t slot) { 
  *((slot_t *) (data + endSlotOffset)) = slot; 
}

void Paxos::QueryMsg::print() {
  printf("QUERY{to:%d from:%d bal:%d slots:%d-%d}\n", 
         getTo(), getFrom(), getBallot(), getStartSlot(), getEndSlot());
}

Paxos::ConfirmedMsg::ConfirmedMsg(char * data) : AcceptMsg(data) {}

Paxos::ConfirmedMsg::ConfirmedMsg(char * data, 
                                  node_t to, 
                                  node_t from, 
                                  ballot_t ballot, 
                                  slot_t slot, 
                                  Value value, 
                                  Vote vote) 
    : AcceptMsg(data, to, from, ballot, slot, value) {
  setType(Type::CONFIRMED);
  setVote(vote);
}

Vote Paxos::ConfirmedMsg::getVote() { return *((Vote *) (data+voteOffset)); }

void Paxos::ConfirmedMsg::setVote(Vote vote) { 
  *((Vote *) (data + voteOffset)) = vote; 
}

void Paxos::ConfirmedMsg::print() {
  printf("CONFIRM{to:%d from:%d bal:%d slot:%d val:%d vote:%d}\n", 
         getTo(), getFrom(), getBallot(), 
         getSlot(), getValue(), getVote().count());
}

