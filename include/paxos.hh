#ifndef PAXOS_HH
#define PAXOS_HH

typedef int Value;
typedef uint8_t node_t;
typedef uint32_t ballot_t;
typedef uint32_t slot_t;
typedef struct vote_t {
  uint32_t votes = 0;

  void cast(node_t id) { votes |= 1 << id; }

  int count() { return __builtin_popcount(votes); }

  bool hasMajorityOf(node_t num) { return (count() * 2) > num; }

  void clear() { votes = 0; }

} Vote;
static_assert(sizeof(Vote) == sizeof(uint32_t));

class Log {
  node_t & maxVotes;
  std::unordered_map<slot_t, Value> values;
  std::unordered_map<slot_t, Vote> votes;
  std::unordered_map<slot_t, ballot_t> ballots;
  slot_t pendingStart = 0;
  slot_t pendingEnd = 0;
  slot_t freeSlot = 0;

  /*
  void setValue(slot_t slot, Value value) 
  { if (isWritable(slot)) values[slot] = value; }

  void setVote(slot_t slot, Vote vote) 
  { if (isWritable(slot)) votes[slot] = vote; }

  void setBallot(slot_t slot, ballot_t ballot) 
  { if (isWritable(slot)) ballots[slot] = ballot; } 
  */

public:
  const static int MAX_PENDING = 4;
  Log(node_t &maxVotes) : maxVotes(maxVotes) {}

  slot_t getPendingStart() { return pendingStart; }

  slot_t getPendingEnd() { return pendingEnd; }

  Value getValue(slot_t slot) { return values[slot]; }

  Vote getVote(slot_t slot) { return votes[slot]; }

  ballot_t getBallot(slot_t slot) { return ballots[slot]; }

  bool isPending(slot_t slot) 
  { return isFilled(slot) && ! votes[slot].hasMajorityOf(maxVotes); }

  bool isConfirmed(slot_t slot) 
  { return isFilled(slot) && votes[slot].hasMajorityOf(maxVotes); }

  bool isFilled(slot_t slot) 
  { return values.contains(slot) && votes.contains(slot); }

  bool isWritable(slot_t slot) 
  { return slot >= pendingStart && slot < pendingStart + MAX_PENDING; }

  bool insert(slot_t slot, Value value, Vote vote, ballot_t ballot) {
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

  bool insert(slot_t slot, Value value, ballot_t ballot) 
  {return insert(slot, value, Vote(), ballot); }

  slot_t getEmptySlot() {
    while (isFilled(freeSlot)) freeSlot++;

    return freeSlot;
  }

  void castVote(slot_t slot, node_t voter) { 
    if (isFilled(slot) && isWritable(slot)) votes[slot].cast(voter); 

    while (isConfirmed(pendingStart)) {
      printf("CONFIRMED: slot:%d value:%d\n", pendingStart, values[pendingStart]);
      pendingStart++;
    }
  }
};

class Paxos : Application {
//private:
  IPv4                      net;
  std::vector<std::string>  servers;
  node_t                    numServers  = 0;
  node_t                    id          = 0;

  ballot_t                  latestBallot = 0;
  ballot_t                  myBallot = 0;
  Vote                      leaderVote;
  Log                       log{numServers};
  int                       intervalsWithoutLeader = 0;
  const static int          MAX_INTERVALS_WO_LEADER = 3;

  bool isLeader() 
  { return latestBallot == myBallot && leaderVote.hasMajorityOf(numServers); }

  const static int          maxPendingValues = Log::MAX_PENDING;
public:
  enum Type : uint8_t { 
    PREPARE = 1, 
    PROMISE = 2,
    ACCEPT,
    ACCEPTED,
    HEARTBEAT,
    REQUEST
  };

  class Message {
  protected:
    char * data; 
    static const int typeOffset   = 0;
    static const int toOffset     = typeOffset  + sizeof(Type);
    static const int fromOffset   = toOffset    + sizeof(node_t);
    static const int ballotOffset = fromOffset  + sizeof(node_t);

  public:
    static const int messageSize = ballotOffset + sizeof(ballot_t);

    Message(char * data) : data(data) {}
    Message(char * data, Type type, node_t to, node_t from, ballot_t ballot) 
        : data(data) {
      setType(type);
      setTo(to);
      setFrom(from);
      setBallot(ballot);
    }

    Type getType() { return *((Type *) (data + typeOffset)); }
    void setType(Type type) { *((Type *) (data + typeOffset)) = type; }

    node_t getTo() { return *((node_t *) (data + toOffset)); }
    void setTo(node_t to) { *((node_t *) (data + toOffset)) = to; }

    node_t getFrom() 
    { return *((node_t *) (data + fromOffset)); }
    void setFrom(node_t from) { *((node_t *) (data + fromOffset)) = from; } 

    ballot_t getBallot() { return *((ballot_t *) (data + ballotOffset)); }
    void setBallot(ballot_t ballot) 
    { *((ballot_t *) (data + ballotOffset)) = ballot; }
    
    void print() { 
      printf("MSG{type:%hhd to:%d  from:%d  bal:%d}\n", getType(), getTo(), getFrom(), getBallot());
    }
  };

  class PrepareMsg : public Message {
  public:
    PrepareMsg(char * data) : Message(data) {}
    PrepareMsg(char * data, node_t to, node_t from, ballot_t ballot) 
      : Message(data, Type::PREPARE, to, from, ballot) {}

    void print() { 
      printf("PREP{to:%d  from:%d  bal:%d}\n", getTo(), getFrom(), getBallot());
    }
  };

  class PromiseMsg : public Message {
  private:
    static const int promisedStartOffset = Message::messageSize;
    static const int promisedEndOffset = promisedStartOffset + sizeof(slot_t);
    static const int valuesOffset = promisedEndOffset + sizeof(slot_t);
    static const int votesOffset = valuesOffset + (maxPendingValues * sizeof(Value));
  public:
    static const int messageSize = votesOffset + (maxPendingValues * sizeof(Vote));

    PromiseMsg(char * data) : Message(data) {
      std::cout << "Prom. Msg. Const. Begin\n";
      if (getPromisedEnd() - getPromisedStart() > maxPendingValues) {
        std::cerr << "ERROR: Bad Range in Prom. Msg. (" << getPromisedStart() 
          << " & " << getPromisedEnd() << ")\n";
        setPromisedEnd(getPromisedStart() + maxPendingValues);
      }
      std::cout << "Prom. Msg. Const. End\n";
    }
    PromiseMsg(char * data, node_t to, node_t from, ballot_t ballot, slot_t promisedStart, slot_t promisedEnd) : Message(data, Type::PROMISE, to, from, ballot) {
      std::cout << "Prom. Msg. Const. Begin\n";
      setPromisedStart(promisedStart);
      setPromisedEnd(promisedEnd);
      if (getPromisedEnd() - getPromisedStart() > maxPendingValues) {
        std::cerr << "ERROR: Bad Range in Prom. Msg. (" << getPromisedStart() 
          << " & " << getPromisedEnd() << ")\n";
        setPromisedEnd(getPromisedStart() + maxPendingValues);
      }
      std::cout << "Prom. Msg. Const. End\n";
    }
    
    slot_t getPromisedStart() 
    { return *((slot_t *) (data + promisedStartOffset)); }
    void setPromisedStart(slot_t slot) 
    { *((slot_t *) (data + promisedStartOffset)) = slot; }

    slot_t getPromisedEnd() 
    { return *((slot_t *) (data + promisedEndOffset)); }
    void setPromisedEnd(slot_t slot) 
    { *((slot_t *) (data + promisedEndOffset)) = slot; }

    Value getValue(slot_t slot) 
    { return ((Value *) (data + valuesOffset))[slot - getPromisedStart()]; }
    void setValue(slot_t slot, Value val) 
    { ((Value *) (data + valuesOffset))[slot - getPromisedStart()] = val; }

    Vote getVote(slot_t slot) 
    { return ((Vote *) (data + votesOffset))[slot - getPromisedStart()]; }
    void setVote(slot_t slot, Vote vote) 
    { ((Vote *) (data + votesOffset))[slot - getPromisedStart()] = vote; }

    void print() {
      printf("PROM{to:%d  from:%d  bal:%d", getTo(), getFrom(), getBallot());
      for (slot_t i = getPromisedStart(); i < getPromisedEnd(); i++)
        printf("\t%ud:%ud:%ud\n", i, getValue(i), getVote(i).count());
    }
  };

  class AcceptMsg : public Message {
    static const int slotOffset = Message::messageSize;
    static const int valueOffset = slotOffset + sizeof(slot_t); 
  public:
    static const int messageSize = valueOffset + sizeof(Value);

    AcceptMsg(char * data) : Message(data) {}
    AcceptMsg(char * data, 
              node_t to, 
              node_t from, 
              ballot_t ballot, 
              slot_t slot, 
              Value value) 
        : Message(data, Type::ACCEPT, to, from, ballot) {
      setSlot(slot);
      setValue(value);
    }

    slot_t getSlot() { return *((slot_t *) (data + slotOffset)); }
    void setSlot(slot_t slot) { *((slot_t *) (data + slotOffset)) = slot; }
    Value getValue() { return *((Value *) (data + valueOffset)); }
    void setValue(Value value) { *((Value *) (data + valueOffset)) = value; }
  
    void print() {
      printf("ACPT{to:%d from:%d bal:%d slot:%d val:%d}\n", getTo(), getFrom(), 
          getBallot(), getSlot(), getValue());
    }
  };

  class AcceptedMsg : public Message {
    static const int slotOffset = Message::messageSize;
    static const int valueOffset = slotOffset + sizeof(slot_t); 
  public:
    static const int messageSize = valueOffset + sizeof(Value);

    AcceptedMsg(char * data) : Message(data) {}
    AcceptedMsg(char * data, 
                node_t to, 
                node_t from, 
                ballot_t ballot, 
                slot_t slot, 
                Value value) 
        : Message(data, Type::ACCEPTED, to, from, ballot) {
      setSlot(slot);
      setValue(value);
    }

    slot_t getSlot() { return *((slot_t *) (data + slotOffset)); }
    void setSlot(slot_t slot) { *((slot_t *) (data + slotOffset)) = slot; }
    Value getValue() { return *((Value *) (data + valueOffset)); }
    void setValue(Value value) { *((Value *) (data + valueOffset)) = value; }
  
    void print() {
      printf("ACPTED{to:%d from:%d bal:%d slot:%d val:%d}\n", getTo(), getFrom(), 
          getBallot(), getSlot(), getValue());
    }
  };

  class HeartbeatMsg : public Message {
  public: 
    HeartbeatMsg(char * data) : Message(data) {}
    HeartbeatMsg(char * data, node_t to, node_t from, ballot_t ballot)
      : Message(data, Type::HEARTBEAT, to, from, ballot) {}

    void print() {
      printf("HEART{to:%d from:%d bal:%d}\n", getTo(), getFrom(), getBallot());
    }
  };
 
  class PaxosTask : public Task {
    const static int WAIT_TIME_MS = 1000;
    Paxos *pax;

  public:
    PaxosTask(Paxos *pax) : Task(Type::NON_BLOCKING, WAIT_TIME_MS), pax(pax) 
    { printf("REeady\n"); ready(); }

    void executeTask() {
      printf("%d Timer int:%d\n", pax->id, pax->intervalsWithoutLeader);

      if (pax == nullptr) return;

      if (pax->numServers <= 0) { 
        // Paxos servers not yet established: do nothing.
        new PaxosTask(pax);
        return;
      }

      if (pax->isLeader()) {
        // send heartbeat message
        char msgRaw[Paxos::HeartbeatMsg::messageSize] = {0};
        HeartbeatMsg msg = HeartbeatMsg(msgRaw, pax->id, pax->id, pax->latestBallot);
        for (node_t i = 0; i < pax->numServers; i++) {
          if (i == pax->id) continue;
          msg.setTo(i);
          pax->net.sendMessage(pax->servers[i], HeartbeatMsg::messageSize, msgRaw);
        }

      } else {
        // determine if leader is live
        if (pax->intervalsWithoutLeader++ > MAX_INTERVALS_WO_LEADER) {
          while (pax->myBallot < pax->latestBallot) pax->myBallot += pax->numServers;
          pax->leaderVote.clear();
          pax->leaderVote.cast(pax->id);
          pax->latestBallot = pax->myBallot;

          char msgRaw[Paxos::PrepareMsg::messageSize] = {0};
          Paxos::PrepareMsg msg = Paxos::PrepareMsg(msgRaw, pax->id, pax->id, pax->latestBallot);
          for (node_t i = 0; i < pax->numServers; i++) {
            if (i == pax->id) continue;
            msg.setTo(i);
            pax->net.sendMessage(pax->servers[i], PrepareMsg::messageSize, msgRaw);
          }
        }
      }

      new PaxosTask(pax);
    }
  };

public:
  Paxos(std::string name, std::string address); 

  void addServer(std::string name, std::string address);

  void finalizeServers();

  void processMessage(int length, char *message);

  void handlePrepare(PrepareMsg &prep);
  
  void handlePromise(PromiseMsg &prom);

  void handleAccept(AcceptMsg &accept);

  void handleAccepted(AcceptedMsg &accepted);

  void handleHeartbeat(HeartbeatMsg &heartbeat);

  void requestValue(Value value);
};

#endif // PAXOS_HH

