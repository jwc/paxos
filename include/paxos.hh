#ifndef PAXOS_HH
#define PAXOS_HH

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
private:
  node_t & maxVotes;
  std::unordered_map<slot_t, Value> values;
  std::unordered_map<slot_t, Vote> votes;
  std::unordered_map<slot_t, ballot_t> ballots;
  slot_t pendingStart = 0;
  slot_t pendingEnd = 0;
  slot_t freeSlot = 0;

public:
  const static int MAX_PENDING = 2;

  Log(node_t &maxVotes) : maxVotes(maxVotes) {}

  slot_t getPendingStart();

  slot_t getPendingEnd();

  Value getValue(slot_t slot);

  Vote getVote(slot_t slot);

  ballot_t getBallot(slot_t slot);

  bool isPending(slot_t slot);

  bool isConfirmed(slot_t slot);

  bool isFilled(slot_t slot);

  bool isWritable(slot_t slot);

  bool insert(slot_t slot, Value value, Vote vote, ballot_t ballot);

  bool insert(slot_t slot, Value value, ballot_t ballot);

  slot_t getEmptySlot();

  void castVote(slot_t slot, node_t voter);
};

class Paxos : public Consensus {
public:
  Paxos(std::string name, std::string address); 

  void addServer(std::string name, std::string address);

  void finalizeServers();

  void requestValue(Value value);

  void registerApplication(Application *app) { this->app = app; }

private:
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
  const static int          maxPendingValues = Log::MAX_PENDING;

  Application               *app = nullptr;
  slot_t                    processedUpTo = 0;

  enum Type : uint8_t { 
    PREPARE = 1, 
    PROMISE = 2,
    ACCEPT,
    ACCEPTED,
    HEARTBEAT,
    QUERY,
    CONFIRMED,
    REQUEST
  };

  class PaxosTask : public Task {
    const static int WAIT_TIME_MS = 1000;
    Paxos *pax;

    void executeTask() override;

  public:
    explicit PaxosTask(Paxos *pax);
  };

  class Message {
  protected:
    char * data; 
    static const int typeOffset   = 0;
    static const int toOffset     = typeOffset  + sizeof(Type);
    static const int fromOffset   = toOffset    + sizeof(node_t);
    static const int ballotOffset = fromOffset  + sizeof(node_t);

    template <typename T> inline void set(int location, T word) {
      *((T *) (data + location)) = word;
    }

    template <typename T> inline T get(int location) { 
      return *((T *) (data + location));
    } 

  public:
    static const int messageSize = ballotOffset + sizeof(ballot_t);

    Message(char * data); 

    Message(char * data, Type type, node_t to, node_t from, ballot_t ballot);

    Type getType() { return get<Paxos::Type>(typeOffset); }

    void setType(Type type) { set(typeOffset, type); }

    node_t getTo() { return get<node_t>(toOffset); }

    void setTo(node_t to) { set(toOffset, to); }

    node_t getFrom() { return get<node_t>(fromOffset); }

    void setFrom(node_t from) { set(fromOffset, from); }

    ballot_t getBallot() { return get<ballot_t>(ballotOffset); }

    void setBallot(ballot_t ballot) { set(ballotOffset, ballot); }

    void print();
  };

  class PrepareMsg : public Message {
  public:
    PrepareMsg(char * data); 
    PrepareMsg(char * data, node_t to, node_t from, ballot_t ballot);

    void print();
  };

  class PromiseMsg : public Message {
  private:
    static const int promisedStartOffset = Message::messageSize;
    static const int promisedEndOffset = promisedStartOffset + sizeof(slot_t);
    static const int valuesOffset = promisedEndOffset + sizeof(slot_t);
    static const int votesOffset = valuesOffset + (maxPendingValues * sizeof(Value));
  public:
    static const int messageSize = votesOffset + (maxPendingValues * sizeof(Vote));
     
    PromiseMsg(char * data);
    PromiseMsg(char * data, node_t to, node_t from, ballot_t ballot, 
        slot_t promisedStart, slot_t promisedEnd);
    
    slot_t getPromisedStart();
    void   setPromisedStart(slot_t slot);

    slot_t getPromisedEnd();
    void   setPromisedEnd(slot_t slot);

    Value getValue(slot_t slot);
    void  setValue(slot_t slot, Value val);

    Vote getVote(slot_t slot);
    void setVote(slot_t slot, Vote vote);

    void print();
  };

  class AcceptMsg : public Message {
    static const int slotOffset = Message::messageSize;
    static const int valueOffset = slotOffset + sizeof(slot_t); 
  public:
    static const int messageSize = valueOffset + sizeof(Value);

    AcceptMsg(char * data);
    AcceptMsg(char * data, node_t to, node_t from, ballot_t ballot, 
              slot_t slot, Value value);

    slot_t getSlot();
    void setSlot(slot_t slot);

    Value getValue();
    void setValue(Value value);
  
    void print();
  };

  class AcceptedMsg : public AcceptMsg {
  public:
    AcceptedMsg(char * data);
    AcceptedMsg(char * data, node_t to, node_t from, 
                ballot_t ballot, slot_t slot, Value value);

    void print();
  };

  class HeartbeatMsg : public Message {
    static const int uptoOffset = Message::messageSize;
  public: 
    static const int messageSize = uptoOffset + sizeof(slot_t);

    HeartbeatMsg(char * data);
    HeartbeatMsg(char * data, node_t to, node_t from, ballot_t ballot);

    slot_t getUpto() { return get<slot_t>(uptoOffset); }

    void setUpto(slot_t slot) { set(uptoOffset, slot); }

    void print();
  };

  class QueryMsg : public Message {
    static const int startSlotOffset = Message::messageSize;
    static const int endSlotOffset = startSlotOffset + sizeof(slot_t);
  public:
    static const int messageSize = endSlotOffset + sizeof(slot_t);

    QueryMsg (char * data);
    QueryMsg (char * data, node_t to, node_t from, ballot_t ballot, slot_t start, slot_t end);

    slot_t getStartSlot();
    void setStartSlot(slot_t slot);

    slot_t getEndSlot();
    void   setEndSlot(slot_t slot);

    void print();
  };

  class ConfirmedMsg : public AcceptMsg {
    static const int voteOffset = AcceptMsg::messageSize;
  public:
    static const int messageSize = voteOffset + sizeof(Vote);

    ConfirmedMsg(char * data);
    ConfirmedMsg(char * data, node_t to, node_t from, 
                ballot_t ballot, slot_t slot, Value value, Vote vote);

    Vote getVote();
    void setVote(Vote vote);

    void print();
  };

  void processMessage(int length, char *message);

  void handlePrepare(PrepareMsg &prep);
  
  void handlePromise(PromiseMsg &prom);

  void handleAccept(AcceptMsg &accept);

  void handleAccepted(AcceptedMsg &accepted);

  void handleHeartbeat(HeartbeatMsg &heartbeat);

  void handleQuery(QueryMsg &query);

  void handleConfirmed(ConfirmedMsg &confirmed);

  bool isLeader();

  void sendAccept(slot_t slot) {
    char raw[Paxos::AcceptMsg::messageSize] = {0};
    AcceptMsg accept = AcceptMsg(raw, 0, id, latestBallot, 
                                 slot, log.getValue(slot));

    for (node_t i = 0; i < numServers; i++) {
      if (i == id) continue;
      accept.setTo(i);
      net.sendMessage(servers[i], Paxos::AcceptMsg::messageSize, raw);
    }
  }
};

#endif // PAXOS_HH

