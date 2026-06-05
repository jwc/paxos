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
  slot_t pendingStart = 0;
  slot_t pendingEnd = 0;

public:
  const static int maxPending = 4;
  Log(node_t &maxVotes) : maxVotes(maxVotes) {}

  slot_t getPendingStart() { return pendingStart; }
  slot_t getPendingEnd() { return pendingEnd; }

  Value getValue(slot_t slot) { return values[slot]; }

  Vote getVote(slot_t slot) { return votes[slot]; }

  void setValue(slot_t slot, Value value) 
  { if (isWritable(slot)) values[slot] = value; }

  void setVote(slot_t slot, Vote vote) 
  { if (isWritable(slot)) votes[slot] = vote; }

  bool isPending(slot_t slot) 
  { return isFilled(slot) && ! votes[slot].hasMajorityOf(maxVotes); }

  bool isConfirmed(slot_t slot) 
  { return isFilled(slot) && votes[slot].hasMajorityOf(maxVotes); }

  bool isFilled(slot_t slot) 
  { return values.contains(slot) && votes.contains(slot); }

  bool isWritable(slot_t slot) 
  { return slot >= pendingStart && slot < pendingStart + maxPending; }
};

class Paxos : Application {
//private:
  IPv4                      net;
  std::vector<std::string>  servers;
  node_t                    numServers  = -1;
  node_t                    id          = -1;

  ballot_t                  latestBallot = 0;
  ballot_t                  myBallot = 0;
  Vote                      leaderVote;
  Log                       log{numServers};

  const static int          maxPendingValues = Log::maxPending;
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

public:
  Paxos(std::string name, std::string address); 

  void addServer(std::string name, std::string address);

  void finalizeServers();

  void processMessage(int length, char *message);

  void handlePrepare(PrepareMsg &prep);

};

#endif // PAXOS_HH

