#ifndef PAXOS_HH
#define PAXOS_HH

typedef int Value;
typedef uint8_t node_t;
typedef uint32_t ballot_t;
typedef uint32_t slot_t;
typedef struct vote_t {
  uint32_t votes = 0;
  void castVote(node_t id) {
    votes |= 1 << id;
  }
  int getVoteCount() {
    return __builtin_popcount(votes);
  }
  void clear() { votes = 0; }
} Vote;
static_assert(sizeof(Vote) == sizeof(uint32_t));

class Paxos : Application {
public:
  Paxos(std::string name, std::string address); 

  void addServer(std::string name, std::string address);

  void finalizeServers();

  void processMessage(int length, char *message);

//private:
  IPv4                      net;
  std::vector<std::string>  servers;
  node_t                    numServers  = -1;
  node_t                    id          = -1;

  ballot_t                  latestBallot = 0;
  ballot_t                  myBallot = 0;
  Vote                      leaderVote;

  const static int          maxPendingValues = 4;

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
        printf("\t%ud:%ud:%ud\n", i, getValue(i), getVote(i).getVoteCount());
    }
  };
};

#endif // PAXOS_HH

