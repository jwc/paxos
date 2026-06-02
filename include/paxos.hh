#ifndef PAXOS_HH
#define PAXOS_HH

typedef int Value;
typedef uint8_t node_t;
typedef uint32_t ballot_t;

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
  private:
    char * data; 
    static const int typeOffset   = 0;
    static const int toOffset     = typeOffset  + sizeof(Type);
    static const int fromOffset   = toOffset    + sizeof(node_t);
    static const int ballotOffset = fromOffset  + sizeof(node_t);

  public:
    static const int messageSize = ballotOffset + sizeof(ballot_t);

    Message(char * data) : data(data) {}
    Message(char * data, Type type, node_t to, node_t from, ballot_t ballot) : data(data) {
      setType(type);
      setTo(to);
      setFrom(from);
      setBallot(ballot);
    }

    Type getType() { return *((Type *) (data + typeOffset)); }
    void setType(Type type) { *((Type *) (data + typeOffset)) = type; }

    node_t getTo() { return *((node_t *) (data + toOffset)); }
    void setTo(node_t to) { *((node_t *) (data + toOffset)) = to; }

    node_t getFrom() { return *((node_t *) (data + fromOffset)); }
    void setFrom(node_t from) { *((node_t *) (data + fromOffset)) = from; } 

    ballot_t getBallot() { return *((ballot_t *) (data + ballotOffset)); }
    void setBallot(ballot_t ballot) { *((ballot_t *) (data + ballotOffset)) = ballot; }
    
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

  /*
  class PromiseMsg : public Message {
  public:
    uint32_t promisedStart;
    uint32_t promisedEnd;
    Value    commitments[maxPendingValues];
    uint32_t commitmentVals[maxPendingValues];
  };
  */
};

#endif // PAXOS_HH

