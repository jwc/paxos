#ifndef PAXOS_HH
#define PAXOS_HH

class Paxos : Application {
public:
  Paxos(std::string name, std::string address); 

  void addServer(std::string name, std::string address);

  void finalizeServers();

  void processMessage(int length, char *message);

private:
  IPv4                      net;
  std::vector<std::string>  servers;
  int                       numServers  = -1;
  int                       id          = -1;

  class Message {
  public:
    enum Type : uint8_t { 
      PREPARE, 
      PROMISE,
      ACCEPT,
      ACCEPTED,
      HEARTBEAT,
      REQUEST
    };
    Type type;

    int to;
    int from;

  };
};

#endif // PAXOS_HH

