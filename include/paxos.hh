#ifndef PAXOS_HH
#define PAXOS_HH

class Paxos {
public:
  Paxos(std::string name, std::string address); 

  void addServer(std::string name, std::string address);

  void finalizeServers();

private:
  IPv4                      net;
  std::vector<std::string>  servers;
  int                       numServers  = -1;
  int                       id          = -1;
};

#endif // PAXOS_HH

