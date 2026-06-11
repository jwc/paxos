#ifndef INTERFACES_HH
#define INTERFACES_HH

typedef int Value;

class Application {
  public:
    virtual void processValue(Value) = 0;
};

class Consensus {
public:
  virtual void addServer(std::string name, std::string address) = 0;

  virtual void finalizeServers() = 0;

  virtual void processMessage(int, char*) = 0;

  virtual void requestValue(Value value) = 0;

  //virtual void registerApplication(Application *app) = 0;
};

class Networking {
public:
  virtual void addNode(std::string name, std::string address) = 0;

  virtual void sendMessage(std::string name, int length, char *message) = 0;
  
  virtual void registerConsensus(Consensus *consensus) = 0;
};

#endif // INTERFACES_HH

