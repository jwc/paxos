#ifndef IPV4_HH
#define IPV4_HH

class Networking {
public:
  Networking (std::string name, std::string address);
private: 
};

class Application {
public:
  virtual void processMessage(int, char*) = 0;
};

/**
 * A network object for connecting with other nodes via IPv4. 
 */
class IPv4 : public Networking {
public:
  static const uint16_t DEFAULT_PORT = 9876;

  explicit IPv4(std::string name, std::string address);

  void addNode(std::string name, std::string address);

  void sendMessage(std::string name, int length, char *message);

  void registerApp(Application *app) { this->app = app; }

protected:

private:
  std::string nodeName;
  std::mutex mtx;
  struct sockaddr_in stringToSockaddr(std::string input);
  int setupSocket(int id);
  int connectSocket(int id);
  int connectNode(int id);
  void sendMessage(int id, int length, char *message);
  std::unordered_map<std::string, int> nameToId;
  std::unordered_map<int, struct sockaddr_in> idToAddr;
  std::unordered_map<int, int> idToSock;
  std::set<int> openSockets;
  Application *app = nullptr;
  
  friend class ReceiverTask;
  friend class ListenerTask;
  friend class EndNetTask;
};

class IPv4Task : public Task {
protected:
  IPv4 *net;
  int socket;
  IPv4Task(IPv4 *net, int socket)
    : Task(Type::BLOCKING, 0),
      net(net), 
      socket(socket) {}
  //virtual void executeTask() = 0;
};

class ReceiverTask : IPv4Task {
public:
  ReceiverTask(IPv4 *net, int socket) : IPv4Task(net, socket) { ready(); }
  void executeTask() override {
    int id = -1;

    std::cout << "ReceiverTask Created.\n";

    while (true) {
      uint32_t msgSize = 0;
      if (read(socket, &msgSize, sizeof(msgSize)) != sizeof(msgSize)) {
        break;
      }

      msgSize = ntohl(msgSize);
      char *message = new char[msgSize + 1];
      message[msgSize] = '\0';
      if (read(socket, message, msgSize) != msgSize) {
        delete[] message;
        break;
      }

      if (id == -1) {
        std::lock_guard<std::mutex> lock(net->mtx);
        std::string name(message);

        if (net->nameToId.contains(name)) {
          id = net->nameToId[name];
          if ( ! net->idToSock.contains(id)) {
            net->idToSock[id] = socket;
          } 
        } else {
          id = net->nameToId[name] = net->nameToId.size();
          net->idToSock[id] = socket;
        }
        
        delete[] message;

      } else {
        // Do something w/ the message
        std::cout << "RECV:'" << message << "'\n";

        if (net->app != nullptr) net->app->processMessage(msgSize, message);

        delete[] message;
      }
    }

    {
      std::lock_guard<std::mutex> lock(net->mtx);

      if (net->openSockets.contains(socket)) {
        net->openSockets.erase(socket);
        shutdown(socket, SHUT_RD);

        if (net->idToSock.contains(id) && net->idToSock[id] == socket) {
          net->idToSock.erase(id);
        }
      }
    }

    std::cout << "FIN RECEIVERTASK\n";
  }
};


class ListenerTask : IPv4Task {
public:
  ListenerTask(IPv4 *net, int socket) : IPv4Task(net, socket) { ready(); }
  void executeTask() override {

    std::cout << "ListenerTask Created:" << socket << "\n";

    while (true) {
      struct sockaddr_in  respAddress;
      int                 alen = sizeof( respAddress );
      int                 respSocket = accept( socket, 
                                (struct sockaddr *) &respAddress, 
                                (socklen_t*) &alen);
      if (respSocket < 0) {
        printf("Accept\n");
        fprintf(stderr, "ERRNO:%d\n", errno);
        std::cout << "FIN LISTENERTASK:" << socket << "\n";
        return;
      }

      uint32_t nameLen = net->nodeName.size();
      uint32_t netOrderNameLen = htonl(nameLen);
      send(respSocket, &netOrderNameLen, sizeof(netOrderNameLen), 0);
      send(respSocket, net->nodeName.c_str(), nameLen, 0);

      fprintf(stderr, "socket %d added.\n", respSocket);
      net->openSockets.insert(respSocket);
      new ReceiverTask(net, respSocket);
    }
  }
};

class EndNetTask: IPv4Task {
public:
  EndNetTask(IPv4 *net) : IPv4Task(net, 0) { registerCleanupTask(); }
  void executeTask() override {
    std::lock_guard<std::mutex> lock(net->mtx);
    std::cout << "END NET:" << net->openSockets.size() << "\n";

    for (int sock : net->openSockets) {
      shutdown(sock, SHUT_RD);
    }
    net->openSockets.clear();
    net->idToSock.clear();

    std::cout << "FIN ENDNETTASK\n";
  }
};


#endif // IPV4_HH

