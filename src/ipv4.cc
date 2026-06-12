#include "header.hh"

// Public:

IPv4::IPv4(std::string name, std::string address) {
  //std::cout << "IPv4 Constructor\n";
  std::lock_guard<std::mutex> lock(mtx);

  nodeName = name;
  nameToId[name] = 0;
  idToAddr[0] = stringToSockaddr(address); 

  int sock = setupSocket(0);
  if (sock < 0) {
    std::cerr << "ERROR: setupSocket\n";
  }

  if (bind(sock, (struct sockaddr *)&idToAddr[0], sizeof(struct sockaddr_in))) {
    std::cerr << "ERROR: bind\n";
  }

  if (listen(sock, 3)) {
    std::cerr << "ERROR: listen\n";
  }

  idToSock[0] = sock;
  openSockets.insert(sock);

  //std::cout << "Create ListenerTask\n";
  new ListenerTask(this, sock);

  new EndNetTask(this);
}

void IPv4::addNode(std::string name, std::string address) {
  std::lock_guard<std::mutex> lock(mtx);

  int id = -1;
  if (nameToId.contains(name)) {
    id = nameToId[name];
  } else {
    id = nameToId[name] = nameToId.size();
  }

  if ( ! idToAddr.contains(id)) {
    idToAddr[id] = stringToSockaddr(address);
  }
}

// Private:

struct sockaddr_in IPv4::stringToSockaddr(std::string input) {
  struct sockaddr_in output;
  union {
    uint32_t addr;
    struct { uint8_t byte0, byte1, byte2, byte3; };
  } ipAddress = { .addr = INADDR_ANY };
  uint16_t port = DEFAULT_PORT;

  sscanf(input.c_str(), "%hhd.%hhd.%hhd.%hhd:%hd", &ipAddress.byte0, 
      &ipAddress.byte1, &ipAddress.byte2, &ipAddress.byte3, &port); 

  output.sin_family = AF_INET;
  output.sin_addr.s_addr = ipAddress.addr;
  output.sin_port = htons(port);

  return output;
}

int IPv4::setupSocket(int id) {
  if ( ! idToAddr.contains(id)) return -1;

  int sock = socket(PF_INET, SOCK_STREAM, 0);

  if (sock < 0) {
    std::cerr << "ERROR: socket\n";
    return -1;
  }

  int optVal = 1;
  if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &optVal, sizeof(int)) < 0) {
    std::cerr << "ERROR: setsockopt-1\n";
    return -1;
  }

  if (setsockopt(sock, SOL_SOCKET, SO_REUSEPORT, &optVal, sizeof(int)) < 0) {
    std::cerr << "ERROR: setsockopt-2\n";
    return -1;
  }

  return sock;
}

int IPv4::connectSocket(int id) {
  int sock = setupSocket(id);
  if (sock < 0) {
    return -1;
  }


  return sock;
}

void IPv4::sendMessage(int id, int length, char *message) {
  int socket = -1;
  {
    std::lock_guard<std::mutex> lock(mtx);

    if (id < 0 || id >= (int) nameToId.size()) return;

    if (idToSock.contains(id)) {
      socket = idToSock[id];
    } else {
      if ((socket = setupSocket(id)) == -1) return;
      openSockets.insert(socket);

      if (connect(socket, (const sockaddr*)&idToAddr[id], 
            sizeof(struct sockaddr_in)) == -1) {
        close(socket);
        return;
      }

      idToSock[id] = socket;
      uint32_t nameLen = nodeName.size();
      uint32_t netOrderNameLen = htonl(nameLen);
      send(socket, &netOrderNameLen, sizeof(netOrderNameLen), 0);
      send(socket, nodeName.c_str(), nameLen, 0); 

      new ReceiverTask(this, socket);
    }

    uint32_t netOrderLength = htonl(length);
    send(socket, &netOrderLength, sizeof(netOrderLength), 0);
    send(socket, message, length, 0);
  }
}

void IPv4::sendMessage(std::string name, int length, char *message) {
  if ( ! nameToId.contains(name)) return;

  int id = nameToId[name];

  if (id == 0) {
    consensus->processMessage(length, message);
    return;
  }

  sendMessage(id, length, message);
}

