#include "header.hh"

Paxos::Paxos(std::string name, std::string address) : net(name, address) {
  servers.push_back(name);
  net.registerApp(this);
}

void Paxos::addServer(std::string name, std::string address) {
  if (numServers != -1) return;

  net.addNode(name, address);

  servers.push_back(name);
}

void Paxos::finalizeServers() {
  numServers = servers.size();
  std::string name = servers[0];

  std::sort(servers.begin(), servers.end());

  for (int i = 0; i < numServers; i++) {
    if (servers[i] == name) {
      id = i; 
      return;
    }
  }
}

void Paxos::processMessage(int length, char *message) {
  std::cout << "processMessage()\n";
  if (numServers < 0) {
    return;
  }

  Message *msg = new Message();

  int32_t *ptr = (int32_t *) message;
  Message::Type type = (Message::Type) ptr[0];
  printf("test:%d ex:%d sizeof():%ld\n", (int) type, Message::Type::ACCEPT, sizeof(Message::Type));

  if (sscanf(message, "%d %d %d", (int*) &msg->type, &msg->to, &msg->from) != 3) {
    delete msg;
    std::cerr << "ERROR: bad sscanf()\n";
    return;
  }

  printf("MSG: type:%d to:%d from:%d\n", msg->type, msg->to, msg->from);
  delete msg;

}

