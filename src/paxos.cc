#include "header.hh"

Paxos::Paxos(std::string name, std::string address) : net(name, address) {
  servers.push_back(name);
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

