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
      break;
    }
  }

  myBallot = id + numServers;
}

void Paxos::processMessage(int length, char *message) {
  std::cout << "processMessage()\n";
  if (numServers < 0) {
    return;
  }

  if (length < Message::messageSize) {
    std::cerr << "ERROR: Message Too Short!\n";
    return;
  }

  Message m = Message(message);
  switch (m.getType()) {
    case PREPARE:
      {
        PrepareMsg prep = PrepareMsg(message);
        prep.print();
        handlePrepare(prep);
        break;
      }
    case PROMISE:
      {
        if (length < PromiseMsg::messageSize) {
          std::cerr << "ERROR: Prom. Msg. Too Short!\n";
          return;
        }
        PromiseMsg prom = PromiseMsg(message);
        prom.print();
        break;
      }
    case ACCEPT:
    case ACCEPTED:
    case HEARTBEAT:
    case REQUEST:
      m.print();
      break;
    default:
      std::cerr << "ERROR: Invalid Message Type Recieved!\n";
      m.print();
  }
}

void Paxos::handlePrepare(Paxos::PrepareMsg &prep) {
  std::cout<<"handlePrep(): "<<latestBallot<<" vs "<<prep.getBallot()<<"\n";
  if (prep.getBallot() > latestBallot) {
    std::cout << "TRUE\n";

    latestBallot = prep.getBallot();
    char bytes[Paxos::PromiseMsg::messageSize];
    for (int i = 0; i < Paxos::PromiseMsg::messageSize; i++) bytes[i] = 0;
    PromiseMsg prom = Paxos::PromiseMsg(bytes, 0, id, latestBallot, log.getPendingStart(), log.getPendingEnd());

    std::cout << "HI1\n";
    slot_t i;
    for (i = log.getPendingStart(); i < log.getPendingEnd(); i++) {
      if (log.isFilled(i)) {
        prom.setValue(i, log.getValue(i));
        prom.setVote(i, log.getVote(i));
      }
    }
    
    printf("# servers: %d\n", numServers);
    std::cout << "numServers:" << numServers << "\n";
    for (i = 0; i < numServers; i++) {
      prom.setTo(i);
      std::cout << id << "->" << i << std::endl;
      net.sendMessage(servers[i], Paxos::PromiseMsg::messageSize, bytes);
    }
  }
}
