#include "header.hh"

class OtherTask : Task {
private:
  OtherTask() : Task(Type::NON_BLOCKING, 2500) { ready(); }

  void executeTask() override {
    printf("Ran OtherTask\n");
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
    OtherTask::create();
  } 

public:
  static void create() { new OtherTask(); }
};

int main() {
  //Task::create(150000000);
  //Task::create(500);
  //Task::create(1500);
  printf("Created\n");
  OtherTask::create();

  //IPv4 net1 = IPv4("node1", "127.0.0.1:8989");
  //IPv4 net2 = IPv4("node2", "127.0.0.1:9898");
  
  Paxos pax1 = Paxos("node1", "127.0.0.1:8989");
  pax1.addServer("node2", "127.0.0.1:9898");
  pax1.finalizeServers();

  Paxos pax2 = Paxos("node2", "127.0.0.1:9898");
  pax2.addServer("node1", "127.0.0.1:8989");
  pax2.finalizeServers();

  std::this_thread::sleep_for(std::chrono::milliseconds(6000));
  std::cout << "Sending REqusts - - - - - - - - - - - - - - -\n";
  pax1.requestValue(11);
  pax2.requestValue(22);  

  //net1.addNode("node2", "127.0.0.1:9898");
  //net2.addNode("node1", "127.0.0.1:8989");
  //std::this_thread::sleep_for(std::chrono::milliseconds(500));
  //char c1[] = "1 2 3\0004 5 6\0007 8 9";
  //char c2[Paxos::PromiseMsg::messageSize] = {0};
  //Paxos::Message m1 = Paxos::Message(c2, Paxos::Type::PREPARE, 1, 2, 7);
  //Paxos::PromiseMsg m2 = Paxos::PromiseMsg(c2, 1, 3, 7, 11, 13);
  //m1.getType();
  //m2.getType();
  //net1.sendMessage("node2", Paxos::PromiseMsg::messageSize, c2);
  //std::this_thread::sleep_for(std::chrono::milliseconds(500));
  //net2.sendMessage("node1", strlen(m1), m1);
  
  //for (int i = 0; i < 5; i++) { OtherTask::create(); }
  //for (int i = 0; i < 5; i++) Task::create();
  
  std::this_thread::sleep_for(std::chrono::milliseconds(8000));
  printf("main ending\n");
  Task::end();

  printf("main returning\n");
  return 0;
}

