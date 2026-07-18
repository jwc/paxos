#include "header.hh"

union Operation {
  Value raw;
  struct {
    int16_t change;
  };
};
static_assert(sizeof(Operation) == sizeof(Value));

class Number : public Application {
  Consensus &cons;
  int64_t changes = 0;
  int64_t number = 0;

  void processValue(Value value) override {
    //printf("APP:processValue(%d)\n", value);

    Operation o = { .raw = value };
    number += o.change;
    changes++;

    print();
  }

public:
  Number(Consensus &cons) : cons(cons) { cons.registerApplication(this); }

  int get() { return number; }

  void request(int num) { 
    Operation o;
    o.change = num;

    cons.requestValue(o.raw);
  }

  void print() {
      printf("%ld:%ld\n", changes, number);
  }
};

class NumberTask : public Task {
  Number & num;
  NumberTask(Number & num) : Task(Type::NON_BLOCKING, 2500), num(num) { ready(); }
  void executeTask() override {
    num.request(rand() % 64);

    new NumberTask(num);
  }
public: 
  static void run(Number & num) { new NumberTask(num); }
};

int main(int argc, char **argv) {
  std::cout << "argc:" << argc << "\n";
  if (argc <= 2) {
    Task::end();
    return 1;
  }

  Paxos pax = Paxos(argv[1], argv[1]);
  Number num = Number(pax);

  for (int i = 2; i < argc; i++) {
    pax.addServer(argv[i], argv[i]);
  }
  pax.finalizeServers();

  NumberTask::run(num);

  std::string line;
  int n;
  while (true) {
    num.print();

    getline(std::cin, line);
    if (sscanf(line.c_str(), "%d\n", &n) == 1) {
      num.request(n);

    } else {
      break;
    }
  }

  std::cout << " Ending - - - - - - - - - - - - - - -\n";
  Task::end();
  return 0;
}

