#include "header.hh"

union Operation {
  Value raw;
  struct {
    int16_t key;
    int16_t value;
  };
};
static_assert(sizeof(Operation) == sizeof(Value));

class Map : public Application {
  Consensus &cons;
  std::unordered_map<int, int> data;

  void processValue(Value value) override {
    printf("APP:processValue(%d)\n", value);

    Operation o = { .raw = value };
    data[o.key] = o.value;

    print();
  }

public:
  Map(Consensus &cons) : cons(cons) { cons.registerApplication(this); }

  int get(int i) { return data[i]; }

  void set(int i, int v) { 
    Operation o;
    o.key = i;
    o.value = v;

    cons.requestValue(o.raw);
  }

  void print() {
    int arraySize = 8;
    int rowSize = 8;
    for (int i = 0; i < arraySize; i += rowSize) {
      for (int j = i; j < i + rowSize; j++) {
        printf("%d:%d\t", j, get(j));
      }
      printf("\n");
    }
  }
};

class MapTask : public Task {
  Map & map;
  MapTask(Map & map) : Task(Type::NON_BLOCKING, 2500), map(map) { ready(); }
  void executeTask() override {
    map.set(rand() % 8, rand() % 32);

    new MapTask(map);
  }
public: 
  static void run(Map & map) { new MapTask(map); }
};

int main(int argc, char **argv) {
  std::cout << "argc:" << argc << "\n";
  if (argc <= 2) {
    Task::end();
    return 1;
  }

  Paxos pax = Paxos(argv[1], argv[1]);
  Map map = Map(pax);

  for (int i = 2; i < argc; i++) {
    pax.addServer(argv[i], argv[i]);
  }
  pax.finalizeServers();

  MapTask::run(map);

  std::string line;
  int num, key, val;
  while (true) {
    map.print();

    getline(std::cin, line);
    num = sscanf(line.c_str(), "%d %d\n", &key, &val);

    if (num == 2) {
      map.set(key, val);

    } else {
      break;
    }
  }

  std::cout << " Ending - - - - - - - - - - - - - - -\n";
  Task::end();
  return 0;
}

