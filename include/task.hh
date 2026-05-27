#ifndef TASK_HH
#define TASK_HH

class Task {
public:
  enum Type { BLOCKING, NON_BLOCKING };

  virtual void ready() final;

  virtual void registerCleanupTask() final;

  static void end(); 

protected:
  explicit Task(Type type = Type::NON_BLOCKING, int delay = 0);

  virtual ~Task() = default;

  virtual void executeTask() = 0;

private:
  enum Type type;
  int delay;
  bool enqueued = false;

  friend class TimerTask;

  static class TaskManager {
  public:
    TaskManager();

    void enqueue(Task *task);
    
    void enqueue(Task *task, int delay);

    void enqueueCleanup(Task *task);

    void updateWaitingTasks();

    void end();

  private:
    using clock = std::chrono::high_resolution_clock;
    using p = std::pair<std::chrono::time_point<clock>, Task*>;
    using min_heap = std::priority_queue<p, std::deque<p>, std::greater<p>>;

    std::deque<Task*>       nonblockingTasks;
    std::deque<std::thread> nonblockingThreadPool;
    std::mutex              nonblockingLock;
    std::condition_variable nonblockingCV;
    const static int        maxPoolSize = 16;
    bool                    shouldEnd = false;

    std::deque<Task*>       blockingTasks;
    std::deque<std::thread> blockingThreadPool;
    std::mutex              blockingLock;
    std::condition_variable blockingCV;
    int                     blockingThreadsWaiting = 0;

    min_heap                waitingTasks;
    std::mutex              waitingLock;

    std::mutex              cleanupLock;
    std::deque<Task*>       cleanupTasks;

    void worker(Type type);
  } taskManager;
};

class TimerTask : Task {
private:
  friend class Task::TaskManager;

  TimerTask();

  static void create(); 

  void executeTask() override;
};

#endif // TASK_HH

