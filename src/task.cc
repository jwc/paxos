#include "header.hh" 

// Task:

Task::Task(Type type, int delay) : type(type), delay(delay) {}

void Task::ready() {
  if (enqueued) return;
  enqueued = true;

  if (delay == 0) 
    taskManager.enqueue(this);
  else 
    taskManager.enqueue(this, delay);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
}

void Task::registerCleanupTask() {
  if (enqueued) return;
  enqueued = true;

  taskManager.enqueueCleanup(this); 
}

void Task::end() { taskManager.end(); }

// TaskManager: 

Task::TaskManager Task::taskManager;

Task::TaskManager::TaskManager() {
  int poolSize = std::thread::hardware_concurrency(); 
  poolSize = poolSize > maxPoolSize ? maxPoolSize : poolSize;
  for (int i = 0; i < poolSize; i++) {
    std::thread t(&TaskManager::worker, this, Type::NON_BLOCKING);
    nonblockingThreadPool.push_back(move(t));
  }

  TimerTask::create();
}

void Task::TaskManager::worker(Type type) {
  Task *task = nullptr;

  if (type == Type::NON_BLOCKING) {
    while (true) {
      if (task != nullptr) {
        task->executeTask();
        delete task;
      }

      std::unique_lock<std::mutex> lock(nonblockingLock);
      nonblockingCV.wait(lock, [&] { 
        return ( ! nonblockingTasks.empty()) || shouldEnd;
      });

      if (shouldEnd) return;

      task = nonblockingTasks.back();
      nonblockingTasks.pop_back();
    }
  } else {
    while (true) {
      if (task != nullptr) {
        task->executeTask();
        delete task;
      }

      std::unique_lock<std::mutex> lock(blockingLock);
      blockingThreadsWaiting++;
      blockingCV.wait(lock, [&] { 
        return ( ! blockingTasks.empty()) || shouldEnd; 
      });
      blockingThreadsWaiting--;

      if (shouldEnd) return;

      task = blockingTasks.back();
      blockingTasks.pop_back();
    }
  }
}

void Task::TaskManager::enqueue(Task *task) { 
  if (task->type == Type::NON_BLOCKING) {
    std::lock_guard<std::mutex> lock(nonblockingLock);

    nonblockingTasks.push_front(task);

    nonblockingCV.notify_one();
  } else {
    std::lock_guard<std::mutex> lock(blockingLock);

    blockingTasks.push_front(task);
    
    if (blockingThreadsWaiting > 0) {
      blockingCV.notify_one();
    } else if (! shouldEnd) {
      std::thread t(&TaskManager::worker, this, Type::BLOCKING);
      blockingThreadPool.push_back(move(t));
    }
  }
}

void Task::TaskManager::enqueue(Task *task, int delay) {
  std::lock_guard<std::mutex> lock(waitingLock);

  waitingTasks.push({std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(delay), task});
}

void Task::TaskManager::updateWaitingTasks() {
  using std::chrono::time_point;
  using std::chrono::high_resolution_clock;
  using std::chrono::milliseconds;

  time_point<high_resolution_clock> timestamp = high_resolution_clock::now();

  std::lock_guard<std::mutex> lock(waitingLock);
  while (( ! waitingTasks.empty()) && waitingTasks.top().first < timestamp) {
    this->enqueue(waitingTasks.top().second);
    waitingTasks.pop();
  }
}

void Task::TaskManager::enqueueCleanup(Task *task) {
  std::lock_guard<std::mutex> lock(taskManager.cleanupLock);
  cleanupTasks.push_back(task);
}

void Task::TaskManager::end() { 
  shouldEnd = true; 
  nonblockingCV.notify_all();
  blockingCV.notify_all();

  while ( ! cleanupTasks.empty()) {
    Task * task = cleanupTasks.front();
    cleanupTasks.pop_front();
    task->executeTask();
    delete task;
  }

  while ( ! nonblockingThreadPool.empty()) {
    nonblockingThreadPool[0].join();
    nonblockingThreadPool.pop_front();
  }

  while ( ! blockingThreadPool.empty()) {
    blockingThreadPool[0].join();
    blockingThreadPool.pop_front();
  }
  
  while ( ! nonblockingTasks.empty()) {
    delete nonblockingTasks.back();
    nonblockingTasks.pop_back();
  }

  while ( ! blockingTasks.empty()) {
    delete blockingTasks.back();
    blockingTasks.pop_back(); 
  }

  while ( ! waitingTasks.empty()) {
    delete waitingTasks.top().second;
    waitingTasks.pop();
  }
}

// TimerTask:

TimerTask::TimerTask() : Task(Type::BLOCKING, 0) { ready(); }

void TimerTask::create() { new TimerTask(); }

void TimerTask::executeTask() {
  this->taskManager.updateWaitingTasks();

  std::this_thread::sleep_for(std::chrono::milliseconds(1));

  TimerTask::create();
}

