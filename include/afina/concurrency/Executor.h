#ifndef AFINA_CONCURRENCY_EXECUTOR_H
#define AFINA_CONCURRENCY_EXECUTOR_H

#include <condition_variable>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unistd.h>

namespace Afina {
namespace Concurrency {

/**
 * # Thread pool
 */
class Executor {
    enum class State {
        // Threadpool is fully operational, tasks could be added and get executed
        kRun,

        // Threadpool is on the way to be shutdown, no ned task could be added, but existing will be
        // completed as requested
        kStopping,

        // Threadppol is stopped
        kStopped
    };

public:
    Executor(size_t low_watermark, size_t hight_watermark, size_t max_queue_size, size_t idle_time)
        : _low_watermark(low_watermark), _hight_watermark(hight_watermark), _max_queue_size(max_queue_size),
          _idle_time(idle_time) {}
    ~Executor() {
        Stop(true);
    }
    // Executor(std::string name, int size);
    //~Executor();

    /**
     * Signal thread pool to stop, it will stop accepting new jobs and close threads just after each become
     * free. All enqueued jobs will be complete.
     *
     * In case if await flag is true, call won't return until all background jobs are done and all threads are stopped
     */
    double getSizeRatio()
    {
        std::lock_guard<std::mutex> m(_mutex);
        double res = double(_cur_workers) / _hight_watermark;
        std::cout << res << " res" << std::endl;
        return res;
    }
    void Stop(bool await = false) {
        std::unique_lock<std::mutex> u_lock(_mutex);
        if (_state == State::kStopped)
            return;
        _state = State::kStopping;
        if (_tasks.empty()) {
            _empty_condition.notify_all();
        }
        if (await) {
            while (_state != State::kStopped)
                awaitStop.wait(u_lock);
        }
    }
    void Start() {
        {
            std::unique_lock<std::mutex> g_lock(_mutex);
            if (_state == State::kRun)
                return;
            _state = State::kRun;
            _cur_workers = 0;
        }
        for (size_t i = 0; i < _low_watermark; i++) {
            std::unique_lock<std::mutex> u_lock(_mutex);
            std::thread th(&Executor::perform, this);
            th.detach();
            _cur_workers++;
        }
    }

    /**
     * Add function to be executed on the threadpool. Method returns true in case if task has been placed
     * onto execution queue, i.e scheduled for execution and false otherwise.
     *
     * That function doesn't wait for function result. Function could always be written in a way to notify caller about
     * execution finished by itself
     */
    template <typename F, typename... Types> bool Execute(F &&func, Types... args) {
        // Prepare "task"
        auto exec = std::bind(std::forward<F>(func), std::forward<Types>(args)...);

        std::unique_lock<std::mutex> lock(this->_mutex);
        if (_state != State::kRun) {
            return false;
        }

        // Enqueue new task
        _tasks.push_back(exec);
        _empty_condition.notify_one();
        return true;
    }

private:
    // No copy/move/assign allowed
    Executor(const Executor &) = delete;
    Executor(Executor &&) = delete;
    Executor &operator=(const Executor &) = delete;
    Executor &operator=(Executor &&) = delete;

    /**
     * Main function that all pool threads are running. It polls internal task queue and execute tasks
     */
    void perform() {
        int counter = 0;
        while (true) {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> m(_mutex);
                if (!_tasks.empty()) {
                    task = _tasks.front();
                    _tasks.pop_front();
                } else if (_tasks.empty() && _state == State::kStopping) {
                    _cur_workers--;
                    if (_cur_workers == 0) {
                        _state = State::kStopped;
                        awaitStop.notify_all();
                        return;
                    }
                } else {
                    while (_tasks.empty()) {
                        if (_empty_condition.wait_for(m, std::chrono::milliseconds(10)) == std::cv_status::timeout) {
                            if (_cur_workers > _low_watermark) {
                                --_cur_workers;
                                return;
                            }
                            if (_state == State::kStopping && _tasks.empty()) {
                                --_cur_workers;
                                if (_cur_workers == 0) {
                                    _state = State::kStopped;
                                    awaitStop.notify_all();
                                }
                                return;
                            }
                        } else {
                            if (_state == State::kStopping && _tasks.empty()) {
                                --_cur_workers;
                                if (_cur_workers == 0) {
                                    _state = State::kStopped;
                                    awaitStop.notify_all();
                                }
                                return;
                            }
                        }
                    }
                }
                task = _tasks.front();
                _tasks.pop_front();
            }
            {
                std::lock_guard<std::mutex> g_lock(_mutex);
                if(!_tasks.empty() && _state == State::kRun && _cur_workers < _hight_watermark)
                {
                    ++_cur_workers;
                    std::thread th(&Executor::perform, this);
                    th.detach();
                }
            }
            task();
        }
    }

    /**
     * Mutex to protect state below from concurrent modification
     */
    std::mutex _mutex;

    /**
     * Conditional variable to await new data in case of empty queue
     */
    std::condition_variable _empty_condition;

    /**
     * Vector of actual threads that perorm execution
     */
    std::vector<std::thread> _threads;

    /**
     * Task queue
     */
    std::deque<std::function<void()>> _tasks;

    /**
     * Flag to stop bg threads
     */
    State _state;

    size_t _low_watermark;
    size_t _hight_watermark;
    size_t _max_queue_size;
    size_t _idle_time;
    size_t _cur_workers;

    std::condition_variable awaitStop;
};

} // namespace Concurrency
} // namespace Afina

#endif // AFINA_CONCURRENCY_EXECUTOR_H
