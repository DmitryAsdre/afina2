#ifndef AFINA_NETWORK_MT_NONBLOCKING_CONNECTION_H
#define AFINA_NETWORK_MT_NONBLOCKING_CONNECTION_H

#include <cstring>

#include <sys/epoll.h>
#include <afina/execute/Command.h>
#include <protocol/Parser.h>
#include <spdlog/logger.h>
#include <vector>
#include <atomic>

namespace Afina {
namespace Network {
namespace MTnonblock {

class Connection {
public:
    Connection(int s, std::shared_ptr<Afina::Storage> &ps, std::shared_ptr<spdlog::logger> &p1) : 
    _socket(s),
    _pStorage(ps),
    _logger(p1)
     {
        std::memset(&_event, 0, sizeof(struct epoll_event));
        _is_alive.store(true );//,std::memory_order_release); // what the hell?
        _data_available.store(false);//, std::memory_order_release); //what the hell?
        _read_bytes = 0;
        _head_written_count = 0;
        _event.data.ptr = this;
    }

    inline bool isAlive() const { return _is_alive; }

    void Start();

protected:
    void OnError();
    void OnClose();
    void DoRead();
    void DoWrite();

private:
    friend class Worker;
    friend class ServerImpl;

    std::atomic<bool> _is_alive;
    std::atomic<bool> _data_available;

    int _socket;
    std::vector<std::string> _output_queue;
    char _read_buffer[4096];
    size_t _read_bytes;
    int _head_written_count;
    std::shared_ptr<spdlog::logger> _logger;
    std::shared_ptr<Afina::Storage> _pStorage;

    std::size_t _arg_remains;
    Protocol::Parser _parser;
    std::string _argument_for_command;
    std::unique_ptr<Execute::Command> _command_to_execute;
    struct epoll_event _event;
};

} // namespace MTnonblock
} // namespace Network
} // namespace Afina

#endif // AFINA_NETWORK_MT_NONBLOCKING_CONNECTION_H
