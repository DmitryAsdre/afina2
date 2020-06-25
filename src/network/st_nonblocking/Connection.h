#ifndef AFINA_NETWORK_ST_NONBLOCKING_CONNECTION_H
#define AFINA_NETWORK_ST_NONBLOCKING_CONNECTION_H

#include <cstring>

#include <afina/execute/Command.h>
#include <sys/epoll.h>

#include <protocol/Parser.h>
#include <spdlog/logger.h>
#include <vector>

namespace Afina {
namespace Network {
namespace STnonblock {

class Connection {
public:
    Connection(int s, std::shared_ptr<Afina::Storage> &ps, std::shared_ptr<spdlog::logger> &p1) : 
    _socket(s), _pStorage(ps), _logger(p1) {
        std::memset(&_event, 0, sizeof(struct epoll_event));
        _event.data.ptr = this;
        _is_alive = true;
        _end_reading = false;
        _arg_remains = 0;
        _read_bytes = 0;
        _head_written_count = 0;
        std::memset(_read_buffer, 0, 4096);
    }

    inline bool isAlive() const { return _is_alive; }

    void Start();

protected:
    void OnError();
    void OnClose();
    void DoRead();
    void DoWrite();

private:
    friend class ServerImpl;

    bool _is_alive;
    bool _end_reading;

    int _socket;
    struct epoll_event _event;

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

};

} // namespace STnonblock
} // namespace Network
} // namespace Afina

#endif // AFINA_NETWORK_ST_NONBLOCKING_CONNECTION_H
