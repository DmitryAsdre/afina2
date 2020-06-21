#include "Connection.h"

#include <iostream>
#include <sys/socket.h>
#include <sys/uio.h>
#include <errno.h>

namespace Afina {
namespace Network {
namespace STnonblock {

// See Connection.h
void Connection::Start() {
    _logger->debug("Connection on {} socket started", _socket);
    _event.data.fd = _socket;
    _event.data.ptr = this;
    _event.events = EPOLLIN | EPOLLHUP | EPOLLERR;
}

// See Connection.h
void Connection::OnError() {
    _logger->warn("Connection on {} socket has error", _socket);
    _is_alive = false;
}

// See Connection.h
void Connection::OnClose() {
    _logger->debug("Connection on {} socket has closed", _socket);
    _is_alive = false;
}

// See Connection.h
void Connection::DoRead() {
    _logger->debug("DoRead on {} socket", _socket);
    //Эта часть скопирована из ServerImpl.cpp OnRun и добавлен logger
    try {
        int read_count = -1;
        while ((read_count = read(_socket, _read_buffer + _read_bytes, sizeof(_read_buffer) - _read_bytes)) > 0) {
            _read_bytes += read_count;
            _logger->debug("Got {} bytes from socket", read_count);

            while (_read_bytes > 0) {
                _logger->debug("Process {} bytes", _read_bytes);
                // There is no command yet
                if (!_command_to_execute) {
                    std::size_t parsed = 0;
                    try {
                        if (_parser.Parse(_read_buffer, _read_bytes, parsed)) {
                            // There is no command to be launched, continue to parse input stream
                            // Here we are, current chunk finished some command, process it
                            _logger->debug("Found new command: {} in {} bytes", _parser.Name(), parsed);
                            _command_to_execute = _parser.Build(_arg_remains);
                            if (_arg_remains > 0) {
                                _arg_remains += 2;
                            }
                        }
                    } catch (std::runtime_error &ex) { //Добавлен блок try catch
                        _output_queue.push_back("(?^u:ERROR)");
                        _event.events = EPOLLIN | EPOLLHUP | EPOLLERR | EPOLLOUT; //Поднимаем флаг, что надо еще писать. Изначально сами флаги состоят из EPOLLIN EPOLLHUP EPOLLERR без EPOLLOUT
                        throw std::runtime_error(ex.what());
                    }

                    // Parsed might fails to consume any bytes from input stream. In real life that could happens,
                    // for example, because we are working with UTF-16 chars and only 1 byte left in stream
                    if (parsed == 0) {
                        break;
                    } else {
                        std::memmove(_read_buffer, _read_buffer + parsed, _read_bytes - parsed);
                        _read_bytes -= parsed;
                    }
                }

                // There is command, but we still wait for argument to arrive...
                if (_command_to_execute && _arg_remains > 0) {
                    _logger->debug("Fill argument: {} bytes of {}", _read_bytes, _arg_remains);
                    // There is some parsed command, and now we are reading argument
                    std::size_t to_read = std::min(_arg_remains, std::size_t(_read_bytes));
                    _argument_for_command.append(_read_buffer, to_read);

                    std::memmove(_read_buffer, _read_buffer + to_read, _read_bytes - to_read);
                    _arg_remains -= to_read;
                    _read_bytes -= to_read;
                }

                // There is command & argument - RUN!
                if (_command_to_execute && _arg_remains == 0) {
                    _logger->debug("Start command execution");

                    std::string result;
                    _command_to_execute->Execute(*_pStorage, _argument_for_command, result);

                    // Send response
                    result += "\r\n";

                    _output_queue.push_back(result); //Добавляем в очередь на запись и ждем 
                    if (_output_queue.size() == 1) {
                        _event.events = EPOLLIN | EPOLLHUP | EPOLLERR | EPOLLOUT; //Если до добавления ответа не было ничего в очереди
                    }                                                             //добавляем флаг на отправку EPOLLOUT                           

                    // Prepare for the next command
                    _command_to_execute.reset();
                    _argument_for_command.resize(0);
                    _parser.Reset();
                }
            }
        } // while (read_count)
        _end_reading = true;
        if (_read_bytes == 0) {
            _logger->debug("Connection closed");
        } else {
            throw std::runtime_error(std::string(strerror(errno)));
        }
    } catch (std::runtime_error &ex) {
        _logger->error("Failed to process connection on descriptor {}: {}", _socket, ex.what());
        _end_reading = true;
    }
}

// See Connection.h
void Connection::DoWrite() {
    _logger->debug("DoWrite on {} socket", _socket);
    struct iovec tmp[_output_queue.size()];

    size_t i(0);
    for(i = 0; i < _output_queue.size(); ++i){
        tmp[i].iov_base = &(_output_queue[i][0]);
        tmp[i].iov_len = _output_queue[i].size();
    }

    tmp[0].iov_base = static_cast<char *>(tmp[0].iov_base) + _head_written_count; //Может быть часть команды или чего-то там уже записали в строчке 133. Там мб остаток быть из команды
    tmp[0].iov_len -= _head_written_count;

    int written_bytes = writev(_socket, tmp, i); //writev syscall write multiple buffers with one syscall

    if(written_bytes <= 0){
        if(errno != EINTR && errno != EAGAIN && errno != EPIPE){
            _is_alive = false;
            throw std::runtime_error("Failed to send response");
        }
    }
    i = 0;
    for(size_t j = 0; j < _output_queue.size(); j++){ // все что записали надо посчитать и удалить
        auto command = _output_queue[j];
        if(written_bytes - command.size() >= 0)
        {
            ++i;
            written_bytes -= command.size();
        }
        else{
            break;
        }
    }
    _output_queue.erase(_output_queue.begin(), _output_queue.begin() + i); //удаление всего, что записали полностью
    _head_written_count = written_bytes;
}

} // namespace STnonblock
} // namespace Network
} // namespace Afina
