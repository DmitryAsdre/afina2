#include "SimpleLRU.h"

namespace Afina {
namespace Backend {

void SimpleLRU::MoveToHead(lru_node &head) {
    if (!head.prev)
        return;
    auto tmp = std::move(head.prev->next);
    if (tmp->next)
        tmp->next->prev = tmp->prev;
    else
        tail = tmp->prev;
    tmp->prev->next = std::move(tmp->next);
    tmp->prev = nullptr;
    _lru_head->prev = tmp.get();
    tmp->next = std::move(_lru_head);
    _lru_head = std::move(tmp);
}

void SimpleLRU::FreeSpace(size_t s) {
    lru_node *node = tail;
    while (s > 0) {
        size_t all_size = node->key.size() + node->value.size();
        _lru_index.erase(node->key);
        node = node->prev;
        if (node == nullptr)
            _lru_head.reset();
        else
            node->next.reset();
        _cur_size -= all_size;
        if (all_size >= s)
            s = 0;
        else
            s -= all_size;
    }
    tail = node;
}

void SimpleLRU::SetHead(const std::string &key, const std::string &value) {
    std::unique_ptr<lru_node> new_head(new lru_node({key, value}));
    new_head->prev = nullptr;
    if (_lru_head)
        _lru_head->prev = new_head.get();
    else
        tail = new_head.get();
    new_head->next = std::move(_lru_head);
    _lru_head = std::move(new_head);
    _lru_index.insert({_lru_head->key, *_lru_head});
}

// See MapBasedGlobalLockImpl.h
bool SimpleLRU::Put(const std::string &key, const std::string &value) {
    if (key.size() + value.size() > _max_size)
        return false;
    auto it = _lru_index.find(key);
    if (it == _lru_index.end()) {
        if (key.size() + value.size() + _cur_size > _max_size)
            FreeSpace(key.size() + value.size() + _cur_size - _max_size);
        SetHead(key, value);
        _cur_size += key.size() + value.size();
    } else {
        auto & node = it->second.get();
        MoveToHead(node);
        if (_max_size < _cur_size - node.value.size() + value.size())
            FreeSpace(_cur_size - node.value.size() + value.size() - _max_size);
        _cur_size -= node.value.size();
        node.value = value;
        _cur_size += value.size();
    }
    return true;
}

// See MapBasedGlobalLockImpl.h
bool SimpleLRU::PutIfAbsent(const std::string &key, const std::string &value) {
    if (key.size() + value.size() > _max_size)
        return false;
    auto it = _lru_index.find(key);
    if (it == _lru_index.end()) {
        if (key.size() + value.size() + _cur_size > _max_size)
            FreeSpace(key.size() + value.size() + _cur_size - _max_size);
        SetHead(key, value);
        _cur_size += key.size() + value.size();
    } else
        return false;
    return true;
}

// See MapBasedGlobalLockImpl.h
bool SimpleLRU::Set(const std::string &key, const std::string &value) {
    if (key.size() + value.size() > _max_size)
        return false;
    auto it = _lru_index.find(key);
    if (it == _lru_index.end())
        return false;
    auto & node = it->second.get();
    MoveToHead(node);
    if (_max_size < _cur_size - node.value.size() + value.size())
        FreeSpace(_cur_size - node.value.size() + value.size() - _max_size);
    _cur_size -= node.value.size();
    node.value = value;
    _cur_size += node.value.size();
    return true;
}

// See MapBasedGlobalLockImpl.h
bool SimpleLRU::Delete(const std::string &key) {
    auto it = _lru_index.find(key);
    if (it == _lru_index.end())
        return false;
    auto & node = it->second.get();
    _cur_size -= key.size() + node.value.size();
    MoveToHead(node);
    auto tmp = std::move(_lru_head);
    if(tmp->next)
        _lru_head = std::move(tmp->next);
        _lru_head->prev = nullptr;
    _lru_index.erase(it);
    tmp.reset();
    return true;
}

// See MapBasedGlobalLockImpl.h
bool SimpleLRU::Get(const std::string &key, std::string &value) {
    auto it = _lru_index.find(key);
    if (it == _lru_index.end())
        return false;
    value = it->second.get().value;
    MoveToHead(it->second.get());
    return true;
}

SimpleLRU::~SimpleLRU() {
    _lru_index.clear();
    if (_lru_head) {
        lru_node *node = _lru_head.get();
        while (node->next)
            node = node->next.get();
        while (node->prev != nullptr) {
            node = node->prev;
            node->next.reset();
        }
    }
}

} // namespace Backend
} // namespace Afina
