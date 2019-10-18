#ifndef __lmdbpp
#define __lmdbpp

#include <lmdb.h>
#include <stdexcept>
#include <utility>
#include <vector>

namespace lmdbpp
{
class Error : public std::runtime_error
{
public:
    const int code;
    Error(const char* what, const int return_code) : runtime_error{what}, code(return_code) {}
};

void check(int return_code)
{
    if (return_code) throw Error{mdb_strerror(return_code), return_code};
}

template <typename T = void>
class Val
{
public:
    Val() {}
    Val(const T* data) : _val{sizeof(T), (void*)data} {}
    Val(const T* data, size_t size) : _val{size, (void*)data} {}
    Val(const std::string& str) : _val{str.length(), (void*)str.c_str()} {}
    Val(const Val& o) = default;

    const T* data() { return const_cast<T*>((char*)_val.mv_data); }
    void data(T* d) { _val.mv_data = d; }
    size_t size() { return _val.mv_size; }
    void size(size_t s) { _val.mv_size = s; }

    std::string as_str() const { return std::string{const_cast<char*>((char*)_val.mv_data), _val.mv_size}; }

    operator MDB_val*() { return &_val; }

private:
    MDB_val _val{0, 0};
};

// wrapper for MDB_val[2], use for MDB_MULTIPLE
template <typename T>
class MultiVal
{
public:
    MultiVal() {}

    MultiVal(const T* array, size_t element_size, size_t element_count)
        : _val{{element_size, const_cast<T*>(array)}, {element_count, 0}}
    {
    }

    MultiVal(const std::vector<T>& vec) : _val{{sizeof(T), const_cast<T*>(vec.data())}, {vec.size(), 0}} {}

    MultiVal(const MultiVal& o) = default;

    operator MDB_val*() { return _val; }

private:
    MDB_val _val[2]{{}, {}};
};

template <typename TKey = void, typename TVal = void>
struct KeyVal
{
    Val<TKey> key;
    Val<TVal> val;
};

class Env
{
public:
    Env() { mdb_env_create(&_env); }
    ~Env() { mdb_env_close(_env); }

    void set_maxdbs(int n) { check(mdb_env_set_maxdbs(_env, n)); }
    void set_mapsize(size_t size) { check(mdb_env_set_mapsize(_env, size)); }
    void open(const std::string& path, unsigned int flags = 0, mdb_mode_t mode = 0644)
    {
        check(mdb_env_open(_env, path.c_str(), flags, mode));
    }

    operator MDB_env*() const { return _env; }

    MDB_env* _env = nullptr;
};

class Dbi
{
public:
    Dbi() {}

    Dbi(MDB_env* env, MDB_txn* txn, const char* name, unsigned int flags = 0) : _env(env), _dbi(-1)
    {
        check(mdb_dbi_open(txn, name, flags, &_dbi));
    }

    ~Dbi()
    {
        if (_dbi != -1) mdb_dbi_close(_env, _dbi);
    }

    Dbi& operator=(Dbi&& o)
    {
        std::swap(_dbi, o._dbi);
        std::swap(_env, o._env);
        return *this;
    }

    Dbi(Dbi&& o)
    {
        std::swap(_dbi, o._dbi);
        std::swap(_env, o._env);
    }

    operator MDB_dbi() const { return _dbi; }

    MDB_dbi _dbi = -1;
    MDB_env* _env = nullptr;
};

class Cursor
{
public:
    Cursor() {}

    Cursor(MDB_txn* txn, MDB_dbi dbi, bool autoclose = false) : _txn(txn), _dbi(dbi), _autoclose(autoclose)
    {
        check(mdb_cursor_open(_txn, _dbi, &_cursor));
    }

    ~Cursor()
    {
        if (_autoclose && _cursor != nullptr) close();
    }

    Cursor(const Cursor& o) = default;

    void close() { mdb_cursor_close(_cursor); }

    void put(MDB_val* key, MDB_val* val, unsigned int flags = 0) { check(mdb_cursor_put(_cursor, key, val, flags)); }

    template <typename TKey, typename TVal>
    void put(const KeyVal<TKey, TVal>& kv, MDB_cursor_op op)
    {
        put(kv.key, kv.val, op);
    }

    void get(MDB_val* key, MDB_val* val, MDB_cursor_op op) { check(mdb_cursor_get(_cursor, key, val, op)); }

    template <typename TKey, typename TVal>
    void get(KeyVal<TKey, TVal>& kv, MDB_cursor_op op)
    {
        get(kv.key, kv.val, op);
    }

    Cursor& operator=(Cursor&& o)
    {
        std::swap(_txn, o._txn);
        std::swap(_dbi, o._dbi);
        std::swap(_cursor, o._cursor);
        return *this;
    }

    Cursor(Cursor&& o)
    {
        std::swap(_txn, o._txn);
        std::swap(_dbi, o._dbi);
        std::swap(_cursor, o._cursor);
    }

    operator MDB_cursor*() const { return _cursor; }

    MDB_txn* _txn = nullptr;
    MDB_dbi _dbi = -1;
    MDB_cursor* _cursor = nullptr;

private:
    bool _autoclose = false;
};

class Txn
{
public:
    Txn() {}

    Txn(MDB_env* env, unsigned int flags = 0, bool autocommit = false) : _env(env), _autocommit(autocommit)
    {
        check(mdb_txn_begin(_env, nullptr, flags, &_txn));
    };

    ~Txn()
    {
        if (_autocommit && _txn != nullptr) commit();
    }

    Txn& operator=(Txn&& o)
    {
        std::swap(_env, o._env);
        std::swap(_txn, o._txn);
        std::swap(_autocommit, o._autocommit);
        return *this;
    }

    Txn(Txn&& o)
    {
        std::swap(_env, o._env);
        std::swap(_txn, o._txn);
        std::swap(_autocommit, o._autocommit);
    }

    Txn(Txn& o)
    {
        std::swap(_env, o._env);
        std::swap(_txn, o._txn);
        std::swap(_autocommit, o._autocommit);
    }

    void commit() { check(mdb_txn_commit(_txn)); }
    void abort() { mdb_txn_abort(_txn); }

    void get(MDB_dbi dbi, MDB_val* key, MDB_val* val) { check(mdb_get(_txn, dbi, key, val)); }

    template <typename TKey, typename TVal>
    void get(MDB_dbi dbi, KeyVal<TKey, TVal>& kv)
    {
        get(dbi, kv.key, kv.val);
    }

    void put(MDB_dbi dbi, MDB_val* key, MDB_val* val, unsigned int flags = 0)
    {
        check(mdb_put(_txn, dbi, key, val, flags));
    }

    template <typename TKey, typename TVal>
    void put(MDB_dbi dbi, KeyVal<TKey, TVal>& kv, unsigned int flags = 0)
    {
        put(dbi, kv.key, kv.val, flags);
    }

    Dbi open_dbi(const char* name, unsigned int flags = 0) { return Dbi(_env, _txn, name, flags); }
    Dbi open_dbi(const std::string& name, unsigned int flags = 0) { return open_dbi(name.c_str(), flags); }
    Dbi open_dbi(unsigned int flags = 0) { return open_dbi(nullptr, flags); }

    Cursor open_cursor(MDB_dbi dbi, bool autoclose = false) { return Cursor(_txn, dbi, autoclose); }

    operator MDB_txn*() const { return _txn; }

    MDB_txn* _txn = nullptr;
    MDB_env* _env = nullptr;

private:
    bool _autocommit = false;
};

// read-only view of a single value for a given key
template <typename T>
class ValueView
{
public:
    ValueView(MDB_env* env, MDB_dbi dbi, MDB_val* key) : _txn{env, MDB_RDONLY, true} { _txn.get(dbi, key, &_val); }

    const T* ptr() { return (T*)_val.mv_data; }

    size_t size() { return _val.mv_size; }

private:
    MDB_val _val{0, 0};
    Txn _txn;
};

// for-each compatible iterate-able thing for MDB_MULTIPLE + MDB_DUPFIXED values
// for a given key
template <typename T>
class MultipleValueView
{
public:
    class Iterator
    {
    public:
        bool operator==(const Iterator& o) { return _end && o._end; }
        bool operator!=(const Iterator& o) { return !(*this == o); }

        Iterator& operator++()
        {
            if (!_multi->next(&_item)) _end = true;
            return *this;
        }

        const T& operator*() { return *_item; }
        const T* operator->() { return _item; }

    private:
        Iterator(MultipleValueView<T>* multi) : _multi(multi) {}
        MultipleValueView* _multi;
        bool _end = false;
        const T* _item = 0;
        friend class MultipleValueView;
    };

    MultipleValueView(MDB_env* env, MDB_dbi dbi, MDB_val* key)
        : _k(*key), _txn{env, MDB_RDONLY, true}, _c{_txn, dbi, true}
    {
    }

    Iterator begin() { return ++Iterator{this}; }

    Iterator end()
    {
        Iterator it{this};
        it._end = true;
        return it;
    }

    bool next(T const** item)
    {
        if (_v.mv_data == nullptr || _v.mv_size == 0)
        {
            int r;
            try
            {
                if (_v.mv_data == nullptr)
                {
                    _c.get(&_k, &_v, MDB_SET);
                    _c.get(&_k, &_v, MDB_GET_MULTIPLE);
                }
                else
                {
                    _c.get(&_k, &_v, MDB_NEXT_MULTIPLE);
                }
            }
            catch (Error& e)
            {
                *item = 0;
                return false;
            }
        }

        *item = (T*)_v.mv_data;
        _v.mv_size -= sizeof(T);
        _v.mv_data = ((T*)_v.mv_data) + 1;

        return true;
    }

private:
    Txn _txn;
    Cursor _c;
    MDB_val _k;
    MDB_val _v{0, nullptr};
};

struct Sentinel
{
};
const Sentinel _sentinel{};

template <typename TItem>
class IteratorBase
{
public:
    bool operator!=(const Sentinel& o) { return !_end; }
    const TItem& operator*() { return _val; }
    const TItem* operator->() { return &_val; }
    IteratorBase& operator++()
    {
        this->next();
        return *this;
    }
    virtual IteratorBase& begin() = 0;
    const Sentinel& end() { return _sentinel; }

protected:
    virtual void next() = 0;
    TItem _val;
    bool _end = false;
};

template <typename TIter>
class IteratorContainer
{
public:
    IteratorContainer(TIter& it) : _it(it) {}
    TIter& begin() { return _it; }
    const Sentinel& end() { return _sentinel; }

private:
    TIter& _it;
};

// iterates over all key/val pairs in a dbi
template <typename TKey, typename TVal>
class SimpleKVIterator : public IteratorBase<KeyVal<TKey, TVal>>
{
public:
    SimpleKVIterator(MDB_env* env, MDB_dbi dbi) : _txn{env, MDB_RDONLY, true}, _c{_txn, dbi, true} {}
    SimpleKVIterator(SimpleKVIterator& o) : _txn{o._txn}, _c{o._c} {}
    SimpleKVIterator& begin() { return *this; }

protected:
    void next() override
    {
        try
        {
            _c.get(this->_val, this->_val.key.data() == nullptr ? MDB_FIRST : MDB_NEXT);
        }
        catch (Error e)
        {
            this->_end = true;
        }
    }

    Txn _txn;
    Cursor _c;
};

template <typename TKey>
class SimpleKeyIterator : public IteratorBase<Val<TKey>>
{
public:
    // TODO: query mdb to figure out if this is dup?
    SimpleKeyIterator(MDB_env* env, MDB_dbi dbi, bool is_dup = false)
        : _txn{env, MDB_RDONLY, true}, _c{_txn, dbi, true}, _is_dup(is_dup)
    {
    }
    SimpleKeyIterator(SimpleKeyIterator& o) : _txn{o._txn}, _c{o._c} { std::swap(_is_dup, o._is_dup); }
    SimpleKeyIterator& begin() { return *this; }

protected:
    void next() override
    {
        try
        {
            if (this->_val.data() == nullptr) { _c.get(this->_val, nullptr, MDB_FIRST); }
            else
            {
                if (_is_dup) _c.get(this->_val, nullptr, MDB_LAST_DUP);
                _c.get(this->_val, nullptr, MDB_NEXT);
            }
        }
        catch (Error& e)
        {
            this->_end = true;
        }
    }
    Txn _txn;
    Cursor _c;
    bool _is_dup = false;
};

}  // namespace lmdbpp

#endif
