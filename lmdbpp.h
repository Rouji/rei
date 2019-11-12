#ifndef __lmdbpp
#define __lmdbpp

#include <lmdb.h>
#include <stdexcept>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace lmdbpp
{
class Error : public std::runtime_error
{
public:
    const int code;
    Error(int return_code)
        : runtime_error{mdb_strerror(return_code)}
        , code(return_code)
    {
    }
};

// clang-format off
class KeyExistsError : public Error{using Error::Error;};
class NotFoundError : public Error{using Error::Error;};
class PageNotFoundError : public Error{using Error::Error;};
class CorruptedError : public Error{using Error::Error;};
class PanicError : public Error{using Error::Error;};
class VersionMismatchError : public Error{using Error::Error;};
class InvalidError : public Error{using Error::Error;};
class MapFullError : public Error{using Error::Error;};
class DbsFullError : public Error{using Error::Error;};
class ReadersFullError : public Error{using Error::Error;};
class TlsFullError : public Error{using Error::Error;};
class TxnFullError : public Error{using Error::Error;};
class CursorFullError : public Error{using Error::Error;};
class PageFullError : public Error{using Error::Error;};
class MapResizedError : public Error{using Error::Error;};
class IncompatibleError : public Error{using Error::Error;};
class BadRslotError : public Error{using Error::Error;};
class BadTxnError : public Error{using Error::Error;};
class BadValsizeError : public Error{using Error::Error;};
class BadDbiError : public Error{using Error::Error;};
class ProblemError : public Error{using Error::Error;};

const std::unordered_map<int, void (*)(int)> _error_map{
    {MDB_KEYEXIST, [](int code){throw KeyExistsError(code);}},
    {MDB_NOTFOUND, [](int code){throw NotFoundError(code);}},
    {MDB_PAGE_NOTFOUND, [](int code){throw PageNotFoundError(code);}},
    {MDB_CORRUPTED, [](int code){throw CorruptedError(code);}},
    {MDB_PANIC, [](int code){throw PanicError(code);}},
    {MDB_VERSION_MISMATCH, [](int code){throw VersionMismatchError(code);}},
    {MDB_INVALID, [](int code){throw InvalidError(code);}},
    {MDB_MAP_FULL, [](int code){throw MapFullError(code);}},
    {MDB_DBS_FULL, [](int code){throw DbsFullError(code);}},
    {MDB_READERS_FULL, [](int code){throw ReadersFullError(code);}},
    {MDB_TLS_FULL, [](int code){throw TlsFullError(code);}},
    {MDB_TXN_FULL, [](int code){throw TxnFullError(code);}},
    {MDB_CURSOR_FULL, [](int code){throw CursorFullError(code);}},
    {MDB_PAGE_FULL, [](int code){throw PageFullError(code);}},
    {MDB_MAP_RESIZED, [](int code){throw MapResizedError(code);}},
    {MDB_INCOMPATIBLE, [](int code){throw IncompatibleError(code);}},
    {MDB_BAD_RSLOT, [](int code){throw BadRslotError(code);}},
    {MDB_BAD_TXN, [](int code){throw BadTxnError(code);}},
    {MDB_BAD_VALSIZE, [](int code){throw BadValsizeError(code);}},
    {MDB_BAD_DBI, [](int code){throw BadDbiError(code);}}
};
// clang-format on

void check(int return_code)
{
    if (!return_code)
        return;
    auto func = _error_map.find(return_code);
    if (func != _error_map.end())
        func->second(return_code);
    else
        throw Error(return_code);
}

template <typename T = void>
class Val
{
public:
    Val()
    {
    }
    Val(const T* data)
        : _val{sizeof(T), (void*)data}
    {
    }
    Val(const T* data, size_t size)
        : _val{size, (void*)data}
    {
    }
    Val(const std::string& str)
        : _val{str.length(), (void*)str.c_str()}
    {
    }
    Val(const Val& o) = default;

    T* data()
    {
        return (T*)_val.mv_data;
    }
    void data(T* d)
    {
        _val.mv_data = d;
    }
    size_t size()
    {
        return _val.mv_size;
    }
    void size(size_t s)
    {
        _val.mv_size = s;
    }

    std::string to_str() const
    {
        return std::to_string(*(T*)_val.mv_data);
    }

    operator MDB_val*()
    {
        return &_val;
    }

private:
    MDB_val _val{0, 0};
};

template <>
std::string Val<char>::to_str() const
{
    return std::string{(char*)_val.mv_data, _val.mv_size};
}

std::string_view val_to_string_view(Val<char>& val)
{
    return std::string_view{val.data(), val.size()};
}

// wrapper for MDB_val[2], use for MDB_MULTIPLE
template <typename T>
class MultiVal
{
public:
    MultiVal()
    {
    }

    MultiVal(const T* array, size_t element_size, size_t element_count)
        : _val{{element_size, const_cast<T*>(array)}, {element_count, 0}}
    {
    }

    MultiVal(const std::vector<T>& vec)
        : _val{{sizeof(T), const_cast<T*>(vec.data())}, {vec.size(), 0}}
    {
    }

    MultiVal(const MultiVal& o) = default;

    operator MDB_val*()
    {
        return _val;
    }

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
    Env()
    {
        mdb_env_create(&_env);
    }
    ~Env()
    {
        mdb_env_close(_env);
    }

    void set_maxdbs(int n)
    {
        check(mdb_env_set_maxdbs(_env, n));
    }
    void set_mapsize(size_t size)
    {
        check(mdb_env_set_mapsize(_env, size));
    }
    void open(const std::string& path, unsigned int flags = 0, mdb_mode_t mode = 0644)
    {
        check(mdb_env_open(_env, path.c_str(), flags, mode));
    }

    operator MDB_env*() const
    {
        return _env;
    }

    MDB_env* _env = nullptr;
};

class Dbi
{
public:
    Dbi()
    {
    }

    Dbi(MDB_env* env, MDB_txn* txn, const char* name, unsigned int flags = 0)
        : _env(env)
        , _dbi(-1)
    {
        check(mdb_dbi_open(txn, name, flags, &_dbi));
    }

    ~Dbi()
    {
        if (_dbi != -1)
            mdb_dbi_close(_env, _dbi);
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

    operator MDB_dbi() const
    {
        return _dbi;
    }

    static unsigned int flags(MDB_txn* txn, MDB_dbi dbi)
    {
        unsigned int f = 0;
        check(mdb_dbi_flags(txn, dbi, &f));
        return f;
    }

    unsigned int flags(MDB_txn* txn)
    {
        return flags(txn, _dbi);
    }

private:
    MDB_dbi _dbi = -1;
    MDB_env* _env = nullptr;
};

class Cursor
{
public:
    Cursor()
    {
    }

    Cursor(MDB_txn* txn, MDB_dbi dbi, bool autoclose = false)
        : _txn(txn)
        , _dbi(dbi)
        , _autoclose(autoclose)
    {
        check(mdb_cursor_open(_txn, _dbi, &_cursor));
    }

    ~Cursor()
    {
        if (_autoclose && _cursor != nullptr)
            close();
    }

    Cursor(const Cursor& o) = default;

    void close()
    {
        mdb_cursor_close(_cursor);
    }

    void put(MDB_val* key, MDB_val* val, unsigned int flags = 0)
    {
        check(mdb_cursor_put(_cursor, key, val, flags));
    }

    template <typename TKey, typename TVal>
    void put(const KeyVal<TKey, TVal>& kv, MDB_cursor_op op)
    {
        put(kv.key, kv.val, op);
    }

    void get(MDB_val* key, MDB_val* val, MDB_cursor_op op)
    {
        check(mdb_cursor_get(_cursor, key, val, op));
    }
    void get(MDB_val* key, MDB_cursor_op op)
    {
        get(key, nullptr, op);
    }

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

    operator MDB_cursor*() const
    {
        return _cursor;
    }

    MDB_txn* _txn = nullptr;
    MDB_dbi _dbi = -1;
    MDB_cursor* _cursor = nullptr;

private:
    bool _autoclose = false;
};

class Txn
{
public:
    Txn()
    {
    }

    Txn(MDB_env* env, unsigned int flags = 0, bool autocommit = false)
        : _env(env)
        , _autocommit(autocommit)
    {
        check(mdb_txn_begin(_env, nullptr, flags, &_txn));
    };

    ~Txn()
    {
        if (_autocommit && _txn != nullptr)
            commit();
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

    void commit()
    {
        check(mdb_txn_commit(_txn));
    }
    void abort()
    {
        mdb_txn_abort(_txn);
    }

    void get(MDB_dbi dbi, MDB_val* key, MDB_val* val)
    {
        check(mdb_get(_txn, dbi, key, val));
    }
    void get(MDB_dbi dbi, MDB_val* key)
    {
        get(dbi, key, nullptr);
    }

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

    Dbi open_dbi(const char* name, unsigned int flags = 0)
    {
        return Dbi(_env, _txn, name, flags);
    }
    Dbi open_dbi(const std::string& name, unsigned int flags = 0)
    {
        return open_dbi(name.c_str(), flags);
    }
    Dbi open_dbi(unsigned int flags = 0)
    {
        return open_dbi(nullptr, flags);
    }

    Cursor open_cursor(MDB_dbi dbi, bool autoclose = false)
    {
        return Cursor(_txn, dbi, autoclose);
    }

    operator MDB_txn*() const
    {
        return _txn;
    }

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
    ValueView(MDB_env* env, MDB_dbi dbi, MDB_val* key)
        : _txn{env, MDB_RDONLY, true}
    {
        _txn.get(dbi, key, _val);
    }

    ValueView(ValueView& o)
        : _txn{o._txn}
        , _val{o._val}
    {
    }

    const T* ptr()
    {
        return _val.data();
    }

    size_t size()
    {
        return _val.size();
    }

    std::string to_string() const
    {
        return _val.to_str();
    }

private:
    Val<T> _val{0, 0};
    Txn _txn;
};

}  // namespace lmdbpp

#endif
