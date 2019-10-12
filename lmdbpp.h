#ifndef __lmdbpp
#define __lmdbpp

#include <lmdb.h>
#include <utility>

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
    if (return_code)
        throw Error{mdb_strerror(return_code), return_code};
}

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

    MDB_env* _env = 0;
};

class Dbi
{
public:
    Dbi() : _env(0), _dbi(-1)
    {}

    Dbi(MDB_env* env, MDB_txn* txn, const char* name, unsigned int flags = 0) : _env(env), _dbi(-1)
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

    MDB_dbi _dbi;
    MDB_env* _env;
};

class Cursor
{
public:
    Cursor() : _txn(nullptr), _dbi(-1), _cursor(nullptr), _autoclose(false) {}

    Cursor(MDB_txn* txn, MDB_dbi dbi, bool autoclose = false) : _txn(txn), _dbi(dbi), _autoclose(autoclose)
    {
        check(mdb_cursor_open(_txn, _dbi, &_cursor));
    }

    ~Cursor()
    {
        if (_autoclose)
            close();
    }

    void close()
    {
        mdb_cursor_close(_cursor);
    }

    void put(MDB_val* key, MDB_val* val, unsigned int flags = 0)
    {
        check(mdb_cursor_put(_cursor, key, val, flags));
    }

    void get(MDB_val* key, MDB_val* val, MDB_cursor_op op)
    {
        check(mdb_cursor_get(_cursor, key, val, op));
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

    MDB_txn* _txn;
    MDB_dbi _dbi;
    MDB_cursor* _cursor;

private:
    bool _autoclose;
};

class Txn
{
public:
    Txn() : _env(nullptr), _txn(nullptr) {}

    Txn(MDB_env* env, unsigned int flags = 0, bool autocommit = false) : _env(env), _autocommit(autocommit)
    {
        check(mdb_txn_begin(_env, nullptr, flags, &_txn));
    };

    ~Txn()
    {
        if (_autocommit)
            commit();
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

    void put(MDB_dbi dbi, MDB_val* key, MDB_val* val, unsigned int flags = 0)
    {
        check(mdb_put(_txn, dbi, key, val, flags));
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

    MDB_txn* _txn;
    MDB_env* _env;

private:
    bool _autocommit;
};

//read-only view of a value for a given key
template <typename T>
class ValueView
{
public:
    ValueView(MDB_env* env, MDB_dbi dbi, MDB_val* key) : _txn{env, MDB_RDONLY, true}
    {
        _txn.get(dbi, key, &_val);
    }

    const T* ptr()
    {
        return (T*)_val.mv_data;
    }

    size_t size()
    {
        return _val.mv_size;
    }

private:
    MDB_val _val{0,0};
    Txn _txn;
};

//for-each compatible iterate-able thing for MDB_MULTIPLE values for a given key
template <typename T>
class MultipleValueView
{
public:
    class Iterator
    {
    public:
        bool operator==(const Iterator& o)
        {
            return _end && o._end;
        }
        bool operator!=(const Iterator& o)
        {
            return !(*this == o);
        }

        Iterator& operator++()
        {
            if (!_multi->next(&_item))
                _end = true;
            return *this;
        }

        const T& operator*()
        {
            return *_item;
        }

        const T* operator->()
        {
            return _item;
        }

    private:
        Iterator(MultipleValueView<T>* multi) : _multi(multi){}
        MultipleValueView* _multi;
        bool _end = false;
        const T* _item = 0;
        friend class MultipleValueView;
    };

    MultipleValueView(MDB_env* env, MDB_dbi dbi, MDB_val key) : _k(key), _txn{env, MDB_RDONLY, true}, _c{_txn, dbi, true} {}


    Iterator begin()
    {
        return ++Iterator{this};
    }

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
            catch(Error& e)
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


}

#endif
