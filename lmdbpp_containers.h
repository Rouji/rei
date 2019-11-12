#ifndef __lmdbpp_iterators
#define __lmdbpp_iterators

#include "lmdbpp.h"

namespace lmdbpp
{

struct IteratorSentinel
{
};
const IteratorSentinel _sentinel{};

template <typename TData>
class IteratorBase
{
public:
    IteratorBase(IteratorBase& o)
        : _data(o._data)
        , _end(o._end)
    {
    }
    bool operator!=(const IteratorSentinel& o)
    {
        return !_end;
    }
    TData& operator*()
    {
        return _data;
    }
    TData* operator->()
    {
        return &_data;
    }
    IteratorBase& operator++()
    {
        this->next();
        return *this;
    }

protected:
    IteratorBase()
    {
    }
    virtual void next() = 0;

    TData _data;
    bool _end = false;
};

class IteratableBase
{
public:
    const IteratorSentinel& end()
    {
        return _sentinel;
    }
};

// iterates over all key/val pairs in a dbi
template <typename TKey, typename TVal>
class KeyValIteratable : public IteratableBase
{
public:
    class Iterator : public IteratorBase<KeyVal<TKey, TVal>>
    {
    protected:
        void next() override
        {
            try
            {
                _c.get(this->_data, this->_data.key.data() == nullptr ? MDB_FIRST : MDB_NEXT);
            }
            catch (NotFoundError& e)
            {
                this->_end = true;
            }
        }

    private:
        friend class KeyValIteratable;
        Iterator(Txn& txn, MDB_dbi dbi)
            : _c{txn, dbi, true}
        {
            next();
        }
        Iterator(Iterator& o)
            : IteratorBase<KeyVal<TKey, TVal>>(o)
            , _c{o._c}
        {
        }
        Cursor _c;
    };

    KeyValIteratable(MDB_env* env, MDB_dbi dbi)
        : _txn{env, MDB_RDONLY}
        , _dbi(dbi)
    {
    }
    KeyValIteratable(KeyValIteratable& o)
        : _txn{o._txn}
    {
    }

    Iterator begin()
    {
        return Iterator{_txn, _dbi};
    }

protected:
    Txn _txn;
    MDB_dbi _dbi;
};

template <typename TKey>
class KeyIteratable : public IteratableBase
{
public:
    class Iterator : public IteratorBase<Val<TKey>>
    {
    protected:
        void next() override
        {
            try
            {
                if (this->_data.data() == nullptr)
                {
                    _c.get(this->_data, _unused_val, MDB_FIRST);
                }
                else
                {
                    if (_is_dup)
                        _c.get(this->_data, _unused_val, MDB_LAST_DUP);
                    _c.get(this->_data, _unused_val, MDB_NEXT);
                }
            }
            catch (NotFoundError& e)
            {
                this->_end = true;
            }
        }

    private:
        friend class KeyIteratable;
        Iterator(Txn& txn, MDB_dbi dbi)
            : _c{txn, dbi, true}
            , _is_dup(Dbi::flags(txn, dbi) & MDB_DUPSORT)
        {
            next();
        }
        Iterator(Iterator& o)
            : IteratorBase<Val<TKey>>(o)
            , _is_dup(o._is_dup)
            , _c{o._c}
        {
        }
        Val<> _unused_val{};
        Cursor _c;
        bool _is_dup = false;
    };

    KeyIteratable(MDB_env* env, MDB_dbi dbi)
        : _txn{env, MDB_RDONLY, true}
        , _dbi(dbi)
    {
    }
    KeyIteratable(KeyIteratable& o)
        : _txn{o._txn}
        , _dbi(o._dbi)
    {
    }

    Iterator begin()
    {
        return Iterator{_txn, _dbi};
    }

protected:
    Txn _txn;
    MDB_dbi _dbi;
};

// pretty much for MDB_MULTIPLE + MDB_DUPFIXED values *only*
template <typename TKey, typename TVal>
class MultipleValueIteratable : public IteratableBase
{
public:
    class Iterator : public IteratorBase<TVal>
    {
    protected:
        void next() override
        {
            if (_kv.val.data() == nullptr || _kv.val.size() == 0)
            {
                try
                {
                    if (_kv.val.data() == nullptr)
                    {
                        _c.get(_kv, MDB_SET);
                        _c.get(_kv, MDB_GET_MULTIPLE);
                    }
                    else
                    {
                        _c.get(_kv, MDB_NEXT_MULTIPLE);
                    }
                }
                catch (NotFoundError& e)
                {
                    this->_end = true;
                }
            }

            this->_data = *_kv.val.data();
            _kv.val.size(_kv.val.size() - sizeof(TVal));
            _kv.val.data(_kv.val.data() + 1);
        }

    private:
        friend class MultipleValueIteratable;
        Iterator(Txn& txn, MDB_dbi dbi, const Val<TKey>& key)
            : _kv{key, {}}
            , _c{txn, dbi, true}
        {
            next();
        }
        Iterator(Iterator& o)
            : IteratorBase<Val<TVal>>(o)
            , _c{o._c}
        {
        }

        Cursor _c;
        KeyVal<TKey, TVal> _kv{};
    };

    MultipleValueIteratable(MDB_env* env, MDB_dbi dbi, const Val<TKey>& key)
        : _txn{env, MDB_RDONLY, true}
        , _dbi(dbi)
        , _key{key}
    {
    }
    MultipleValueIteratable(MultipleValueIteratable& o)
        : _txn{o._txn}
        , _dbi(o._dbi)
        , _key(o._key)
    {
    }

    Iterator begin()
    {
        return Iterator{_txn, _dbi, _key};
    }

protected:
    Txn _txn;
    MDB_dbi _dbi;
    Val<TKey> _key;
};

/*
template <typename TKey, typename TVal>
class Map
{
public:
    class Proxy
    {
    public:
        void operator=(Val<TVal> val)
        {
            _kv.val = val;
            _txn.put(_dbi, _kv);
        }

        operator TVal() const
        {
            return _kv.val;
        }

    private:
        friend class Map;
        Proxy(Txn& txn, MDB_dbi dbi, const Val<TKey>& key)
            : _txn(txn)
            , _dbi(dbi)
            , _kv{key, {}}
        {
            try
            {
                _txn.get(dbi, _kv);
            }
            catch (Error& e)
            {
                if (e.code == MDB_NOTFOUND)
                {
                    _txn.put(dbi, _kv);
                }
            }
        }

        Txn& _txn;
        KeyVal<TKey, TVal> _kv;
        MDB_dbi _dbi = -1;
    };

    Map(MDB_env* env, MDB_dbi dbi, unsigned int flags = 0)
        : _dbi(dbi)
        , _txn{env, flags, true}
    {
    }

    Proxy operator[](const Val<TKey>& key)
    {
        return Proxy{_txn, _dbi, key};
    }
    Proxy operator[](const TKey& key)
    {
        return Proxy{_txn, _dbi, key};
    }

private:
    MDB_dbi _dbi;
    Txn _txn;
};
*/

}  // namespace lmdbpp
#endif
