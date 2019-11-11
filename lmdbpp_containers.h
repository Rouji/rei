#ifndef __lmdbpp_iterators
#define __lmdbpp_iterators

#include "lmdbpp.h"

namespace lmdbpp
{
// for-each compatible iterate-able thing for MDB_MULTIPLE + MDB_DUPFIXED values
// for a given key
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
        Iterator(MultipleValueView<T>* multi)
            : _multi(multi)
        {
        }
        MultipleValueView* _multi;
        bool _end = false;
        const T* _item = 0;
        friend class MultipleValueView;
    };

    MultipleValueView(MDB_env* env, MDB_dbi dbi, MDB_val* key)
        : _k(*key)
        , _txn{env, MDB_RDONLY, true}
        , _c{_txn, dbi, true}
    {
    }

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
            catch (Error& e)
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
            catch (Error& e)
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
