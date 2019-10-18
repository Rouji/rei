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

}
#endif
