#ifndef __lmdbfulltext_h
#define __lmdbfulltext_h

#include "mmap.h"
#include "mecabparser.h"
#include "lmdbpp.h"
#include <cstdint>
#include <cstring>
#include <unordered_map>
#include <memory>
#include <string>
#include <vector>
#include <unordered_set>

class LmdbFullText
{
public:
    static const std::unordered_set<std::string> default_stopwords;

    union WordIdx
    {
        uint64_t n;
        uint32_t parts[2]; //{doc idx, word location}
    };

    LmdbFullText(std::string& db_path, const std::unordered_set<std::string>& stopwords = default_stopwords) : stopwords(stopwords)
    {
        env.set_maxdbs(3);
        env.set_mapsize(1UL * 1024UL * 1024UL * 1024UL * 1024UL); //1tib
        env.open(db_path);

        lmdbpp::Txn txn(env, 0, true);
        dbi_document_content = txn.open_dbi("document_content", MDB_CREATE);
        dbi_document_info = txn.open_dbi("document_info", MDB_CREATE);
        dbi_word_idx = txn.open_dbi("word_idx", MDB_CREATE | MDB_DUPFIXED | MDB_DUPSORT);
    }

    ~LmdbFullText()
    {}

    bool add_document(const std::string& name, const void* ptr, std::size_t size)
    {
        MDB_val k{0, 0}, v{0, 0};

        auto name_hash = strhash(name);

        //write document info and content
        {
            k.mv_data = &name_hash;
            k.mv_size = sizeof(name_hash);

            lmdbpp::Txn txn{env, 0, true};
            v.mv_data = (void*)name.c_str();
            v.mv_size = name.size();
            try
            {
                txn.put(dbi_document_info, &k, &v, MDB_NOOVERWRITE);
            }
            catch (lmdbpp::Error& e)
            {
                if (e.code == MDB_KEYEXIST)
                {
                    std::cout<<"document "<<name<<" already exists"<<std::endl; //TODO
                    return false;
                }
            }

            v.mv_data = (void*)ptr;
            v.mv_size = size;
            txn.put(dbi_document_content, &k, &v);
        }


        MecabParser parser{(const char*)ptr, size};

        std::unordered_map<std::string, std::vector<WordIdx>> word_locations{};

        MDB_txn* txn;
        for (MecabParser::Node n; parser.next(n);)
        {
            //skip stopwords
            //TODO move this to the mecabparser
            if (stopwords.find(n.base) != stopwords.end())
                continue;

            auto it = word_locations.find(n.base);
            if (it == word_locations.end())
            {
                std::tie(it, std::ignore) = word_locations.insert(
                                                std::pair<std::string, std::vector<WordIdx>>(
                                                    n.base,
                                                    std::vector<WordIdx> {}
                                                )
                                            );
            }
            WordIdx idx;
            idx.parts[0] = name_hash;
            idx.parts[1] = n.location;
            it->second.push_back(idx);
        }

        {
            lmdbpp::Txn txn{env, 0, true};
            lmdbpp::Cursor c{txn, dbi_word_idx, true};
            MDB_val vv[2];
            for (const auto& wloc : word_locations)
            {
                k.mv_data = const_cast<char*>(wloc.first.c_str());
                k.mv_size = wloc.first.length();
                vv[0].mv_size = sizeof(WordIdx);
                vv[0].mv_data = const_cast<WordIdx*>(wloc.second.data());
                vv[1].mv_size = wloc.second.size();
                c.put(&k, vv, MDB_MULTIPLE);
            }
        }

        return true;
    }

    template <typename T>
    class LmdbMultipleIterator
    {
    public:
        LmdbMultipleIterator(MDB_env* env, MDB_dbi dbi, MDB_val key) : _k(key), _txn{env, MDB_RDONLY, true}, _c{_txn, dbi, true}
        {}

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
                catch(lmdbpp::Error& e)
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
        lmdbpp::Txn _txn;
        lmdbpp::Cursor _c;
        MDB_val _k;
        MDB_val _v{0, nullptr};
    };

    template <typename T>
    class LmdbValueView
    {
    public:
        LmdbValueView(MDB_env* env, MDB_dbi dbi, MDB_val* key) : _txn{env, MDB_RDONLY, true}
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
        lmdbpp::Txn _txn;
    };

    bool add_document(const std::string& name, const std::string& file_path)
    {
        Mmap mmap{file_path};
        return add_document(name, mmap.ptr(), mmap.size());
    }

    LmdbMultipleIterator<WordIdx> word_iterator(const std::string& word)
    {
        MDB_val k{word.length(), const_cast<char*>(word.c_str())};
        return LmdbMultipleIterator<WordIdx>{env, dbi_word_idx, k};
    }

    LmdbValueView<char> view_document(const std::string& name)
    {
        auto hash = strhash(name.c_str());
        MDB_val k{sizeof(hash), &hash};
        return LmdbValueView<char>{env, dbi_document_content, &k};
    }

    size_t occurrence_count(const std::string& word)
    {
        size_t count = 0;
        MDB_val k{word.length(), const_cast<char*>(word.c_str())};
        MDB_val v{0, nullptr};

        lmdbpp::Txn txn{env, MDB_RDONLY, true};
        lmdbpp::Cursor c{txn, dbi_word_idx, true};
        try
        {
            c.get(&k, &v, MDB_SET);
            c.get(&k, &v, MDB_GET_MULTIPLE);
            while (true)
            {
                count += v.mv_size / sizeof(WordIdx);
                c.get(&k, &v, MDB_NEXT_MULTIPLE);
            }
        }
        catch (lmdbpp::Error& e)
        {
            if (e.code != MDB_NOTFOUND)
                throw;
        }
        return count;
    }

    std::string document_info(uint32_t hash)
    {
        MDB_txn* txn;
        MDB_val k{sizeof(hash), &hash}, v;
        auto ret = mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);
        mdb_get(txn, dbi_document_info, &k, &v);
        std::string name{(char*)v.mv_data, v.mv_size};
        mdb_txn_commit(txn);
        return name;
    }


private:
    uint32_t strhash(const std::string& str) const
    {
        std::hash<std::string> hash_fn;
        auto h = hash_fn(str);
        return (uint32_t)h;
    }

    lmdbpp::Env env;
    lmdbpp::Dbi dbi_word_idx;
    lmdbpp::Dbi dbi_document_info;
    lmdbpp::Dbi dbi_document_content;
    const std::unordered_set<std::string> &stopwords;
};
const std::unordered_set<std::string> LmdbFullText::default_stopwords = { "。", "？", "?", "、" };

#endif
