#ifndef __lmdbfulltext_h
#define __lmdbfulltext_h

#include "mmap.h"
#include "mecabparser.h"
#include <lmdb.h>
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
        mdb_env_create(&env);
        mdb_env_set_maxdbs(env, 3);
        mdb_env_set_mapsize(env, 1UL * 1024UL * 1024UL * 1024UL * 1024UL); //1tib
        mdb_env_open(env, db_path.c_str(), 0, 0644);

        MDB_txn* txn;
        mdb_txn_begin(env, nullptr, 0, &txn);
        mdb_dbi_open(txn, "word_idx", /*MDB_INTEGERKEY |*/ MDB_CREATE | MDB_DUPFIXED | MDB_DUPSORT, &dbi_word_idx);
        mdb_dbi_open(txn, "document_content", MDB_CREATE, &dbi_document_content);
        mdb_dbi_open(txn, "document_info", MDB_CREATE, &dbi_document_info);
        mdb_txn_commit(txn);
    }

    ~LmdbFullText()
    {
        mdb_dbi_close(env, dbi_word_idx);
        mdb_dbi_close(env, dbi_document_info);
        mdb_dbi_close(env, dbi_document_content);
        mdb_env_close(env);
    }

    bool add_document(const std::string& name, const void* ptr, std::size_t size)
    {
        MDB_val k{0, 0}, v{0, 0};
        MDB_txn* txn;
        int r;

        auto name_hash = strhash(name);

        //see if it's already in the db
        //abort if it is
        mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);
        k.mv_data = &name_hash;
        k.mv_size = sizeof(name_hash);
        r = mdb_get(txn, dbi_document_info, &k, &v);
        mdb_txn_commit(txn);
        if (r == 0) // already exists
        {
            std::cout << "document already in db\n";
            return false; //TODO: add error codes?
        }

        r = mdb_txn_begin(env, nullptr, 0, &txn);
        v.mv_data = (void*)name.c_str();
        v.mv_size = name.size();
        r = mdb_put(txn, dbi_document_info, &k, &v, 0);

        v.mv_data = (void*)ptr;
        v.mv_size = size;
        r = mdb_put(txn, dbi_document_content, &k, &v, 0);
        r = mdb_txn_commit(txn);


        MecabParser parser{(const char*)ptr, size};

        std::unordered_map<std::string, std::vector<WordIdx>> word_locations{};

        r = mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);
        for (MecabParser::Node n; parser.next(n);)
        {
            //skip stopwords
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
        r = mdb_txn_commit(txn);

        MDB_cursor* c;
        r = mdb_txn_begin(env, nullptr, 0, &txn);
        r = mdb_cursor_open(txn, dbi_word_idx, &c);
        MDB_val vv[2];
        for (const auto& wloc : word_locations)
        {
            std::cout<<wloc.first<<'\n';
            k.mv_data = const_cast<char*>(wloc.first.c_str());
            k.mv_size = wloc.first.length();
            vv[0].mv_size = sizeof(WordIdx);
            vv[0].mv_data = const_cast<WordIdx*>(wloc.second.data());
            vv[1].mv_size = wloc.second.size();
            r = mdb_cursor_put(c, &k, vv, MDB_MULTIPLE);
            if (r) std::cout << r << "\n";
        }
        mdb_cursor_close(c);
        r = mdb_txn_commit(txn);

        return true;
    }

    template <typename T>
    class LmdbGetIterator
    {
    public:
        LmdbGetIterator(MDB_txn* txn, MDB_dbi dbi, MDB_val key) : _txn(txn), _k(key)
        {
            auto r = mdb_cursor_open(_txn, dbi, &_c);
        }

        ~LmdbGetIterator()
        {
            mdb_cursor_close(_c);
            mdb_txn_commit(_txn);
        }

        bool next(T const** item)
        {
            if (_v.mv_data == nullptr || _v.mv_size == 0)
            {
                int r;
                if (_v.mv_data == nullptr)
                {
                    r = mdb_cursor_get(_c, &_k, &_v, MDB_SET); //TODO: handle MDB_NOTFOUND
                    r = mdb_cursor_get(_c, &_k, &_v, MDB_GET_MULTIPLE);
                }
                else
                {
                    r = mdb_cursor_get(_c, &_k, &_v, MDB_NEXT_MULTIPLE);
                }
                if (r)
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
        MDB_txn* _txn;
        MDB_cursor* _c;
        MDB_val _k;
        MDB_val _v{0, nullptr};
    };

    bool add_document(const std::string& name, const std::string& file_path)
    {
        Mmap mmap{file_path};
        return add_document(name, mmap.ptr(), mmap.size());
    }

    LmdbGetIterator<WordIdx> word_iterator(const std::string& word)
    {
        MDB_txn* txn;
        auto r = mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);
        MDB_val k{word.length(), const_cast<char*>(word.c_str())};
        return LmdbGetIterator<WordIdx>(txn, dbi_word_idx, k);
    }

    size_t occurrence_count(const std::string& word)
    {
        size_t count = 0;
        MDB_val k{word.length(), const_cast<char*>(word.c_str())};
        MDB_val v{0, nullptr};
        MDB_txn* txn;
        MDB_cursor* c;

        mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);
        mdb_cursor_open(txn, dbi_word_idx, &c);
        auto r = mdb_cursor_get(c, &k, &v, MDB_SET);
        mdb_cursor_get(c, &k, &v, MDB_GET_MULTIPLE);
        do
        {
            count += v.mv_size / sizeof(WordIdx);
        }
        while (mdb_cursor_get(c, &k, &v, MDB_NEXT_MULTIPLE) == 0);
        mdb_cursor_close(c);
        mdb_txn_commit(txn);
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

    MDB_env* env;
    MDB_dbi dbi_word_idx;
    MDB_dbi dbi_document_info;
    MDB_dbi dbi_document_content;
    const std::unordered_set<std::string> &stopwords;
};
const std::unordered_set<std::string> LmdbFullText::default_stopwords = { "。", "？", "?", "、" };

#endif
