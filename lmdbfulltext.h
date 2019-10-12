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
        _env.set_maxdbs(3);
        _env.set_mapsize(1UL * 1024UL * 1024UL * 1024UL * 1024UL); //1tib
        _env.open(db_path);

        lmdbpp::Txn txn(_env, 0, true);
        _dbi_document_content = txn.open_dbi("document_content", MDB_CREATE);
        _dbi_document_info = txn.open_dbi("document_info", MDB_CREATE);
        _dbi_word_idx = txn.open_dbi("word_idx", MDB_CREATE | MDB_DUPFIXED | MDB_DUPSORT);
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

            lmdbpp::Txn txn{_env, 0, true};
            v.mv_data = (void*)name.c_str();
            v.mv_size = name.size();
            try
            {
                txn.put(_dbi_document_info, &k, &v, MDB_NOOVERWRITE);
            }
            catch (lmdbpp::Error& e)
            {
                if (e.code == MDB_KEYEXIST)
                {
                    std::cout<<"document "<<name<<" already exists"<<std::endl; //TODO
                    return false;
                }
                else
                {
                    throw;
                }
            }

            v.mv_data = (void*)ptr;
            v.mv_size = size;
            txn.put(_dbi_document_content, &k, &v);
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
            lmdbpp::Txn txn{_env, 0, true};
            lmdbpp::Cursor c{txn, _dbi_word_idx, true};
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

    bool add_document(const std::string& name, const std::string& file_path)
    {
        Mmap mmap{file_path};
        return add_document(name, mmap.ptr(), mmap.size());
    }

    lmdbpp::MultipleValueView<WordIdx> word_indices(const std::string& word)
    {
        MDB_val k{word.length(), const_cast<char*>(word.c_str())};
        return lmdbpp::MultipleValueView<WordIdx>{_env, _dbi_word_idx, k};
    }

    lmdbpp::ValueView<char> view_document(const std::string& name)
    {
        auto hash = strhash(name.c_str());
        MDB_val k{sizeof(hash), &hash};
        return lmdbpp::ValueView<char>{_env, _dbi_document_content, &k};
    }

    size_t word_occurrence_count(const std::string& word)
    {
        size_t count = 0;
        MDB_val k{word.length(), const_cast<char*>(word.c_str())};
        MDB_val v{0, nullptr};

        lmdbpp::Txn txn{_env, MDB_RDONLY, true};
        lmdbpp::Cursor c{txn, _dbi_word_idx, true};
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
        auto ret = mdb_txn_begin(_env, nullptr, MDB_RDONLY, &txn);
        mdb_get(txn, _dbi_document_info, &k, &v);
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

    lmdbpp::Env _env;
    lmdbpp::Dbi _dbi_word_idx;
    lmdbpp::Dbi _dbi_document_info;
    lmdbpp::Dbi _dbi_document_content;
    const std::unordered_set<std::string> &stopwords;
};
const std::unordered_set<std::string> LmdbFullText::default_stopwords = { "。", "？", "?", "、" };

#endif
