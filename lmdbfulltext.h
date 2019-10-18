#ifndef __lmdbfulltext_h
#define __lmdbfulltext_h

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "lmdbpp.h"
#include "mecabparser.h"
#include "mmap.h"

class LmdbFullText
{
public:
    union WordIdx {
        uint64_t n;
        uint32_t parts[2];  //{doc idx, word location}
    };

    LmdbFullText(std::string& db_path)
    {
        _env.set_maxdbs(3);
        _env.set_mapsize(1UL * 1024UL * 1024UL * 1024UL * 1024UL);  // 1tib
        _env.open(db_path);

        lmdbpp::Txn txn(_env, 0, true);
        _dbi_document_content = txn.open_dbi("document_content", MDB_CREATE);
        _dbi_document_info = txn.open_dbi("document_info", MDB_CREATE);
        _dbi_word_idx = txn.open_dbi("word_idx", MDB_CREATE | MDB_DUPFIXED | MDB_DUPSORT);
    }

    ~LmdbFullText() {}

    bool add_document(const std::string& name, const void* ptr, std::size_t size)
    {
        auto name_hash = strhash(name);

        // write document info and content
        {
            lmdbpp::KeyVal<decltype(name_hash), char> kv{{&name_hash, sizeof(name_hash)}, {name}};

            lmdbpp::Txn txn{_env, 0, true};
            try
            {
                txn.put(_dbi_document_info, kv, MDB_NOOVERWRITE);
            }
            catch (lmdbpp::Error& e)
            {
                if (e.code == MDB_KEYEXIST)
                {
                    std::cout << "document " << name << " already exists" << std::endl;  // TODO
                    return false;
                }
                else
                {
                    throw;
                }
            }

            txn.put(_dbi_document_content, kv.key, lmdbpp::Val<void>{ptr, size});
        }

        MecabParser parser{(const char*)ptr, size};

        std::unordered_map<std::string, std::vector<WordIdx>> word_locations{};

        MDB_txn* txn;
        WordIdx idx;
        for (MecabParser::Node n; parser.next(n);)
        {
            auto it = word_locations.find(n.base);
            if (it == word_locations.end())
            {
                std::tie(it, std::ignore) =
                    word_locations.insert(std::pair<std::string, std::vector<WordIdx>>(n.base, std::vector<WordIdx>{}));
            }
            idx.parts[0] = name_hash;
            idx.parts[1] = n.location;
            it->second.push_back(idx);
        }

        {
            lmdbpp::Txn txn{_env, 0, true};
            lmdbpp::Cursor c{txn, _dbi_word_idx, true};
            lmdbpp::Val<char> k;
            lmdbpp::MultiVal<WordIdx> v;
            for (const auto& wloc : word_locations)
            {
                k = lmdbpp::Val<char>{wloc.first};
                v = lmdbpp::MultiVal<WordIdx>{wloc.second};
                c.put(k, v, MDB_MULTIPLE);
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
        return lmdbpp::MultipleValueView<WordIdx>{_env, _dbi_word_idx, lmdbpp::Val<char>{word}};
    }

    lmdbpp::ValueView<char> view_document(const std::string& name)
    {
        auto hash = strhash(name.c_str());
        return lmdbpp::ValueView<char>{_env, _dbi_document_content, lmdbpp::Val<decltype(hash)>{&hash}};
    }

    size_t word_occurrence_count(const std::string& word)
    {
        size_t count = 0;
        lmdbpp::KeyVal<char, WordIdx> kv{{word}, {}};
        lmdbpp::Txn txn{_env, MDB_RDONLY, true};
        lmdbpp::Cursor c{txn, _dbi_word_idx, true};
        try
        {
            c.get(kv, MDB_SET);
            c.get(kv, MDB_GET_MULTIPLE);
            while (true)
            {
                count += kv.val.size() / sizeof(WordIdx);
                c.get(kv, MDB_NEXT_MULTIPLE);
            }
        }
        catch (lmdbpp::Error& e)
        {
            if (e.code != MDB_NOTFOUND) throw;
        }
        return count;
    }

    // TODO: make this something iterable that doesn't load all words into memory
    auto word_list() { return lmdbpp::SimpleKeyIterator<char>{_env, _dbi_word_idx, true}; }

    std::string document_info(uint32_t hash)
    {
        lmdbpp::Txn txn{_env, MDB_RDONLY, true};
        lmdbpp::KeyVal<decltype(hash), char> kv{{&hash, sizeof(hash)}, {}};
        txn.get(_dbi_document_info, kv);
        return kv.val.as_str();
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
};

#endif
