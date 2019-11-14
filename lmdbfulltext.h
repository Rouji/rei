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
#include "lmdbpp_containers.h"
#include "mecab_tagger.h"
#include "mmap.h"

using namespace lmdbpp;
using tagger::MecabParser;

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

        {
            Txn txn{_env, 0, true};
            _dbi_document_content = txn.open_dbi("document_content", MDB_CREATE);
            _dbi_document_info = txn.open_dbi("document_info", MDB_CREATE);
            _dbi_word_idx = txn.open_dbi("word_idx", MDB_CREATE | MDB_DUPFIXED | MDB_DUPSORT);
        }
    }

    ~LmdbFullText()
    {
    }

    bool add_document(const std::string& name, const void* ptr, std::size_t size)
    {
        auto name_hash = strhash(name);

        // write document info and content
        {
            KeyVal<decltype(name_hash), char> kv{{&name_hash, sizeof(name_hash)}, {name}};

            Txn txn{_env, 0, true};
            try
            {
                txn.put(_dbi_document_info, kv, MDB_NOOVERWRITE);
            }
            catch (KeyExistsError& e)
            {
                std::cout << "document " << name << " already exists" << std::endl;  // TODO
                return false;
            }

            txn.put(_dbi_document_content, kv.key, Val<void>{ptr, size});
        }

        MecabParser parser{(const char*)ptr, size};

        std::unordered_map<std::string, std::vector<WordIdx>> word_locations{};

        MDB_txn* txn;
        WordIdx idx;
        for (tagger::Node n; parser.next(n);)
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
            Txn txn{_env, 0, true};
            Cursor c{txn, _dbi_word_idx, true};
            Val<char> k;
            MultiVal<WordIdx> v;
            for (const auto& wloc : word_locations)
            {
                k = Val<char>{wloc.first};
                v = MultiVal<WordIdx>{wloc.second};
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

    auto word_indices(const std::string& word)
    {
        return MultipleValueIteratable<char, WordIdx>{_env, _dbi_word_idx, Val<char>{word}};
    }

    ValueView<char> view_document(const std::string& name)
    {
        auto hash = strhash(name.c_str());
        return ValueView<char>{_env, _dbi_document_content, Val<decltype(hash)>{&hash}};
    }

    size_t word_occurrence_count(const std::string& word)
    {
        size_t count = 0;
        KeyVal<char, WordIdx> kv{{word}, {}};
        Txn txn{_env, MDB_RDONLY, true};
        Cursor c{txn, _dbi_word_idx, true};
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
        catch (NotFoundError& e)
        {
        }
        return count;
    }

    auto word_list()
    {
        return KeyIteratable<char>{_env, _dbi_word_idx};
    }

    auto document_list()
    {
        return KeyValIteratable<uint32_t, char>{_env, _dbi_document_info};
    }

    void test()
    {
        KeyValIteratable<WordIdx, char> kv{_env, _dbi_document_info};
        for (auto& n : kv)
        {
            std::cout << n.val.size() << "\n";
        }
        /*
        {
            auto && __range = kv;
            auto __end = kv.end();
            for (auto __begin = kv.begin(); __begin != __end; ++__begin)
            {
                auto& n = *kv;
                std::cout<<std::string{n.val.data(), n.val.size()}<<" "<<*(uint64_t*)n.key.data()<<"\n";
            }
        }
        */
    }

    std::string document_info(uint32_t hash)
    {
        Txn txn{_env, MDB_RDONLY, true};
        KeyVal<decltype(hash), char> kv{{&hash, sizeof(hash)}, {}};
        txn.get(_dbi_document_info, kv);
        return kv.val.to_str();
    }

private:
    uint32_t strhash(const std::string& str) const
    {
        std::hash<std::string> hash_fn;
        auto h = hash_fn(str);
        return (uint32_t)h;
    }

    Env _env;
    Dbi _dbi_word_idx;
    Dbi _dbi_document_info;
    Dbi _dbi_document_content;
};

#endif
