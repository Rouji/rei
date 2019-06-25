#include "mmap.h"
#include "mecabparser.h"
#include <lmdb.h>
#include <cstdint>
#include <cstring>
#include <unordered_map>
#include <memory>
#include <sstream>
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
        uint32_t parts[2];
    };

    LmdbFullText(std::string& db_path, const std::unordered_set<std::string>& stopwords = default_stopwords) : stopwords(stopwords)
    {
        mdb_env_create(&env);
        mdb_env_set_maxdbs(env, 3);
        mdb_env_set_mapsize(env, 1UL * 1024UL * 1024UL * 1024UL * 1024UL); //1tib
        mdb_env_open(env, db_path.c_str(), 0, 0644);

        /* create DBIs if they don't exist
         * NOTE: this is necessary because dbi_open can't create
         * DBIs on first open when your transaction is MDB_RDONLY
         */
        MDB_txn* txn;
        mdb_txn_begin(env, nullptr, 0, &txn);
        dbi_open(txn, "word_idx");
        dbi_open(txn, "document_content");
        dbi_open(txn, "document_name");
        mdb_txn_commit(txn);
    }

    ~LmdbFullText()
    {
        close_all_dbi();
        mdb_env_close(env);
    }

    bool add_document(const std::string& name, const void* ptr, std::size_t size)
    {
        MDB_val k{0, 0}, v{0, 0};
        MDB_txn* txn;


        auto name_hash = strhash(name);

        //see if it's already in the db
        //abort if it is
        mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);
        auto dbi_doc_name = dbi_open(txn, "document_name");
        k.mv_data = &name_hash;
        k.mv_size = sizeof(name_hash);
        auto ret = mdb_get(txn, dbi_doc_name, &k, &v);
        mdb_txn_commit(txn);
        if (ret == 0) // already exists
        {
            std::cout << "document already in db\n";
            return false; //TODO: add error codes?
        }

        mdb_txn_begin(env, nullptr, 0, &txn);
        v.mv_data = (void*)name.c_str();
        v.mv_size = name.size();
        mdb_put(txn, dbi_doc_name, &k, &v, 0);

        auto dbi_doc_cont = dbi_open(txn, "document_content");
        v.mv_data = (void*)ptr;
        v.mv_size = size;
        mdb_put(txn, dbi_doc_cont, &k, &v, 0);
        mdb_txn_commit(txn);


        MecabParser parser{(const char*)ptr, size};

        std::unordered_map<std::string, std::vector<WordIdx>> locations{};

        mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);
        auto dbi_words = dbi_open(txn, "word_idx");
        for (MecabParser::Node n; parser.next(n);)
        {
            //skip stopwords
            if (stopwords.find(n.base) != stopwords.end())
                continue;

            //load indices for the word from db if they're not already in the map
            auto it = locations.find(n.base);
            if (it == locations.end())
            {
                std::tie(it, std::ignore) = locations.insert(
                                                std::pair<std::string, std::vector<WordIdx>>(
                                                    n.base,
                                                    std::vector<WordIdx> {}
                                                )
                                            );
                k.mv_data = (void*)n.base.c_str();
                k.mv_size = n.base.length();
                auto ret = mdb_get(txn, dbi_words, &k, &v);
                if (ret == 0)
                {
                    auto num = v.mv_size / sizeof(WordIdx);
                    it->second.insert(it->second.end(), (WordIdx*)v.mv_data, ((WordIdx*)v.mv_data) + num);
                }
            }
            WordIdx idx;
            idx.parts[0] = name_hash;
            idx.parts[1] = n.location;
            it->second.push_back(idx);
        }
        mdb_txn_commit(txn);

        mdb_txn_begin(env, nullptr, 0, &txn);
        for (auto loc : locations)
        {
            k.mv_size = loc.first.length();
            k.mv_data = (void*)loc.first.c_str();
            v.mv_size = loc.second.size() * sizeof(WordIdx);
            v.mv_data = loc.second.data();
            mdb_put(txn, dbi_words, &k, &v, 0);
        }
        mdb_txn_commit(txn);

        return true;
    }

    bool add_document(const std::string& name, const std::string& file_path)
    {
        Mmap mmap{file_path};
        return add_document(name, mmap.ptr(), mmap.size());
    }

    bool find_word(const std::string& word, std::vector<WordIdx>* idx = nullptr)
    {
        MDB_val k{word.length(), (void*)word.c_str()}, v;
        AutoCommit ac{};
        mdb_txn_begin(env, nullptr, MDB_RDONLY, &ac.txn);
        auto dbi_words = dbi_open(ac.txn, "word_idx");
        if (mdb_get(ac.txn, dbi_words, &k, &v) == MDB_NOTFOUND)
            return false;
        if (idx)
        {
            auto num = v.mv_size / sizeof(WordIdx);
            idx->insert(idx->end(), (WordIdx*)v.mv_data, ((WordIdx*)v.mv_data) + num);
        }
        return true;
    }

    const std::string& document_name(uint32_t hash)
    {
        auto it = doc_name_cache.find(hash);
        if (it != doc_name_cache.end())
            return it->second;

        MDB_txn* txn;
        MDB_val k{sizeof(hash), &hash}, v;
        auto ret = mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);
        auto dbi = dbi_open(txn, "document_name");
        mdb_get(txn, dbi, &k, &v);
        it = doc_name_cache.insert(
                 std::pair<uint32_t, std::string>(
                     hash,
                     std::string{(char*)v.mv_data, v.mv_size}
                 )
             ).first;
        mdb_txn_commit(txn);
        return it->second;
    }


private:
    MDB_dbi dbi_open(MDB_txn *txn, const std::string& name)
    {
        auto pair = dbi_cache.find(name);
        if (pair != dbi_cache.end())
            return pair->second;
        MDB_dbi dbi;
        mdb_dbi_open(txn, name.c_str(), MDB_CREATE, &dbi);
        dbi_cache[name] = dbi;
        return dbi;
    }

    uint32_t strhash(const std::string& str) const
    {
        std::hash<std::string> hash_fn;
        auto h = hash_fn(str);
        return (uint32_t)h;
    }

    void close_all_dbi()
    {
        for (auto pair : dbi_cache)
            mdb_dbi_close(env, pair.second);
    }

    class AutoCommit
    {
    public:
        AutoCommit(MDB_txn* mdb_txn = nullptr) : txn(mdb_txn) {}
        ~AutoCommit()
        {
            mdb_txn_commit(txn);
        }
        MDB_txn *txn;
    };

    MDB_env* env;
    std::unordered_map<std::string, MDB_dbi> dbi_cache; //MDB_dbi is just an unsigned int
    std::unordered_map<uint32_t, std::string> doc_name_cache;
    const std::unordered_set<std::string> &stopwords;
};
const std::unordered_set<std::string> LmdbFullText::default_stopwords = { "。", "？", "?", "、" };

int main(int argc, char **argv)
{
    if (argc != 3)
    {
        std::cout << "usage: " << argv[0] << " <file> <name>\n";
        return 1;
    }
    std::string input_file{argv[1]};
    std::string name{argv[2]};
    std::string db = "test.mdb";
    LmdbFullText lft{db};
    lft.add_document(name, input_file);
    /*
    std::vector<LmdbFullText::WordIdx> idx{};
    lft.find_word(name, &idx);
    for (auto i : idx)
        std::printf("doc %u(%s) at %d\n", i.parts[0], lft.document_name(i.parts[0]).c_str(), i.parts[1]);
        */

    return 0;
}
