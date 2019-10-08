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
        uint32_t parts[2]; //{doc idx, word location}
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
        dbi_open(txn, "word_idx", /*MDB_INTEGERKEY |*/ MDB_DUPFIXED | MDB_DUPSORT);
        dbi_open(txn, "document_content");
        dbi_open(txn, "document_info");
        mdb_txn_commit(txn);
    }

    ~LmdbFullText()
    {
        close_all_dbi();
        mdb_env_close(env);
    }

    /* //TODO: use MDB_MULTIPLE and MDB_APPENDDUP to append to the location values
     * in DB instead of loading all of them and overwriting
     */
    bool add_document(const std::string& name, const void* ptr, std::size_t size)
    {
        MDB_val k{0, 0}, v{0, 0};
        MDB_txn* txn;


        auto name_hash = strhash(name);

        //see if it's already in the db
        //abort if it is
        mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);
        auto dbi_doc_name = dbi_open(txn, "document_info");
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

        std::unordered_map<std::string, std::vector<WordIdx>> word_locations{};

        mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);
        //auto dbi_words = dbi_open(txn, "word_idx");
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
        mdb_txn_commit(txn);

        MDB_cursor* c;
        mdb_txn_begin(env, nullptr, 0, &txn);
        auto dbi_words = dbi_open(txn, "word_idx");
        mdb_cursor_open(txn, dbi_words, &c);
        MDB_val vv[2];
        for (const auto& wloc: word_locations)
        {
            k.mv_data = const_cast<char*>(wloc.first.c_str());
            k.mv_size = wloc.first.length();
            vv[0].mv_size = sizeof(WordIdx);
            vv[0].mv_data = const_cast<WordIdx*>(wloc.second.data());
            vv[1].mv_size = wloc.second.size();
            auto r = mdb_cursor_put(c, &k, vv, MDB_MULTIPLE);
            if (r) std::cout<<r<<" 1\n";
        }
        mdb_cursor_close(c);
        mdb_txn_commit(txn);

        return true;
    }

    template <typename T>
    class LmdbGetIterator
    {
    public:
        LmdbGetIterator(MDB_txn* txn, MDB_dbi dbi, MDB_val key) : _txn(txn), _k(key)
        {
            mdb_cursor_open(_txn, dbi, &_c);
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
                    mdb_cursor_get(_c, &_k, &_v, MDB_SET);
                    r = mdb_cursor_get(_c, &_k, &_v, MDB_GET_MULTIPLE);
                }
                else
                {
                    r = mdb_cursor_get(_c, &_k, &_v,MDB_NEXT_MULTIPLE);
                }
                if (r)
                {
                    *item = 0;
                    return false;
                }
            }

            *item = (T*)_v.mv_data;
            _v.mv_size -= sizeof(T);
            _v.mv_data = ((T*)_v.mv_data)+1;

            return true;
        }

    private:
        MDB_txn* _txn;
        MDB_cursor* _c;
        MDB_val _k;
        MDB_val _v{0,nullptr};
    };

    bool add_document(const std::string& name, const std::string& file_path)
    {
        Mmap mmap{file_path};
        return add_document(name, mmap.ptr(), mmap.size());
    }

    LmdbGetIterator<WordIdx> word_iterator(const std::string& word)
    {
        MDB_txn* txn;
        mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);
        MDB_val k{word.length(), const_cast<char*>(word.c_str())};
        return LmdbGetIterator<WordIdx>(txn, dbi_open(txn, "word_idx"), k);
    }

    const std::string& document_info(uint32_t hash)
    {
        auto it = doc_name_cache.find(hash);
        if (it != doc_name_cache.end())
            return it->second;

        MDB_txn* txn;
        MDB_val k{sizeof(hash), &hash}, v;
        auto ret = mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);
        auto dbi = dbi_open(txn, "document_info");
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
    MDB_dbi dbi_open(MDB_txn *txn, const std::string& name, unsigned int flags = 0)
    {
        auto pair = dbi_cache.find(name);
        if (pair != dbi_cache.end())
            return pair->second;
        MDB_dbi dbi;
        mdb_dbi_open(txn, name.c_str(), MDB_CREATE | flags, &dbi);
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

    auto it = lft.word_iterator("何");
    for (const LmdbFullText::WordIdx* w; it.next(&w);)
    {
        std::cout<<w->n<<'\n';
    }

    return 0;
}
