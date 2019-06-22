#include <lmdb.h>
#include "mmap.h"
#include <cstdint>
#include <cstring>
#include <unordered_map>
#include <mecab.h>
#include <memory>
#include <sstream>
#include <string>
#include <vector>
#include <unordered_set>


class MecabParser
{
public:
    struct Node
    {
        uint32_t location;   // offset in bytes inside the document
        std::string word;    // word as encountered in document
        std::string base;    // base form of word
        std::string reading; // reading in kana
    };

    MecabParser(char const *input, std::size_t size)
        : input(input), tagger{0}, size(size), mc_node(nullptr)
    {
        tagger = MeCab::createTagger("");
        if (!tagger) throw std::runtime_error{"couldn't create MeCab tagger"};
    }

    bool next(Node &out_node)
    {
        if (!mc_node || !(mc_node = mc_node->next) || mc_node->stat == MECAB_EOS_NODE)
        {
            if (!next_span(span)) return false;

            mc_node = mecab_parse_to_node(span);

            if (mc_node->stat == MECAB_BOS_NODE) mc_node = mc_node->next; //skip beginning of string node
        }

        out_node.location = mc_node->surface - input;
        out_node.word = std::string{mc_node->surface, mc_node->length};
        //std::cout<<"feature:"<<out_node.word<<"->"<<mc_node->feature<<std::endl;
        parse_mecab_feature(mc_node, out_node);

        return true;
    }

    ~MecabParser()
    {
        if (tagger) delete tagger;
    }

private:
    class Span
    {
        public:
            Span(std::size_t s, std::size_t e) : start(s), end(e) {}
            Span() : start(0), end(0) {}
            std::size_t length() const {return end-start;}

        std::size_t start;
        std::size_t end;
    }; // indices into the input data

    MeCab::Tagger *tagger;
    char const *input;
    std::size_t size;
    Span span;
    const MeCab::Node *mc_node;

    bool next_span(Span &s) const
    {
        if (s.end >= size) // we're at the end
            return false;

        ++s.end;
        if (s.start > 0)
            s.start = s.end;
        for (;s.end < size && input[s.end] != '\n'; ++s.end)
            ;

        return true;
    }

    const MeCab::Node* mecab_parse_to_node(const Span& s) const
    {
        return tagger->parseToNode(&input[span.start], span.length());
    }

    void parse_mecab_feature(const MeCab::Node *n, Node &out) const
    {
        int len = 0;
        int i = 0;
        for (auto p = n->feature;; ++p)
        {
            if (*p == ',' || *p == '\0')
            {
                switch (i)
                {
                case 6:
                    out.base = std::string{p - len + 1, (size_t)len - 1};
                    break;
                case 7:
                    out.reading = std::string{p - len + 1, (size_t)len - 1};
                    return;
                }
                len = 0;
                ++i;
            }
            ++len;
        }
    }
};


class LmdbFullText
{
public:
    static const std::unordered_set<std::string> default_stopwords;

    LmdbFullText(std::string& db_path, const std::unordered_set<std::string>& stopwords = default_stopwords) : stopwords(stopwords)
    {
        mdb_env_create(&env);
        mdb_env_set_maxdbs(env, 3);
        mdb_env_set_mapsize(env, 1UL * 1024UL * 1024UL * 1024UL); //1gib
        mdb_env_open(env, db_path.c_str(), 0, 0644);

        //create DBIs if they don't exist
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
        MecabParser parser{(const char*)ptr, size};

        std::unordered_map<std::string, std::vector<WordIdx>> locations{};

        MDB_val k{0,0}, v{0,0};
        MDB_txn* txn;
        mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);
        MDB_dbi dbi_words = dbi_open(txn, "word_idx");
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
                        std::vector<WordIdx>{}
                    )
                );
                //std::cout<<"not in locations:"<<n.base<<std::endl;
                k.mv_data = (void*)n.base.c_str();
                k.mv_size = n.base.length();
                auto ret = mdb_get(txn, dbi_words, &k, &v);
                if (ret ==0)
                {
                    auto num = v.mv_size/sizeof(WordIdx);
                    //std::cout<<"reading "<< num <<" indices from db for:"<<n.base<<std::endl;
                    it->second.insert(it->second.end(), (WordIdx*)v.mv_data, ((WordIdx*)v.mv_data)+num);
                }
            }
            WordIdx idx;
            idx.parts[0] = 0; //TODO: document idx
            idx.parts[1] = n.location;
            it->second.push_back(idx);
            //std::cout<<"pushed into locations:"<<n.base<<"("<<n.word<<")"<<std::endl;
        }
        mdb_txn_commit(txn);

        mdb_txn_begin(env, nullptr, 0, &txn);
        for (auto loc : locations)
        {
            k.mv_size = loc.first.length();
            k.mv_data = (void*)loc.first.c_str();
            v.mv_size = loc.second.size()*sizeof(WordIdx);
            v.mv_data = loc.second.data();
            //std::cout<<"putting "<< loc.second.size() << " indices into db for:"<<loc.first<<std::endl;
            mdb_put(txn, dbi_words, &k, &v, 0);
        }
        mdb_txn_commit(txn);

        mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);
        MDB_cursor* c{};
        mdb_cursor_open(txn, dbi_words, &c);
        while(!mdb_cursor_get(c, &k, &v, MDB_NEXT))
        {
            std::cout << std::string{(char*)k.mv_data, k.mv_size} << " " << v.mv_size/sizeof(WordIdx) << std::endl;
        }
        mdb_cursor_close(c);

        return true;
    }

    bool add_document(const std::string& name, const std::string& file_path)
    {
        Mmap mmap{file_path};
        return add_document(name, mmap.ptr(), mmap.size());
    }



private:
    union WordIdx
    {
        uint64_t n;
        uint32_t parts[2];
    };

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

    void close_all_dbi()
    {
        for (auto pair : dbi_cache)
            mdb_dbi_close(env, pair.second);
    }

    MDB_env* env;
    std::unordered_map<std::string, MDB_dbi> dbi_cache; //MDB_dbi is just an unsigned int
    const std::unordered_set<std::string> &stopwords;
};
const std::unordered_set<std::string> LmdbFullText::default_stopwords = { "。", "？", "?", "、" };

int main(int argc, char **argv)
{
    std::string input_file{argv[1]};
    std::string db = "test.mdb";
    LmdbFullText lft{db};
    lft.add_document(input_file, input_file);

    return 0;
}
