#include <string>
#include <mecab.h>

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

        s.start = s.end == 0 ? 0 : s.end+1;
        ++s.end;
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
