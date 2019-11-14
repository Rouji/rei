#ifndef __tokeniser_h
#define __tokeniser_h

#include <string>

namespace tagging
{

struct Node
{
    uint32_t location;   // offset in bytes inside the document
    std::string word;    // word as encountered in document
    std::string feature; // original feature string
    std::string base;    // base form of word
    std::string reading; // reading in kana
};

class Tagger
{
public:
    virtual bool next(Node& out) = 0;
};
}
#endif
