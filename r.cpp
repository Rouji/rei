#include "lmdbfulltext.h"
#include <string>

int main(int argc, char **argv)
{
    ++argv;
    std::string noun{*(argv++)};
    std::string verb{*(argv++)};

    std::string db = "test.mdb";
    LmdbFullText lft{db};

    if (noun == "doc")
    {
        std::string name{*(argv++)};
        if (verb == "add")
        {
            std::string input_file{*(argv++)};
            lft.add_document(name, input_file);
        }
        else if (verb == "print")
        {
        }
    }
    else if (noun == "word")
    {
        if (verb == "indexes")
        {
            std::string word{*(argv++)};
            auto it = lft.word_iterator(word);
            for (const LmdbFullText::WordIdx * w; it.next(&w);)
            {
                std::cout << w->n << '\n';
            }
        }
        else if (verb == "occurrences")
        {
            std::string word{*(argv++)};
            std::cout << lft.occurrence_count(word) << std::endl;
        }
    }
    /*
     * list documents
     * print document
     * tokenize: print whatever the tokeniser (mecab) makes of a string
     * idx context: print text around a wordidx
     * examples: iterate through all occurences of a word and print their surroundings
     */

    return 0;
}
