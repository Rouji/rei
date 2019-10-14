#include "lmdbfulltext.h"
#include <string>
#include <string_view>

int main(int argc, char **argv)
{
    std::vector<std::string> args{argv, argv+argc};
    if (args.size() < 4)
    {
        std::cerr<<"usage: "<<args[0]<<" <db> <noun> <verb> [options]"<<std::endl;
        return 1;
    }

    auto arg = args.begin();
    std::string& db = *(++arg);
    std::string& noun = *(++arg);
    std::string& verb = *(++arg);

    LmdbFullText lft{db};

    if (noun == "doc")
    {
        const std::string& name{*(++arg)};
        if (verb == "add")
        {
            std::string& input_file{*(++arg)};
            lft.add_document(name, input_file);
        }
        else if (verb == "print")
        {
            auto view = lft.view_document(name);
            std::cout<<std::string_view{view.ptr(), view.size()} <<std::endl;
        }
    }
    else if (noun == "word")
    {
        if (verb == "indices")
        {
            std::string& word{*(++arg)};
            auto it = lft.word_indices(word);
            /*
            for (const LmdbFullText::WordIdx * w; it.next(&w);)
            {
                std::cout << w->parts[0] << " | " << w->parts[1] << '\n';
            }
            */
            for (const auto& i : it)
            {
                std::cout << i.n << '\n';
            }
        }
        else if (verb == "count")
        {
            std::string& word{*(++arg)};
            std::cout << lft.word_occurrence_count(word) << std::endl;
        }
        else if (verb == "list")
        {
            for (const auto& w : lft.word_list())
            {
                std::cout<<w<<"\n";
            }
        }
    }
    /*
     * //TODO:
     * list documents
     * tokenize: print whatever the tokeniser (mecab) makes of a string
     * idx context: print text around a wordidx
     * examples: iterate through all occurences of a word and print their surroundings
     */

    return 0;
}
