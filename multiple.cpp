#include <lmdb.h>
#include <cstdint>
#include <iostream>

int main()
{
    MDB_env* env;
    mdb_env_create(&env);
    mdb_env_set_mapsize(env, 1UL * 1024UL * 1024UL * 1024UL * 1024UL); //1tib
    mdb_env_open(env, "test_multiple.mdb", 0, 0644);

    MDB_txn* txn;
    MDB_dbi dbi;
    auto ret = mdb_txn_begin(env, nullptr, 0, &txn);
    ret = mdb_dbi_open(txn, 0, MDB_INTEGERKEY | MDB_DUPSORT | MDB_DUPFIXED, &dbi);

    MDB_cursor *c;

#define FROM 0
#define TO 1000000
#define NUM (TO - FROM)

    unsigned long long key = 1337ULL;
    unsigned long long dat = 0;
    MDB_val k{sizeof(key), (void*)&key};
    MDB_val v = {sizeof(dat), &dat};
    for (auto i = FROM; i<TO; ++i)
    {
        dat = i;
        ret = mdb_put(txn, dbi, &k, &v, 0/*MDB_APPENDDUP*/); if (ret){std::cout<<"put "<<i<<": "<<mdb_strerror(ret)<<std::endl;}
    }
    ret = mdb_txn_commit(txn); std::cout<<"commit "<<mdb_strerror(ret)<<std::endl;

    MDB_val out{0,0};
    ret = mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn); std::cout<<"txn "<<mdb_strerror(ret)<<std::endl;
    ret = mdb_cursor_open(txn, dbi, &c); std::cout<<"cursor "<<mdb_strerror(ret)<<std::endl;

    ret = mdb_cursor_get(c, &k, &out, MDB_SET); std::cout<<"set "<<mdb_strerror(ret)<<std::endl;
    ret = mdb_cursor_get(c, &k, &out, MDB_GET_MULTIPLE); std::cout<<"get_multiple "<<mdb_strerror(ret)<<std::endl;
    std::cout<<out.mv_size<<" "<<(unsigned long long)out.mv_data<<std::endl;
    ret = mdb_cursor_get(c, &k, &out, MDB_NEXT_MULTIPLE); std::cout<<"next_multiple "<<mdb_strerror(ret)<<std::endl;
    std::cout<<out.mv_size<<" "<<(unsigned long long)out.mv_data<<std::endl;

    mdb_cursor_close(c);




    return 0;
}
