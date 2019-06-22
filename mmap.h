#ifndef __mmap_h
#define __mmap_h

#include <cassert>
#include <fcntl.h>
#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

//cheapo wrapper around a read-only memory map
class Mmap
{
public:
    Mmap(const std::string& file_path)
    {
        struct stat st;
        stat(file_path.c_str(), &st);
        file_size = st.st_size;
        fd = open(file_path.c_str(), O_RDONLY, 0);
        assert(fd != 1);
        map = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
        assert(map != MAP_FAILED);
    }

    ~Mmap()
    {
        int ret = munmap(map, file_size);
        assert(ret == 0);
        close(fd);
    }

    const void* ptr() const
    {
        return (const void*)map;
    }

    std::size_t size() const
    {
        return file_size;
    }

private:
    void* map;
    int fd;
    std::size_t file_size;
};

#endif
