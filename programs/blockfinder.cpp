/**
base64 /dev/urandom | head -c $(( 16 * 1024 * 1024 )) > base64-16MiB
gzip -k base64-16MiB
g++ -mssse3 --std=c++17 -Wall -O3 -DNDEBUG -o blockfinder -I . -I common programs/blockfinder.cpp && ./blockfinder base64-16MiB.gz

Debugging:
g++ -mssse3 -g --std=c++17 -Wall -O3 -DNDEBUG -o blockfinder -I . -I common programs/blockfinder.cpp && gdb -ex r -ex bt --args ./blockfinder base64-16MiB.gz
*/

#include "../lib/gzip_decompress.hpp" //FIXME

#include "prog_util.h"

#include <chrono>
#include <optional>
#include <type_traits>

#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "prog_util.cpp"


[[nodiscard]] std::chrono::time_point<std::chrono::high_resolution_clock>
now() noexcept
{
    return std::chrono::high_resolution_clock::now();
}


/**
 * @return duration in seconds
 */
template<typename T>
[[nodiscard]] double
duration( const T& t0,
          const T& t1 = now() ) noexcept
{
    return std::chrono::duration<double>( t1 - t0 ).count();
}


static int
stat_file(struct file_stream* in, stat_t* stbuf, bool allow_hard_links)
{
    if (tfstat(in->fd, stbuf) != 0) {
        msg("%" TS ": unable to stat file", in->name);
        return -1;
    }

    if (!S_ISREG(stbuf->st_mode) && !in->is_standard_stream) {
        msg("%" TS " is %s -- skipping", in->name, S_ISDIR(stbuf->st_mode) ? "a directory" : "not a regular file");
        return -2;
    }

    if (stbuf->st_nlink > 1 && !allow_hard_links) {
        msg("%" TS " has multiple hard links -- skipping "
            "(use -f to process anyway)",
            in->name);
        return -2;
    }

    return 0;
}


static int
benchmarkBlockFinder(const tchar* path, bool warmUp = false)
{
    struct file_stream in;
    auto ret = xopen_for_read(path, true, &in);
    if (ret != 0) return ret;

    stat_t stbuf;
    ret = stat_file(&in, &stbuf, true);
    if (ret != 0) return ret;

    ret = map_file_contents(&in, size_t(stbuf.st_size));
    if (ret != 0) {
        xclose(&in);
        return ret;
    }

    auto in_p = static_cast<const byte*>(in.mmap_mem);
    OutputConsumer output{};
    ConsumerWrapper consumerWrapper{ output };

    std::vector<size_t> blockOffsets;

    InputStream in_stream( in_p, in.mmap_size );
    DeflateThreadRandomAccess deflate_thread{ in_stream, consumerWrapper };

    const auto t0 = now();
    std::optional<std::decay_t<decltype( t0 )> > t2;
    std::optional<std::decay_t<decltype( t0 )> > t3;
    for ( size_t bitPosition = 0; bitPosition < in.mmap_size * 8; ++bitPosition ) {
        const auto newBitPosition = deflate_thread.sync( bitPosition );
        if ( bitPosition == newBitPosition ) {
            break;
        }

        if ( !t3 ) {
            if ( bitPosition > 1000 ) {
                /* Set time first more distant block which should be at offset ~27 KiB. */
                t3 = now();
                std::cerr << "Found second block at offset: ~" << ( bitPosition / 8 ) << " B\n";
            } else {
                t2 = now();
            }
        }
        bitPosition = newBitPosition;
        blockOffsets.push_back( bitPosition );
    }
    const auto t1 = now();

    if ( warmUp ) {
        return 0;
    }

    /**
     * @verbatim
     * Found second block at offset: ~25614 B
     * Found 496 blocks in 1.90395 s (6.69634 MB/s).
     * Latency to find second block from first one: 3.77238 ms
     * @endverbatim
     * 496 blocks in 13 MiB of compressed and 16 MiB of uncompressed data corresponds to ~26.8 KiB blocks.
     * Note that they specify 100-300ms seek time. Assuming 32 KiB blocks, this would translate to ~400-1200 KB/s.
     * I cannot explain the discrepancy to these valued compared to the measured ones.
     * Maybe they optimized it further after publication.
     */
    const auto bandwidth = stbuf.st_size / duration( t0, t1 ) / 1e6;
    std::cerr << "Found " << blockOffsets.size() << " blocks in " << duration( t0, t1 )
              << " s (" << bandwidth << " MB/s).\n";
    if ( t2 && t3 ) {
        std::cerr << "Latency to find second block from first one: " << duration( t2.value(), t3.value() ) * 1000
                  << " ms\n";
    }

    xclose(&in);
    return 0;
}


int
tmain(int argc, tchar* argv[])
{
    if ( argc < 2 ) {
        std::cerr << "Please specify an input gzip file to find the blocks of\n";
        return 1;
    }

    if ( argc >= 3 ) {
        benchmarkBlockFinder( argv[1], /* warm up */ true );
        for ( int i = 0; i < std::stoi( argv[2] ); ++i ) {
            benchmarkBlockFinder( argv[1] );
        }
        return 0;
    }
    return benchmarkBlockFinder( argv[1] );
}
