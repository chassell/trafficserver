
#include "ts/StdAllocWrapper.h"

#include "ts/ink_queue.h"
#include "ts/ink_defs.h"
#include "ts/ink_resource.h"
#include "ts/ink_align.h"
#include "ts/ink_memory.h"

void AlignedAllocator::re_init(const char *name, unsigned int element_size, unsigned int chunk_size, unsigned int alignment, int advice) 
{
    _name = name; 
    _sz = aligned_spacing(element_size, std::max(sizeof(uint64_t),alignment+0UL) ); // increase to aligned size

    if ( advice == MADV_DONTDUMP ) {
      _arena = jemallctl::proc_arena_nodump;
    } else if ( advice == MADV_NORMAL ) {
      _arena = jemallctl::proc_arena;
    } else {
      ink_abort("allocator re_init: unknown madvise() flags: %x",advice);
    }

    void *preCached[chunk_size];

    for ( int n = chunk_size ; n-- ; ) {
      preCached[n] = mallocx(_sz, (MALLOCX_ALIGN(_sz)|MALLOCX_ARENA(_arena)) );
    }
    for ( int n = chunk_size ; n-- ; ) {
      deallocate( preCached[n] );
    }
}

AlignedAllocator::AlignedAllocator(const char *name, unsigned int element_size)
     : _name(name), _sz( aligned_spacing(element_size,sizeof(uint64_t)) )
{
  // don't pre-allocate before main() is called
}
