
#include <iostream>
#include <vector>
#include <tuple>
#include <stdexcept>
#include <sstream>
#include <bitset>
#include <limits>

#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <math.h>

const size_t MB = 1024*1024;
const size_t GB = 1024*1024*1024;

const size_t ZEF_PAGE_SIZE = 4*MB;
// const size_t ZEF_UID_SHIFT = 32*GB;
const size_t ZEF_UID_SHIFT = 1*GB;
// const size_t ZEF_UID_SHIFT = 16*MB;
const size_t MAX_MMAP_SIZE = 3*ZEF_UID_SHIFT;

const unsigned int PAGE_BITMAP_BITS = ZEF_UID_SHIFT / ZEF_PAGE_SIZE;

// * Utils

void * FloorPtr(void * ptr, size_t mod) {
    return (char*)ptr - (size_t)ptr % mod;
}
void * CeilPtr(void * ptr, size_t mod) {
    void * temp = FloorPtr(ptr, mod);
    if(temp != ptr)
        temp = (char*)temp + mod;
    return temp;
}

void * AlignToSystemPageSize(void * ptr) {
    int page_size = getpagesize();
    return FloorPtr(ptr, page_size);
}

void error(const char * s) {
    std::cerr << "Error: " << s << std::endl;
    exit(1);
}
void error2(const char * s) {
    perror(s);
    exit(1);
}

size_t SizeFromFD(int fd) {
    struct stat buf;
    if(0 != fstat(fd, &buf))
        error2("Could not fstat fd.");
    return buf.st_size;
}

bool IsFDWritable(int fd) {
    int flags;
    flags = fcntl(fd, F_GETFL);
    // The options O_WRONLY or O_RDWR do not share bits, so this is technically
    // not correct, however in our case we never have a O_WRONLY scenario.
    return (flags & O_RDWR);
}

// * Basic struct manipulation

constexpr int MMAP_STYLE_MALLOC = 1;
constexpr int MMAP_STYLE_ANONYMOUS = 2;
constexpr int MMAP_STYLE_FILE_BACKED = 3;
// constexpr int MMAP_STYLE_SHARED = 4;
struct MMAP_ALLOC_INFO {
    void * location;
    // void * blob_ptr;
    int blob_fd;
    int uid_fd;
    int style;
    // These pages will be from the blob_ptr
    std::bitset<PAGE_BITMAP_BITS> occupied_pages;
    // MMAP_ALLOC_INFO(void * location, void * blob_ptr, int blob_fd, int uid_fd, int style) :
    MMAP_ALLOC_INFO(void * location, int blob_fd, int uid_fd, int style) :
        location(location),
        // blob_ptr(blob_ptr),
        blob_fd(blob_fd),
        uid_fd(uid_fd),
        style(style),
        occupied_pages(0)
    {}
};

void * BlobsPtrFromMMap(void * ptr) {
    // Need to leave room for the info struct
    void * new_ptr = CeilPtr((char*)ptr + sizeof(MMAP_ALLOC_INFO), ZEF_UID_SHIFT);

    assert((size_t)new_ptr % ZEF_UID_SHIFT == 0);
    assert(new_ptr > ptr);
    return new_ptr;
}
void * BlobsPtrFromBlob(void * ptr) { return FloorPtr(ptr, ZEF_UID_SHIFT); }
void * UIDFromBlob(void * ptr) { return (char*)(ptr) + ZEF_UID_SHIFT; }

MMAP_ALLOC_INFO& InfoFromBlobsPtr(void * ptr) {
    ptr = (char*)ptr - sizeof(MMAP_ALLOC_INFO);
    return *(MMAP_ALLOC_INFO*)ptr;
}
void * BlobsPtrFromInfo(MMAP_ALLOC_INFO * ptr) { return (char*)ptr + sizeof(MMAP_ALLOC_INFO); }
MMAP_ALLOC_INFO& InfoFromBlob(void * ptr) { return InfoFromBlobsPtr(BlobsPtrFromBlob(ptr)); }



bool CheckPage(MMAP_ALLOC_INFO & info, size_t page_ind) {
    if (page_ind < 0 || page_ind >= PAGE_BITMAP_BITS)
        error("Accessing page out of range");
    return info.occupied_pages[page_ind];
}
bool CheckRange(void * target_ptr, size_t size) {
    void * blob_ptr = BlobsPtrFromBlob(target_ptr);
    MMAP_ALLOC_INFO& info = InfoFromBlobsPtr(blob_ptr);
    // Note: need (size-1) here.
    size_t page_ind_low = ((char*)target_ptr-(char*)blob_ptr) / ZEF_PAGE_SIZE;
    size_t page_ind_high = ((char*)target_ptr+(size-1)-(char*)blob_ptr) / ZEF_PAGE_SIZE;
    for (auto page_ind = page_ind_low ; page_ind <= page_ind_high ; page_ind++) {
        if (!CheckPage(info, page_ind))
            return false;
    }
    return true;
}

bool CheckPage(void * ptr) {
    MMAP_ALLOC_INFO& info = InfoFromBlob(ptr);
    size_t page_ind = ((size_t)ptr % ZEF_UID_SHIFT) / ZEF_PAGE_SIZE;
    return CheckPage(info, page_ind);
}

void EnsurePage(MMAP_ALLOC_INFO & info, size_t page_ind) {
    if (CheckPage(info, page_ind))
        return;
    if(info.style == MMAP_STYLE_MALLOC) {
        std::cerr << "Can't extend malloc pages" << std::endl;
        exit(1);
    }

    void * blobs_ptr = BlobsPtrFromInfo(&info);

    void * blob_start = (char*)blobs_ptr + page_ind*ZEF_PAGE_SIZE;
    void * uid_start = UIDFromBlob(blob_start);

    if (info.style == MMAP_STYLE_ANONYMOUS) {
        // TODO: Disable write on read-only graph
        if(0 != mprotect(blob_start, ZEF_PAGE_SIZE, PROT_READ | PROT_WRITE))
            error2("Could not mprotect new blobs page");
        if(0 != mprotect(uid_start, ZEF_PAGE_SIZE, PROT_READ | PROT_WRITE))
            error2("Could not mprotect new uid page");
        memset(blob_start, 0, ZEF_PAGE_SIZE);
        memset(uid_start, 0, ZEF_PAGE_SIZE);
    }
    else if (info.style == MMAP_STYLE_FILE_BACKED) {
        size_t required_size = (page_ind+1)*ZEF_PAGE_SIZE;
        if (SizeFromFD(info.blob_fd) < required_size) {
            // Note: ftruncate is meant to fill with zeros
            ftruncate(info.blob_fd, required_size);
            ftruncate(info.uid_fd, required_size);
        }
        int prot = PROT_READ;
        if (IsFDWritable(info.blob_fd))
            prot |= PROT_WRITE;
        int flags = MAP_SHARED | MAP_FIXED;
        size_t offset = page_ind*ZEF_PAGE_SIZE;

        if(MAP_FAILED == mmap(blob_start, ZEF_PAGE_SIZE, prot, flags, info.blob_fd, offset))
            error2("Unable to map new page from blob fd.");
        if(MAP_FAILED == mmap(uid_start, ZEF_PAGE_SIZE, prot, flags, info.uid_fd, offset))
            error2("Unable to map new page from uid fd.");
    }

    info.occupied_pages[page_ind] = 1;
}
void EnsureRange(void * ptr, size_t size) {
    void * blob_ptr = BlobsPtrFromBlob(ptr);
    MMAP_ALLOC_INFO& info = InfoFromBlobsPtr(blob_ptr);
    // TODO: See if this can be optimised by converting to a bitshift. I suspect it would be compile-time optimised anyway though
    size_t page_ind_low = ((char*)ptr-(char*)blob_ptr) / ZEF_PAGE_SIZE;
    size_t page_ind_high = ((char*)ptr+size-(char*)blob_ptr) / ZEF_PAGE_SIZE;
    for (auto page_ind = page_ind_low ; page_ind <= page_ind_high ; page_ind++) {
        EnsurePage(info, page_ind);
    }
}


// * Alloc list functions

std::vector<MMAP_ALLOC_INFO*> alloc_list;

MMAP_ALLOC_INFO& CreateMmap(int style=MMAP_STYLE_ANONYMOUS, int blob_fd=-1, int uid_fd=-1) {
    void * location;
    if (style == MMAP_STYLE_MALLOC) {
        location = malloc(MAX_MMAP_SIZE);
        memset(location, 0, MAX_MMAP_SIZE);
        std::cerr << "Malloc of size " << MAX_MMAP_SIZE << std::endl;
    } else {
        // This should create a mmap which doesn't take any physical memory.
        // This reserves the space, so that we can fill it with other maps later on.
        int opts = MAP_ANONYMOUS | MAP_PRIVATE;
        int fd = -1;
        int prot = PROT_NONE;
        location = mmap(NULL, MAX_MMAP_SIZE, prot, opts, fd, 0);
        if(location == MAP_FAILED)
            error2("Could not mmap memory");

        std::cerr << "MMap of size " << MAX_MMAP_SIZE << std::endl;
    }

    void * blob_ptr = BlobsPtrFromMMap(location);

    // Need to allow the info struct to have read/write access
    // TODO: We could also allow other information to go here to keep it close together.
    void * info_ptr = &InfoFromBlobsPtr(blob_ptr);
    assert(info_ptr >= location);
    if (style != MMAP_STYLE_MALLOC) {
        // if(MAP_FAILED == mmap(info_ptr, sizeof(MMAP_ALLOC_INFO), PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE | MAP_FIXED, -1, 0)) {
        void * page_aligned = AlignToSystemPageSize(info_ptr);
        if(0 != mprotect(page_aligned, ((char*)blob_ptr - (char*)page_aligned), PROT_READ | PROT_WRITE))
            error2("Could not mprotect info struct location.");
    }


    MMAP_ALLOC_INFO * info = new (info_ptr) MMAP_ALLOC_INFO(location, blob_fd, uid_fd, style);

    if (style == MMAP_STYLE_MALLOC) {
        // All pages in the range start out as alloced.
        info->occupied_pages.set();
    } else if (style == MMAP_STYLE_FILE_BACKED) {
        // TODO: Sort out the fd handling here.
        // map in already existing parts of the file.
        // Files need to be a multiple of the page size and the same length
        size_t blob_size = SizeFromFD(blob_fd);
        size_t uid_size = SizeFromFD(uid_fd);
        if(blob_size != uid_size)
            error("Blob and uid files are not the same length.");
        if(blob_size % ZEF_PAGE_SIZE != 0)
            error("Blob file is not a multiple of the page size.");
        if(blob_size > ZEF_UID_SHIFT)
            error("Blob file is larger than the maximum allowed blobs length.");
        for(int page_ind = 0 ; page_ind < blob_size / ZEF_PAGE_SIZE ; page_ind++)
            EnsurePage(*info, page_ind);
    }

    alloc_list.push_back(info);
    return *info;
}


using namespace std;
void Wait() {
    char dummy;
    cin.ignore(std::numeric_limits<std::streamsize>::max(),'\n');
}
int main(void) {
    cerr << "PID: " << getpid() << endl;
    cerr << "Page size: " << sysconf(_SC_PAGESIZE) << endl;

    MMAP_ALLOC_INFO& alloc1 = CreateMmap(MMAP_STYLE_ANONYMOUS);
    
    // MMAP_ALLOC_INFO& alloc2 = CreateMmap(MMAP_STYLE_MALLOC);

    MMAP_ALLOC_INFO& alloc3 = CreateMmap();

    void * blobs1 = BlobsPtrFromInfo(&alloc1);
    void * blobs3 = BlobsPtrFromInfo(&alloc3);

    cerr << "Alloc1 bitmap: " << alloc1.occupied_pages << endl;
    // cerr << "Alloc2 bitmap: " << alloc2.occupied_pages << endl;
    cerr << "Alloc3 bitmap: " << alloc3.occupied_pages << endl;

    Wait();

    cerr << "Alloc 1 range: " << CheckRange(blobs1, 1*MB) << endl;
    // cerr << "Alloc 2 range: " << CheckRange(alloc2.blob_ptr, 1*MB) << endl;
    // cerr << "Alloc 2 out of range: " << CheckRange(alloc2, (char*)alloc2.blob_ptr+50*MB, 1*MB) << endl;
    // cerr << "Alloc 2 out of range: " << CheckRange(alloc2, alloc2.location, 1*MB) << endl;

    cerr << "Alloc 3 range: " << CheckRange(blobs3, 8*MB) << endl;
    // EnsureRange(alloc3.blob_ptr, 6*MB);
    // EnsureRange((char*)alloc3.blob_ptr+5*MB, 8*MB);
    EnsureRange((char*)blobs3+1*MB, 8*MB);
    cerr << "Alloc3 bitmap: " << alloc3.occupied_pages << endl;
    cerr << "Alloc 3 range: " << CheckRange(blobs3, 8*MB) << endl;

    cerr << "Writing a" << endl;
    ((char*)blobs3)[5*MB] = 'a';
    cerr << "Writing b" << endl;
    ((char*)blobs3)[7*MB] = 'b';
    cerr << "Writing c" << endl;
    ((char*)blobs3)[8*MB] = 'c';

    Wait();

    cerr << endl << endl;
    cerr << "File backed testing" << endl;

    int blob_fd = open("blobs.data", O_RDWR | O_CREAT, 0644);
    int uid_fd = open("uids.data", O_RDWR | O_CREAT, 0644);
    // int blob_fd = open("blobs.data", O_RDONLY | O_CREAT, 0644);
    // int uid_fd = open("uids.data", O_RDONLY | O_CREAT, 0644);

    cerr << "Blob file size: " << SizeFromFD(blob_fd) << endl;

    MMAP_ALLOC_INFO& file_alloc = CreateMmap(MMAP_STYLE_FILE_BACKED, blob_fd, uid_fd);
    void* file_blobs = BlobsPtrFromInfo(&file_alloc);
    cerr << "File bitmap: " << file_alloc.occupied_pages << endl;

    char * target = (char*)file_blobs + 600*MB;

    cerr << "Ensuring pages" << endl;
    EnsureRange(target, 100*MB);
    cerr << "File bitmap: " << file_alloc.occupied_pages << endl;

    cerr << "Byte at 600MB: " << *target << endl;

    cerr << "Setting byte at 600MB" << endl;
    cin >> *target;
    Wait();

    Wait();

    cerr << "Byte at 600MB: " << *target << endl;

    return 0;
}
