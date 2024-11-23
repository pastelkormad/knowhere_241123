# * Find AIO
#
# AIO_INCLUDE - Where to find libaio.h AIO_LIBRARIES - List of libraries when
# using AIO. AIO_FOUND - True if AIO found.

find_path(URING_INCLUDE_DIR liburing.h HINTS $ENV{URING_ROOT}/include)

find_library(URING_LIBRARIES uring HINTS $ENV{URING_ROOT}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(uring DEFAULT_MSG URING_LIBRARIES URING_INCLUDE_DIR)

mark_as_advanced(URING_INCLUDE_DIR URING_LIBRARIES)