cmake_minimum_required(VERSION 3.10)
project(exd_sqlparser LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_BUILD_TYPE Debug)




add_library(sqlparser SHARED
    src/SqlQueryParser.cpp
    src/SqlQueryScanner.cpp
    src/SqlSyntaxUtils.cpp
    src/ExpressionModelUtils.cpp
    src/SqlDistributedPlanner.cpp
    src/SqlQueryPlanner.cpp
    src/SqlStatement.cpp
    sqlparser.cc

)


target_include_directories(sqlparser
    PUBLIC
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}
    /home/fuckingchen/exceeddata2/exd-compute-lib-v2/source/exd/pervasive/iot/common/exception
    /home/fuckingchen/exceeddata2/exd-compute-lib-v2/source/exd/pervasive/iot/common/util
    /home/fuckingchen/exceeddata2/exd-compute-lib-v2/source/exd/pervasive/iot/common/evaluate/expression
    /home/fuckingchen/exceeddata2/exd-compute-lib-v2/source/exd/pervasive/iot/common/data/typedata
    /home/fuckingchen/exceeddata2/exd-compute-lib-v2/source/exd/pervasive/iot/common/evaluate/model
    /home/fuckingchen/exceeddata2/exd-compute-lib-v2/source/exd/pervasive/iot/common/evaluate/operator/binary
    /home/fuckingchen/exceeddata2/exd-compute-lib-v2/source/exd/pervasive/iot/common/evaluate/runtime/block
    /home/fuckingchen/exceeddata2/exd-compute-lib-v2/source/exd/pervasive/iot/common/evaluate/runtime/block/control
    /home/fuckingchen/exceeddata2/exd-compute-lib-v2/source/exd/pervasive/iot/common/util/mutable
)


#链接libexd.so的动态库
target_link_libraries(sqlparser PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/libexd.a
)

set_target_properties(sqlparser PROPERTIES
    BUILD_RPATH "/home/fuckingchen/exceeddata2/exd-compute-lib-v2/source/build/exd"
)

add_subdirectory(test)
