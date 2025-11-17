cmake_minimum_required(VERSION 3.10)
project(sql_test)

add_executable(Sqltest
    test.cc)

target_include_directories(Sqltest
    PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/../include
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

target_link_directories(Sqltest
    PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/../build
)

target_link_libraries(Sqltest sqlparser)