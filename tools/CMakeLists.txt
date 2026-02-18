set(CORE_TOOLS
  sst_dump.cc
  ldb.cc)
foreach(src ${CORE_TOOLS})
  get_filename_component(exename ${src} NAME_WE)
  add_executable(${exename}${ARTIFACT_SUFFIX}
    ${src})
  target_link_libraries(${exename}${ARTIFACT_SUFFIX} ${ROCKSDB_LIB})
  list(APPEND core_tool_deps ${exename})
endforeach()

if(WITH_TOOLS)
  set(TOOLS
    db_sanity_test.cc
    write_stress.cc
    db_repl_stress.cc
    dump/rocksdb_dump.cc
    dump/rocksdb_undump.cc)
  foreach(src ${TOOLS})
    get_filename_component(exename ${src} NAME_WE)
    add_executable(${exename}${ARTIFACT_SUFFIX}
      ${src})
    target_link_libraries(${exename}${ARTIFACT_SUFFIX} ${ROCKSDB_LIB} ${THIRDPARTY_LIBS})
    list(APPEND tool_deps ${exename})
  endforeach()

  add_custom_target(ldb_tests
    COMMAND python ${CMAKE_CURRENT_SOURCE_DIR}/ldb_tests.py
    DEPENDS ldb)
endif()

# eRPC bloomcheck server (optional, requires ERPC_ROOT)
# Try to find eRPC headers in common locations
set(ERPC_HEADER_PATH "")
if(EXISTS "${ERPC_ROOT}/src/rpc.h")
  set(ERPC_HEADER_PATH "${ERPC_ROOT}/src")
elseif(EXISTS "${ERPC_ROOT}/src/erpc/rpc.h")
  set(ERPC_HEADER_PATH "${ERPC_ROOT}/src")
elseif(EXISTS "${ERPC_ROOT}/include/erpc/rpc.h")
  set(ERPC_HEADER_PATH "${ERPC_ROOT}/include")
elseif(EXISTS "${ERPC_ROOT}/erpc/rpc.h")
  set(ERPC_HEADER_PATH "${ERPC_ROOT}")
endif()

if(ERPC_HEADER_PATH)
  # Find eRPC library
  set(ERPC_LIB_PATH "")
  if(EXISTS "${ERPC_ROOT}/build_perf/liberpc.a")
    set(ERPC_LIB_PATH "${ERPC_ROOT}/build_perf/liberpc.a")
  elseif(EXISTS "${ERPC_ROOT}/build_dbg/liberpc.a")
    set(ERPC_LIB_PATH "${ERPC_ROOT}/build_dbg/liberpc.a")
  elseif(EXISTS "${ERPC_ROOT}/build/liberpc.a")
    set(ERPC_LIB_PATH "${ERPC_ROOT}/build/liberpc.a")
  endif()
  
  if(ERPC_LIB_PATH)
    # Find protobuf (required for bloomcheck proto)
    find_package(Protobuf REQUIRED)
    
    # Add eRPC server executable
    add_executable(bloomcheck_erpc_server${ARTIFACT_SUFFIX}
      bloomcheck_erpc_server.cc
      bloomcheck.pb.cc
    )
    
    # eRPC requires asio headers (even for InfiniBand transport)
    set(ERPC_ASIO_PATH "${ERPC_ROOT}/third_party/asio/include")
    
    target_include_directories(bloomcheck_erpc_server${ARTIFACT_SUFFIX} BEFORE PRIVATE
      ${ERPC_HEADER_PATH}
      ${ERPC_ASIO_PATH}
      ${Protobuf_INCLUDE_DIRS}
    )
    
    target_link_libraries(bloomcheck_erpc_server${ARTIFACT_SUFFIX} PRIVATE
      ${ERPC_LIB_PATH}
      pthread
      numa
      ibverbs
      ${Protobuf_LIBRARIES}
    )
    
    # Add compile definitions for eRPC
    # eRPC uses TRANSPORT definition to select transport type
    # ERPC_INFINIBAND is needed for ib_transport.h
    target_compile_definitions(bloomcheck_erpc_server${ARTIFACT_SUFFIX} PRIVATE
      TRANSPORT=infiniband
      ERPC_INFINIBAND
    )
    
    # Disable Werror for eRPC headers (they have unused function warnings)
    # We only want to treat our own code warnings as errors
    target_compile_options(bloomcheck_erpc_server${ARTIFACT_SUFFIX} PRIVATE
      -Wno-error=unused-function
      -Wno-error=volatile
    )
    
    list(APPEND tool_deps bloomcheck_erpc_server)
    
    message(STATUS "eRPC bloomcheck server enabled (ERPC_ROOT=${ERPC_ROOT}, header_path=${ERPC_HEADER_PATH}, lib_path=${ERPC_LIB_PATH})")
  else()
    message(STATUS "eRPC library not found at ${ERPC_ROOT}/build_perf|build_dbg|build/liberpc.a, skipping bloomcheck_erpc_server")
  endif()
else()
  message(STATUS "eRPC headers not found (ERPC_ROOT=${ERPC_ROOT}), tried: src/erpc/rpc.h, include/erpc/rpc.h, erpc/rpc.h")
endif()
