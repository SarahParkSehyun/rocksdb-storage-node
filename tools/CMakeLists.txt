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
if(EXISTS "${ERPC_ROOT}/src/erpc/rpc.h")
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
    
    target_include_directories(bloomcheck_erpc_server${ARTIFACT_SUFFIX} PRIVATE
      ${ERPC_ROOT}/src
      ${Protobuf_INCLUDE_DIRS}
    )
    
    target_link_libraries(bloomcheck_erpc_server${ARTIFACT_SUFFIX} PRIVATE
      ${ERPC_LIB_PATH}
      pthread
      numa
      ibverbs
      ${Protobuf_LIBRARIES}
    )
    
    # Add compile definitions if needed
    target_compile_definitions(bloomcheck_erpc_server${ARTIFACT_SUFFIX} PRIVATE
      -DTRANSPORT=infiniband
    )
    
    list(APPEND tool_deps bloomcheck_erpc_server)
    
    message(STATUS "eRPC bloomcheck server enabled (ERPC_ROOT=${ERPC_ROOT})")
  else()
    message(STATUS "eRPC library not found, skipping bloomcheck_erpc_server")
  endif()
else()
  message(STATUS "eRPC headers not found (ERPC_ROOT=${ERPC_ROOT}), skipping bloomcheck_erpc_server")
endif()
