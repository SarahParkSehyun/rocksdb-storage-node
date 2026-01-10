#!/bin/bash
cd /home/mw/rocksdb
./db_bench \
  --benchmarks=fillrandom \
  --db=/work/tmp/db_bench \
  --num=1100000000 \
  --partition_index=true \
  --partition_index_and_filters=true \
  --threads=1 \
  --max_background_jobs=64 \
  --key_size=48 \
  --value_size=43 \
  --compression_type=none \
  --use_direct_reads=1 \
  --use_direct_io_for_flush_and_compaction=1 \
  --target_file_size_base=67108864 \
  --bloom_bits=10 \
  --disable_wal=1 \
  --max_write_buffer_number=32 \
  --memtablerep=vector \
  --write_buffer_size=67108864 \
  --seed=1722070161 \
  --block_size=4096 \
  --ttl_seconds=6060243012 \
  --use_existing_db=false \
  --checksum_type=1 \
  --allow_concurrent_memtable_write=0
