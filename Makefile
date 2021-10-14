.PHONY : clean run all benchmark_baseline benchmark_libevent benchmark_uring

all : clean run

clean :
	rm -rf ./test/*

run  :
	mkdir -p test
	CRYSTAL_LOAD_DWARF=1 crystal spec -Dpreview_mt --error-trace --stats

benchmark_baseline:
	crystal build -Dpreview_mt --error-trace --stats --release benchmarks/baseline.cr && ./baseline

benchmark_libevent:
	crystal build -Dpreview_mt --error-trace --stats --release benchmarks/nested_libevent.cr && ./nested_libevent

benchmark_uring:
	crystal build -Dpreview_mt --error-trace --stats --release benchmarks/nested_uring.cr && ./nested_uring

