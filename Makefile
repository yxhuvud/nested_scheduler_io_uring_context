.PHONY : clean run all

all : clean run

clean :
	rm -rf ./test/*

run  :
	mkdir -p test
	CRYSTAL_LOAD_DWARF=1 crystal spec -Dpreview_mt --error-trace --stats

