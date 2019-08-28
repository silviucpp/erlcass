.DEFAULT_GOAL := compile

REBAR=rebar3

# for benchmark

MODULE=erlcass
PROCS=100
REQ=100000
BENCH_PROFILE_ARGS=-pa _build/bench/lib/erlcass/benchmarks -pa _build/bench/lib/*/ebin -noshell -config benchmarks/benchmark.config

C_SRC_DIR = $(shell pwd)/c_src
C_SRC_ENV ?= $(C_SRC_DIR)/env.mk

#regenerate all the time the env.mk
ifneq ($(wildcard $(C_SRC_DIR)),)
	GEN_ENV ?= $(shell erl -noshell -s init stop -eval "file:write_file(\"$(C_SRC_ENV)\", \
		io_lib:format( \
			\"ERTS_INCLUDE_DIR ?= ~s/erts-~s/include/~n\" \
			\"ERL_INTERFACE_INCLUDE_DIR ?= ~s~n\" \
			\"ERL_INTERFACE_LIB_DIR ?= ~s~n\", \
			[code:root_dir(), erlang:system_info(version), \
			code:lib_dir(erl_interface, include), \
			code:lib_dir(erl_interface, lib)])), \
		halt().")
    $(GEN_ENV)
endif

include $(C_SRC_ENV)

nif_compile:
	@./build_deps.sh $(CPP_DRIVER_REV)
	@make V=0 -C c_src -j 8

nif_clean:
	@make -C c_src clean

compile:
	${REBAR} compile

clean:
	${REBAR} clean

ct:
	mkdir -p log
	ct_run -suite integrity_test_SUITE -pa ebin -pa deps/*/ebin erl -pa _build/default/lib/*/ebin -include include -logdir log -erl_args -config benchmarks/benchmark.config

setup_benchmark:
	${REBAR} as bench compile
	erl $(BENCH_PROFILE_ARGS) -eval "load_test:prepare_load_test_table()" -eval "init:stop()."

benchmark:
	erl $(BENCH_PROFILE_ARGS) -eval "benchmark:run($(MODULE), $(PROCS), $(REQ))" -eval "init:stop()."

cpplint:
	cpplint --counting=detailed --filter=-legal/copyright,-build/include_subdir,-build/include_order,-whitespace/braces,-whitespace/parens,-whitespace/newline \
			--linelength=300 \
			--exclude=c_src/*.o --exclude=c_src/*.mk  \
			c_src/*.*

cppcheck:
	cppcheck -j 8 --enable=all \
	 		 -I /usr/local/opt/openssl/include \
	 		 -I /usr/local/include \
	 		 -I _build/deps/cpp-driver/include \
	 		 -I _build/deps/cpp-driver/src \
	 		 -I $(ERTS_INCLUDE_DIR) \
	 		 -I $(ERL_INTERFACE_INCLUDE_DIR) \
	 		 --suppress=*:*_build/deps/* \
	 		 --suppress=*:/usr/local/* \
	 		 --xml-version=2 \
	 		 --output-file=cppcheck_results.xml \
	 		 c_src/
