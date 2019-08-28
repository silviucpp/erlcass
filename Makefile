.DEFAULT_GOAL := compile

REBAR=rebar3

# for benchmark

MODULE=erlcass
PROCS=100
REQ=100000
BENCH_PROFILE_ARGS=-pa _build/bench/lib/erlcass/benchmarks -pa _build/bench/lib/*/ebin -noshell -config benchmarks/benchmark.config

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
