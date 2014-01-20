perf_report:
	./rebar -C rebar-test.config get compile
	erl -pa deps/*/ebin ebin -noshell -run hyper perf_report -s init stop

estimate_report:
	./rebar -C rebar-test.config get compile
	erl -pa deps/*/ebin ebin -noshell -run hyper estimate_report -s init stop
	bin/plot.R
