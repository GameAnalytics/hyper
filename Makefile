perf_report:
	erl -pa deps/*/ebin ebin -noshell -run hyper perf_report -s init stop
