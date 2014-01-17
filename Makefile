perf_report:
	erl -pa deps/*/ebin ebin -noshell -run hyper perf_report -s init stop

estimate_report:
	erl -pa deps/*/ebin ebin -noshell -run hyper estimate_report -s init stop
	bin/plot.R
