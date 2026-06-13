server: test.cc
	g++ $^ -o $@ -luring

.PHONY:clean
clean:
	rm server