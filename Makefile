run: deps
	GOPATH=$(CURDIR)/.go go build -o gdp_replicate.so -buildmode=c-shared

deps:
	rm -Rf $(CURDIR)/.go/src/github.com/tonyyanga/gdp-replicate
	ln -s $(CURDIR) $(CURDIR)/.go/src/github.com/tonyyanga/gdp-replicate
	GOPATH=$(CURDIR)/.go go get -d

clean:
	rm $(CURDIR)/.go/src/github.com/tonyyanga/gdp-replicate/gdp_replicate.so
	rm $(CURDIR)/.go/src/github.com/tonyyanga/gdp-replicate/gdp_replicate.h
	rm -Rf $(CURDIR)/.go/src/github.com/tonyyanga/gdp-replicate
