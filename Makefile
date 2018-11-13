run: deps
	GOPATH=$(CURDIR)/.go go build

deps:
	rm -Rf $(CURDIR)/.go/src/github.com/tonyyanga/gdp-replicate
	ln -s $(CURDIR) $(CURDIR)/.go/src/github.com/tonyyanga/gdp-replicate
	GOPATH=$(CURDIR)/.go go get -d

clean:
	rm $(CURDIR)/.go/src/github.com/tonyyanga/gdp-replicate/gdp-replicate
	rm -Rf $(CURDIR)/.go/src/github.com/tonyyanga/gdp-replicate
