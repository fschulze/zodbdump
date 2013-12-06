virtualenv = virtualenv

all: .installed.cfg

bin/python:
	$(virtualenv) --system-site-packages --clear .
	-./clear-setuptools-dependency-links

bin/buildout: bin/python
	bin/pip install zc.buildout

.installed.cfg: bin/buildout buildout.cfg setup.py
	bin/buildout -v
