
PACKAGE=cass-ops-agent
# set these at the command line for versioned RPMs, otherwise
# they default to these values
VERSION ?= 1.2.0
RELEASE ?= 1

JARVERSION=1.2.0
SCALA_VERSION=2.11
PREFIX=/usr/local

# should not need to change these
JAR=${PACKAGE}-${JARVERSION}.jar
JAR_FAT=${PACKAGE}-assembly-${JARVERSION}.jar
TARGET=target/scala-${SCALA_VERSION}
LIBDIR=${PREFIX}/lib
BINDIR=${PREFIX}/bin
CONFDIR=/etc/cass-ops-agent
LOGDIR=/var/log/cass-ops-agent
RUNDIR=/var/run/cass-ops-agent
DBDIR=/var/lib/cass-ops-agent
SBT_JAVA_OPT=""


all: build-sbt release

build-sbt:
	sbt -verbose ${SBT_JAVA_OPT} compile

release: build-sbt
	sbt -verbose ${SBT_JAVA_OPT} assembly

install: all
	install -d -m 0755 ${DESTDIR}${LIBDIR}/${PACKAGE}
	install -m 0644 ${TARGET}/${JAR_FAT} ${DESTDIR}${LIBDIR}/${PACKAGE}/${JAR}
	install -d -m 0755 ${DESTDIR}/${BINDIR}/cass-ops-cli
	cp -dr --no-preserve=ownership bin/cmdline-tool/* ${DESTDIR}/${BINDIR}/cass-ops-cli
	install -d -m 0755 ${DESTDIR}${CONFDIR}
	install -d -m 0755 ${DESTDIR}${LOGDIR}
	install -d -m 0755 ${DESTDIR}${RUNDIR}
	install -d -m 0755 ${DESTDIR}${DBDIR}
	install -d -m 0755 ${DESTDIR}${DBDIR}/data

TAR_CONTENTS=*.sbt Makefile src/ project/ packages/ bin/cmdline-tool
RPMARCH=`arch`

rpm:
	rm -f $(PACKAGE)-$(VERSION).tar.gz
	tar --transform 's,^,$(PACKAGE)-$(VERSION)/,S' -czvf $(PACKAGE)-$(VERSION).tar.gz $(TAR_CONTENTS) ; \
	mkdir -p .rpmbuild/BUILD \
	.rpmbuild/SPECS \
	.rpmbuild/SOURCES \
	.rpmbuild/SRPMS \
	.rpmbuild/RPMS/$(RPMARCH) \
	.rpmbuild/RPMS/noarch \
	.rpmbuild/tmp
	cp -f $(PACKAGE)-$(VERSION).tar.gz .rpmbuild/SOURCES
	rpmbuild --clean -bb packages/rpm/$(PACKAGE).spec --target $(RPMARCH) \
    --define "_topdir ${PWD}/.rpmbuild" \
    --define "_tmppath ${PWD}/.rpmbuild/tmp" \
    --define "version ${VERSION}" \
    --define "release ${RELEASE}"
	@echo "================================================"
	@echo "RPMs located in .rpmbuild/RPMS"

clean:
	sbt clean
	rm -f $(PACKAGE)-$(VERSION).tar.gz \
    $(TARGET)/$(JAR)
	rm -rf .rpmbuild/

# run server targets

run:
	#sh ./src/scripts/config.sh
	@echo "================================================"
	@echo "Starting $(APP_NAME) in development mode $(TARGET)/$(JAR_FAT)"
	java -server -Xmx1024m -Dstage=development -jar $(TARGET)/$(JAR_FAT)

