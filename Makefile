.PHONY: all build jar test check checkstyle coverage clean

all: check build

build jar:
	./gradlew build shadowJar

test:
	./gradlew test

check:
	./gradlew check

checkstyle:
	./gradlew checkstyleMain checkstyleTest

coverage:
	./gradlew test integrationTest jacocoTestReport jacocoTestCoverageVerification
	@echo "Report: build/reports/jacoco/test/html/index.html"

clean:
	./gradlew clean
