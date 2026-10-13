# The jar and its unpacked libraries are the same for every architecture. This stage therefore
# runs on the architecture of the machine that builds, also when the image is built for another
# one with --platform, and Maven never runs under emulation.
FROM --platform=$BUILDPLATFORM eclipse-temurin:21-jdk-alpine@sha256:0bfc69a4758a86710e5c474032d28400a8bd00874766f9e8b1642ac2fd293159 AS build
WORKDIR /build
COPY mvnw pom.xml ./
COPY .mvn .mvn
COPY src/main src/main
# Without the tests, which need a database and run in ./mvnw verify. The local repository of Maven
# is kept from one build to the next and is in no layer.
RUN --mount=type=cache,target=/root/.m2 ./mvnw -B package -Dmaven.test.skip=true
# Unpacked, the libraries are a layer of their own, which a change to the service does not rebuild.
RUN java -Djarmode=tools -jar target/reliable-webhooks-*.jar extract --layers \
      --destination layers --application-filename reliable-webhooks.jar

# No shell and no package manager. The process runs as the user nonroot of the base image, which
# is named by its number here so that an orchestrator can check it.
FROM gcr.io/distroless/java21-debian13:nonroot@sha256:51c242e7ddaba3666d47115a76a19cfa5076b2ad3ed6077b30d7bac689d98fa1
USER 65532:65532
WORKDIR /app
COPY --from=build /build/layers/dependencies/ ./
COPY --from=build /build/layers/snapshot-dependencies/ ./
COPY --from=build /build/layers/application/ ./
# A lookup that no name server answers holds one of the 32 lookup threads of a replica until the
# resolver of the C library gives up. The resolver of glibc reads these options on top of the
# /etc/resolv.conf that the container runtime writes. By resolv.conf(5) they make it wait 1 s for
# a name server, where its default is 5 s, and ask twice, which is its default.
ENV RES_OPTIONS="timeout:1 attempts:2"
EXPOSE 8080
# StartupInvariants refuses to start without the flag.
ENTRYPOINT ["java", "-XX:+ExitOnOutOfMemoryError", "-jar", "reliable-webhooks.jar"]
