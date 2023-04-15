./gradlew clean build \
&& docker build -t cellarium . \
&& docker tag cellarium stepponomarev/cellarium \
&& docker push stepponomarev/cellarium
