FROM openjdk:8
COPY sb-workflow-handler-1.0.0.jar /opt/
EXPOSE 9060
CMD ["java", "-XX:+PrintFlagsFinal", "$JAVA_OPTIONS", "-XX:+UnlockExperimentalVMOptions", "-XX:+UseCGroupMemoryLimitForHeap", "-jar", "/opt/sb-workflow-handler-1.0.0.jar"]

