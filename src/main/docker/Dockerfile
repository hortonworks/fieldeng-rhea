FROM java:8
ADD rhea-0.0.1.jar app.jar
RUN bash -c 'touch /app.jar'
ENTRYPOINT ["java","-Djava.security.egd=file:/dev/./urandom","-Dspring.profiles.active=container","-jar","/app.jar","--historian.api.host=$API_HOST","--historian.api.port=$API_PORT","> historian.log"]