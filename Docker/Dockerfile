FROM openjdk:11
COPY . /Users/Jones/cs1660_intro_to_cloud_computing/cs1660_intro_to_cloud_computing/Docker
WORKDIR /Users/Jones/cs1660_intro_to_cloud_computing/cs1660_intro_to_cloud_computing/Docker
ADD helloWorld.class helloWorld.class
RUN javac helloWorld.java
CMD ["java", "helloWorld"]