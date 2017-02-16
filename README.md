# persistance
Set of on disk collection implementing JDK List and Map as well as similar long collections


To include in your project

1. Add repo:
```xml
<repositories>
	<repository>
		<id>persistance-mvn-repo</id>
		<url>https://raw.github.com/myteksp/persistance/mvn-repo/</url>
			<snapshots>
				<enabled>true</enabled>
				<updatePolicy>always</updatePolicy>
			</snapshots>
	</repository>
</repositories>
```

2. Include the artifact:
```xml
<dependency>
	<groupId>com.gf.persistance</groupId>
	<artifactId>persistance</artifactId>
	<version>0.0.1-SNAPSHOT</version>
</dependency>
```