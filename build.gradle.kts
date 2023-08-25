plugins {
	java
	alias(libs.plugins.quarkus)
	alias(libs.plugins.jib)
	alias(libs.plugins.owasp)
	`maven-publish`
}

val projectVersion: String by project

group = "net.tomjo"
version = projectVersion

tasks.wrapper {
	distributionType = Wrapper.DistributionType.ALL
	gradleVersion = "8.3"
}

configurations {
	compileOnly {
		extendsFrom(configurations.annotationProcessor.get())
	}
}

java {
	toolchain {
		languageVersion.set(JavaLanguageVersion.of(17))
	}
}

repositories {
	mavenCentral()
}

dependencies {
	implementation(enforcedPlatform(libs.quarkus.bom))
	annotationProcessor(enforcedPlatform(libs.quarkus.bom))
	implementation(libs.pulsar.client.admin)
	implementation(libs.quarkus.arc)
    implementation(libs.quarkus.picocli)
	compileOnly(libs.lombok)
	annotationProcessor(libs.lombok)
	testImplementation(libs.quarkus.junit5)
}

tasks.withType<Test> {
	useJUnitPlatform()
	systemProperty("java.util.logging.manager", "org.jboss.logmanager.LogManager")
}

tasks.withType<JavaCompile> {
	options.encoding = "UTF-8"
	options.compilerArgs.add("-parameters")
}

dependencyCheck {
	format = org.owasp.dependencycheck.reporting.ReportGenerator.Format.ALL.toString()
}

publishing {
	publications {
		create<MavenPublication>(project.name) {
			from(components["java"])
			artifactId = project.name
		}
	}

	repositories {
		maven {
			val s3MavenRepository: String by project
			url = uri(s3MavenRepository)
			credentials(AwsCredentials::class) {
				if (project.hasProperty("s3AccessKey")) {
					val s3AccessKey: String by project
					accessKey = s3AccessKey
				}
				if (project.hasProperty("s3SecretKey")) {
					val s3SecretKey: String by project
					secretKey = s3SecretKey
				}
			}
		}
	}
}

jib {
	to {
		image = project.property("container.image") as String
		tags = setOf(project.version.toString())
	}
}

val configureS3Endpoint by tasks.registering {
	group = "publishing"
	doFirst {
		val s3EndPoint = System.getProperty("org.gradle.s3.endpoint")
		if (s3EndPoint == null) {
			System.setProperty("org.gradle.s3.endpoint", project.property("s3EndPoint") as String)
		}
	}
}

tasks.withType(PublishToMavenRepository::class) {
	dependsOn(tasks.jar)
	dependsOn(configureS3Endpoint)
}
