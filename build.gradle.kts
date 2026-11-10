plugins {
	java
	alias(libs.plugins.quarkus)
	alias(libs.plugins.owasp)
}

val projectVersion: String by project
val nvdApiKey: String? by project

group = "net.tomjo"
version = projectVersion

tasks.wrapper {
	distributionType = Wrapper.DistributionType.ALL
	gradleVersion = "9.9.1"
	distributionSha256Sum= "f20c816177c0ff192881874852260771b462903aef3e794b0dac4759d1fda284"
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
	annotationProcessor(enforcedPlatform(libs.quarkus.bom))
	implementation(enforcedPlatform(libs.quarkus.bom))
	implementation(libs.pulsar.client.admin)
	implementation(libs.pulsar.metadata)
	implementation(libs.pulsar.managed.ledger)
	implementation(libs.quarkus.arc)
    implementation(libs.quarkus.picocli)
    implementation(libs.bookkeeper){
		exclude(group = "org.bouncycastle", module = "bc-fips")
	}
    implementation(libs.zookeeper){
		exclude(group = "org.bouncycastle", module = "bc-fips")
	}
	implementation(libs.vavr)
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
	nvd.apiKey = nvdApiKey
	analyzers.assemblyEnabled = false
}

tasks.withType<AbstractArchiveTask>().configureEach {
	isPreserveFileTimestamps = false
	isReproducibleFileOrder = true
}
