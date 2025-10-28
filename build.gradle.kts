plugins {
	java
    application
}

group = "sdmitry"
version = "0.0.1-SNAPSHOT"
description = "Demo KV Store no deps"

java {
	toolchain {
		languageVersion = JavaLanguageVersion.of(21)
	}
}

repositories {
	mavenCentral()
}

dependencies {
}

tasks.withType<Test> {
	useJUnitPlatform()
}

application {
    mainClass.set("sdmitry.kv.KV")

    applicationDefaultJvmArgs = listOf("--add-modules=jdk.httpserver")
}

tasks.jar {
    manifest {
        attributes["Main-Class"] = "sdmitry.kv.KV"
    }
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}
