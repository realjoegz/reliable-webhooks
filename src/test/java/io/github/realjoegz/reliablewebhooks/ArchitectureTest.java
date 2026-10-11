package io.github.realjoegz.reliablewebhooks;

import static com.tngtech.archunit.base.DescribedPredicate.not;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.equivalentTo;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAPackage;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import io.github.realjoegz.reliablewebhooks.http.WebhookHttpClient;
import org.apache.hc.client5.http.DnsResolver;

/**
 * The address check at connect time holds only if every outbound request goes through the one
 * client that has it. These rules keep any other HTTP client out of the service.
 */
@AnalyzeClasses(
    packagesOf = ReliableWebhooksApplication.class,
    importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

  // The clients of the JDK and of Spring. A java.net.URL is among them because it opens a
  // connection by itself, through openStream and openConnection.
  @ArchTest
  static final ArchRule noClassUsesAnHttpClientOfTheJdkOrOfSpring =
      noClasses()
          .should()
          .dependOnClassesThat()
          .resideInAnyPackage(
              "java.net.http..",
              "org.springframework.web.client..",
              "org.springframework.http.client..")
          .orShould()
          .dependOnClassesThat()
          .haveFullyQualifiedName("java.net.HttpURLConnection")
          .orShould()
          .dependOnClassesThat()
          .haveFullyQualifiedName("java.net.URLConnection")
          .orShould()
          .dependOnClassesThat()
          .haveFullyQualifiedName("java.net.URL");

  @ArchTest
  static final ArchRule onlyTheHttpPackageUsesTheApacheLibrary =
      noClasses()
          .that()
          .resideOutsideOfPackage("io.github.realjoegz.reliablewebhooks.http")
          .should()
          .dependOnClassesThat()
          .resideInAPackage("org.apache.hc..");

  // The resolver interface is the one type of the library that another class needs: the guard
  // implements it. With nothing else of the library, no other class can build or drive a client.
  @ArchTest
  static final ArchRule onlyWebhookHttpClientBuildsAClient =
      noClasses()
          .that()
          .doNotHaveFullyQualifiedName(WebhookHttpClient.class.getName())
          .should()
          .dependOnClassesThat(
              resideInAPackage("org.apache.hc..").and(not(equivalentTo(DnsResolver.class))));
}
