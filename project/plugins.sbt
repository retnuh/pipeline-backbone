resolvers += Resolver.url("sbt-plugins", url("https://dl.bintray.com/fashioninsightscentre/sbt-plugins/"))(Resolver.ivyStylePatterns)
credentials += Credentials(Path.userHome / ".sbt" / ".bintrayCredentials")

addSbtPlugin("me.lessis" % "bintray-sbt" % "0.3.0")

addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.6.0")
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.8.2")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.3.5")
addSbtPlugin("de.johoop" % "jacoco4sbt" % "2.2.0")
