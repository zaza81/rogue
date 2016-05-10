libraryDependencies <++= (scalaVersion) { scalaVersion =>
  val specsVersion = scalaVersion match {
    case "2.11.7" => "2.4.2"
    case "2.10.4" => "1.12.3"
  }
  val liftVersion = "3.0-SNAPSHOT"
  def sv(s: String) = s + "_" + (scalaVersion match {
      case "2.11.7" => "2.11"
      case "2.10.4" => "2.10"
  })
  Seq(
    "com.foursquare"           % sv("rogue-field")     % "2.5.0"      % "compile",
    "com.github.zaza81"              % sv("lift-mongodb")    % liftVersion  % "compile" intransitive(),
    "com.github.zaza81"              % sv("lift-common")     % liftVersion  % "compile",
    "com.github.zaza81"              % sv("lift-json")       % liftVersion  % "compile",
    "com.github.zaza81"              % sv("lift-util")       % liftVersion  % "compile",
    "joda-time"                % "joda-time"           % "2.1"        % "provided",
    "org.joda"                 % "joda-convert"        % "1.2"        % "provided",
    "org.mongodb"              % "mongodb-driver"   % "3.2.2"     % "compile",
    "org.mongodb"              % "mongodb-driver-async"% "3.2.2"     % "compile",
    "junit"                    % "junit"               % "4.5"        % "test",
    "com.novocode"             % "junit-interface"     % "0.6"        % "test",
    "ch.qos.logback"           % "logback-classic"     % "0.9.26"     % "provided",
    "org.specs2"              %% "specs2"              % specsVersion % "test",
    "org.scala-lang"           % "scala-compiler"      % scalaVersion % "test"
  )
}

Seq(RogueBuild.defaultSettings: _*)
