libraryDependencies <++= (scalaVersion) { scalaVersion =>
  val liftVersion = "3.0-SNAPSHOT"
  def sv(s: String) = s + "_" + (scalaVersion match {
      case "2.11.7" => "2.11"
      case "2.10.4" => "2.10"
  })
  Seq(
    "com.github.zaza81"              % sv("lift-util")           % liftVersion  % "compile" intransitive(),
    "com.github.zaza81"              % sv("lift-common")         % liftVersion  % "compile" intransitive(),
    "com.github.zaza81"              % sv("lift-record")         % liftVersion  % "compile" intransitive(),
    "com.github.zaza81"              % sv("lift-mongodb-record") % liftVersion  % "compile" intransitive(),
    "com.github.zaza81"              % sv("lift-mongodb")        % liftVersion  % "compile" intransitive(),
    "com.github.zaza81"              % sv("lift-webkit")         % liftVersion  % "compile" intransitive(),
    "com.github.zaza81"              % sv("lift-json")           % liftVersion  % "compile",
    "joda-time"                % "joda-time"               % "2.1"        % "compile",
    "org.joda"                 % "joda-convert"            % "1.2"        % "compile",
    "org.mongodb"              % "mongodb-driver"       % "3.2.2"     % "compile")
}

Seq(RogueBuild.defaultSettings: _*)
