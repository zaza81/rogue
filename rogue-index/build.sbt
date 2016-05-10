libraryDependencies <++= (scalaVersion) { scalaVersion =>
  def sv(s: String) = s + "_" + (scalaVersion match {
      case "2.11.7" => "2.11"
      case "2.10.4" => "2.10"
  })
  Seq(
    "com.foursquare"           % sv("rogue-field")     % "2.5.0"      % "compile"
  )
}

Seq(RogueBuild.defaultSettings: _*)
