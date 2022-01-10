pipeline {
  agent any

  environment {
    branch          = "${env.BRANCH_NAME}"
  }

  stages {

    stage("Build") {
      steps {
        script {      
          sh './gradlew build'
        }
      }
    } 
  }

  post {
    failure {
      // Notify everyone via Slack:
      slackSend(
        color: '#FF0000',
        channel: '#devops',
        message: "@here Graylog delimited output plugin build failure on branch ${branch}"
      )
    }

    success {
      archiveArtifacts artifacts: '**/build/libs/*.jar', fingerprint: true
      slackSend(
        color: "#00FF00",
        channel: "#devops",
        message: "Graylog delimited output plugin build success on branch ${branch}"
      )
    }
  }
}
