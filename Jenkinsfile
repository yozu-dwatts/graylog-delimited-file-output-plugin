pipeline {
  agent any

  environment {
    branch          = "${env.BRANCH_NAME}"
  }

  stages {

    stage("Build") {
      steps {
        script {      
          
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
      slackSend(
        color: "#00FF00",
        channel: "#devops",
        message: "Graylog delimited output plugin build success on branch ${branch}"
      )
    }
  }
}
