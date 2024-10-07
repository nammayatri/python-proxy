pipeline {
  agent {
    kubernetes {
      label 'dind-agent'
    }
  }
  environment {
    ACCOUNT_ID = '463356420488'
  }
  stages {
    stage('Initialize') {
      steps {
        script {
          env.LAST_COMMIT_HASH = sh(script: "git rev-parse HEAD", returnStdout: true).trim().substring(0,6)
        }
      }
    }
    
    stage('Pull from Github and Push') {
      steps {
          withCredentials([string(credentialsId: 'pk_docker_hub', variable: 'DOCKER_PASSWORD')]) {
            sh 'docker build -t python-proxy .'
            sh 'echo $DOCKER_PASSWORD'
            sh 'echo $DOCKER_PASSWORD | docker login -u 12349901 --password-stdin'
            sh "docker tag python-proxy:latest 12349901/python-proxy:${env.LAST_COMMIT_HASH}"
            sh "docker push 12349901/python-proxy:${env.LAST_COMMIT_HASH}"
        }
      }
    }
  }
}
